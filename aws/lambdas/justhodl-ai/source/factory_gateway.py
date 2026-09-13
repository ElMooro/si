"""Authenticated factory admission; guests never receive the Brain action surface.

Called only after private_http_denied has verified the existing service token.
The Worker supplies an identity derived from a verified bearer token. Request
bodies cannot choose that identity, grant roles, schedule work or release code.
"""
import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from factory_core import Invalid, canonical, digest, identifier, iso, validate_prediction, verify_state
from factory_store import Conflict, Store

CFG = Config(connect_timeout=3, read_timeout=10, retries={'max_attempts': 2})


def admit_quota(store, agent, maximum):
    key = 'factory/admission/daily/' + store.clock().date().isoformat() + '-' + agent + '.json'
    for _ in range(3):
        row, etag = store.read(store.private, key)
        count = (row or {}).get('count', 0)
        if count >= maximum:
            raise Invalid('daily_submission_limit')
        try:
            store.put(store.private, key, {'count': count + 1}, etag=etag, absent=etag is None)
            return
        except Conflict:
            pass
    raise Conflict('admission_busy')


def actor(event, invites):
    headers = {k.lower(): str(v) for k, v in (event.get('headers') or {}).items()}
    uid, role = headers.get('x-jh-factory-uid', ''), headers.get('x-jh-factory-role', '')
    if not uid or len(uid) > 160 or role not in ('owner', 'user'):
        raise Invalid('verified_factory_identity_required')
    if role == 'owner':
        return 'owner', True
    for invitation in invites.get('allowlist', []):
        if invitation.get('uid') == uid and invitation.get('enabled') is True:
            return identifier(invitation['agent']), False
    raise Invalid('invitation_required')



ROSTER = ("model", "student", "coder", "researcher", "investor", "deployer", "livermore", "wyckoff", "soros", "druckenmiller")
COMPUTE_INFLIGHT = 8
MATERIALIZE_PER_TICK = 100
DECLARE_CAP = 1000000
CHAT_KEEP = 80


def _workers(store):
    row, etag = store.read(store.private, "factory/salon/workers.json")
    if not isinstance(row, dict):
        row = {"schema_version": "factory-workers.v1", "active": [], "queued": [], "retired": 0}
    return row, etag


def chat_snapshot(store, agent, owner):
    log, _ = store.read(store.private, "factory/salon/chat/" + agent + ".json")
    workers, _ = _workers(store)
    return {
        "ok": True,
        "agent": agent,
        "owner": owner,
        "roster": list(ROSTER),
        "messages": (log or {}).get("messages", [])[-CHAT_KEEP:],
        "workers": {"active": len((workers or {}).get("active") or []), "queued": len((workers or {}).get("queued") or []),
                    "retired": (workers or {}).get("retired") or 0, "cap": ACTIVE_WORKER_CAP},
    }



def _fleet_meta(store):
    row, etag = store.read(store.private, "factory/fleet/meta.json")
    if not isinstance(row, dict):
        row = {"schema_version": "factory-fleet.v1", "declared": 0, "materialized": 0, "inflight": 0,
               "queued": 0, "retired": 0, "learn_bytes": 0, "compute_inflight_cap": COMPUTE_INFLIGHT}
    return row, etag


def chat_snapshot(store, agent, owner):
    log, _ = store.read(store.private, "factory/salon/chat/" + agent + ".json")
    meta, _ = _fleet_meta(store)
    return {
        "ok": True, "agent": agent, "owner": owner, "roster": list(ROSTER), "live_model": "brain",
        "messages": (log or {}).get("messages", [])[-CHAT_KEEP:],
        "workers": {
            "active": int(meta.get("inflight") or 0),
            "queued": int(meta.get("queued") or 0),
            "declared": int(meta.get("declared") or 0),
            "materialized": int(meta.get("materialized") or 0),
            "retired": int(meta.get("retired") or 0),
            "cap": COMPUTE_INFLIGHT,
            "note": "Same 8 compute slots at 1 or 1,000,000 agents. Extra cards queue. Learning log expands.",
        },
    }


def spawn_workers(store, agent, body, policy):
    task = str(body.get("task") or body.get("text") or "").strip()
    role = identifier(str(body.get("role") or "researcher"))[:40]
    try:
        count = int(body.get("count") or 0)
    except (TypeError, ValueError):
        count = 0
    if count < 0:
        raise Invalid("spawn_count_required")
    count = min(count, DECLARE_CAP)
    if not task or len(task) > 2000:
        raise Invalid("spawn_task_required")
    if role in ("owner",) or role.startswith("teacher-"):
        raise Invalid("reserved_agent_name")
    now = iso(store.clock())
    batch_id = "batch-" + digest({"agent": agent, "task": task, "at": now, "n": count})[:16]
    batch = {"schema_version": "factory-fleet-batch.v1", "id": batch_id, "role": role, "task": task[:500],
             "requested": count, "remaining": count, "materialized": 0, "spawned_by": agent, "spawned_at": now}
    store.immutable(store.private, "factory/fleet/batches/" + batch_id + ".json", batch)
    store.immutable(store.private, "factory/fleet/inbox/" + batch_id + ".json", {"batch": batch_id, "at": now})
    meta, etag = _fleet_meta(store)
    meta = {**meta, "declared": int(meta.get("declared") or 0) + count,
            "queued": int(meta.get("queued") or 0) + count, "updated_at": now, "last_batch": batch_id}
    store.put(store.private, "factory/fleet/meta.json", meta, etag=etag, absent=etag is None)
    return {"ok": True, "created": count, "batch": batch_id, "active": int(meta.get("inflight") or 0),
            "queued": int(meta.get("queued") or 0), "declared": meta["declared"], "cap": COMPUTE_INFLIGHT,
            "note": "Accepted %s %s cards. Compute stays %s live slots. The tick materializes 100/min so the control plane never changes." % (count, role, COMPUTE_INFLIGHT)}



VOICES = {
    "student": "You are Student, supervisor of Khalid's JustHodl factory. You pick bounded experiments, keep verified skills, and dispatch Coder, Researcher, Investor, Deployer. You already run a 64-case protected coding exam. You do not train weights or place orders.",
    "coder": "You are Coder on JustHodl. You propose restricted repair programs against protected tests. You do not get IAM, AWS console, or unbounded generation. Chart Pro, volume, QR tape, and factory code are in-scope when asked.",
    "researcher": "You are Researcher. You validate public sources, provenance, delayed tape, OFR, FRED, Yahoo, Binance. You never invent a print.",
    "investor": "You are Investor. Research forecasts only for SPY QQQ IWM TLT GLD BTC. Livermore, Wyckoff, Soros, Druckenmiller are principle cards. No broker orders. CLUB WALL locks Monday 09:30 ET.",
    "deployer": "You are Deployer. You queue exact artifacts for Khalid. You never ship yourself.",
    "livermore": "You are the Livermore principle card. Trend discipline. Do not average losers. Answer as that doctrine applied to the question.",
    "wyckoff": "You are the Wyckoff principle card. Accumulation, markup, distribution, markdown. Test the hypothesis; do not preach.",
    "soros": "You are the Soros principle card. Reflexivity: flows, expectations, prices. Test the feedback loop.",
    "druckenmiller": "You are the Druckenmiller principle card. Preserve capital. Size only with evidence.",
}


def _snapshot_text(state):
    if not isinstance(state, dict):
        return "state unavailable"
    wall = state.get("wall") or {}
    book = state.get("skillbook") or state.get("verified_skills") or []
    exam = ((state.get("exams") or {}).get("coding") or {})
    return "skills=%s exam=%s wall_graded=%s pending=%s gen=%s" % (
        len(book) if isinstance(book, list) else book, exam, wall.get("graded"), wall.get("pending"), state.get("generation"))




CRUMB_NOTE = re.compile(
    r"(?i)^(do you already|let me know if|we.?ll bypass|how do i run|would i pull|"
    r"what about|can you|wait,?|hold on|ok\b|okay\b|thanks\b|i think it)"
)


def _usable_note(note):
    text = " ".join(str((note or {}).get("text") or "").split())
    if len(text) < 48 or text.count(" ") < 7:
        return False
    if CRUMB_NOTE.search(text):
        return False
    if text.endswith("?") and len(text) < 96:
        return False
    return True


def _outside_facts(store, state):
    bits = []
    factory = state if isinstance(state, dict) else {}
    wall = factory.get("wall") or {}
    skills = factory.get("skillbook") or factory.get("verified_skills") or []
    exam = ((factory.get("exams") or {}).get("coding") or factory.get("coding_exam") or {})
    bits.append("factory wall pending=%s graded=%s" % (wall.get("pending"), wall.get("graded")))
    bits.append("verified_skills=%s" % (len(skills) if isinstance(skills, list) else skills))
    if exam:
        bits.append("coding_exam=%s" % json.dumps(exam)[:280])
    agents = factory.get("agents") or []
    if agents:
        bits.append("roster=%s" % ",".join(str(a.get("id") or a) for a in (agents[:8] if isinstance(agents, list) else [])))
    ai, _ = store.read(store.public, "data/ai.json")
    ai = ai if isinstance(ai, dict) else {}
    mr = ai.get("market_read") or {}
    if mr.get("stances"):
        bits.append("fleet_stances=%s" % json.dumps(mr.get("stances"))[:400])
    if mr.get("generated_at"):
        bits.append("fleet_read_at=%s" % mr.get("generated_at"))
    pipe = ai.get("pipeline") or {}
    bits.append("pipeline=%s retrieval=%s" % (pipe.get("status"), pipe.get("retrieval_endpoint")))
    for key in ("data/risk-gate.json", "data/khalid-risk.json", "data/verdict.json", "data/ai-factory.json"):
        try:
            doc, _ = store.read(store.public, key)
        except Exception:
            doc = None
        if not isinstance(doc, dict) or not doc:
            continue
        keep = {k: doc.get(k) for k in ("posture", "sizing", "stance", "verdict", "regime", "as_of", "status", "gate", "generation") if k in doc}
        if keep:
            bits.append("%s %s" % (key.split("/")[-1], json.dumps(keep)[:280]))
    return " | ".join(bits)[:3500]


def _http_json(url, timeout=6, headers=None):
    hdr = {"User-Agent": "JustHodlResearch/1.0 (https://justhodl.ai; factory desk)", "Accept": "application/json"}
    if headers:
        hdr.update(headers)
    req = urllib.request.Request(url, headers=hdr)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _look_outside(question):
    """Live public look. Missing source = missing, never invented. Private notes never sent."""
    q = " ".join((question or "").split())[:180]
    if not q:
        return []
    hits = []
    t0 = time.time()
    quoted = urllib.parse.quote(q)
    try:
        data = _http_json(
            "https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch=%s&srlimit=3&format=json" % quoted,
            timeout=6,
        )
        for row in ((data.get("query") or {}).get("search") or [])[:3]:
            title = row.get("title") or ""
            snippet = re.sub(r"<[^>]+>", "", row.get("snippet") or "")
            snippet = re.sub(r"\s+", " ", snippet).strip()
            if title:
                hits.append({
                    "source": "wikipedia",
                    "title": title,
                    "url": "https://en.wikipedia.org/wiki/" + title.replace(" ", "_"),
                    "snippet": snippet[:320],
                })
    except Exception:
        pass
    low = q.lower()
    if time.time() - t0 < 8:
        try:
            data = _http_json(
                "https://api.duckduckgo.com/?q=%s&format=json&no_html=1&skip_disambig=1" % quoted,
                timeout=5,
            )
            if data.get("AbstractText") or data.get("Abstract"):
                hits.append({
                    "source": "duckduckgo",
                    "title": data.get("Heading") or q,
                    "url": data.get("AbstractURL") or "",
                    "snippet": (data.get("AbstractText") or data.get("Abstract") or "")[:400],
                })
            for topic in (data.get("RelatedTopics") or [])[:3]:
                if isinstance(topic, dict) and topic.get("Text"):
                    hits.append({
                        "source": "duckduckgo",
                        "title": (topic.get("FirstURL") or "related").rsplit("/", 1)[-1],
                        "url": topic.get("FirstURL") or "",
                        "snippet": topic.get("Text")[:300],
                    })
        except Exception:
            pass
    if time.time() - t0 < 8 and any(w in low for w in ("code", "model", "agent", "learn", "huggingface", "llm", "train")):
        try:
            models = _http_json("https://huggingface.co/api/models?search=%s&limit=3" % urllib.parse.quote(q[:80]), timeout=5)
            if isinstance(models, list):
                for model in models[:3]:
                    mid = model.get("modelId") or model.get("id") or ""
                    if not mid:
                        continue
                    hits.append({
                        "source": "huggingface",
                        "title": mid,
                        "url": "https://huggingface.co/" + mid,
                        "snippet": "downloads=%s tags=%s" % (model.get("downloads"), (model.get("tags") or [])[:5]),
                    })
        except Exception:
            pass
    if time.time() - t0 < 8 and any(w in low for w in ("code", "github", "python", "chart", "lambda", "agent")):
        try:
            data = _http_json(
                "https://api.github.com/search/repositories?q=%s&per_page=3" % urllib.parse.quote(q[:80]),
                timeout=5,
                headers={"Accept": "application/vnd.github+json"},
            )
            for repo in (data.get("items") or [])[:3]:
                hits.append({
                    "source": "github",
                    "title": repo.get("full_name") or "",
                    "url": repo.get("html_url") or "",
                    "snippet": (repo.get("description") or "")[:300],
                })
        except Exception:
            pass
    seen = set()
    out = []
    for hit in hits:
        url = hit.get("url") or hit.get("title")
        if url in seen:
            continue
        seen.add(url)
        out.append(hit)
    return out[:8]


def _bank_research(store, question, hits):
    if not hits:
        return
    now = iso(store.clock())
    store.immutable(
        store.private,
        "factory/fleet/learn/research/" + digest(question + now)[:16] + ".json",
        {"schema_version": "factory-research.v1", "at": now, "q": question[:500], "hits": hits, "n": len(hits)},
    )


def _public_think(question, facts):
    """GLM on public look + warehouse only. Private Brain notes never leave the box. No Anthropic."""
    try:
        from llm_router import ZAI_BASE_URL, GLM_REASON, _zai_key
        key = (_zai_key() or "").strip()
    except Exception:
        return ""
    if not key:
        return ""
    payload = {
        "model": GLM_REASON,
        "max_tokens": 500,
        "messages": [
            {"role": "system", "content": (
                "You are JustHodl's outside reasoner. You see a live public look (Wikipedia, search, GitHub, HuggingFace) "
                "plus warehouse scores. You never see private notes. Answer Khalid. Cite sources by name. No orders, no IAM."
            )},
            {"role": "user", "content": "QUESTION:\n%s\n\nPUBLIC LOOK + WAREHOUSE:\n%s" % (question[:1500], facts[:3500])},
        ],
    }
    req = urllib.request.Request(
        ZAI_BASE_URL.rstrip("/") + "/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + key},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode())
        msg = ((body.get("choices") or [{}])[0].get("message") or {})
        return (msg.get("content") or msg.get("reasoning_content") or "").strip()[:2200]
    except Exception:
        return ""


LEARN_TRACKS = {
    "code": {
        "title": "How to learn to code — then get good",
        "agent": "coder",
        "why": "First learn how to learn. Then climb a public engineering ladder. Keep only what grades on the protected exam. No weight training.",
        "stages": [
            {
                "id": "learn-how-to-learn",
                "title": "Stage 0 — How to learn how to code",
                "pages": ["Deliberate practice", "Test-driven development", "Rubber duck debugging"],
                "drill": "Do not binge tutorials. Pick one failing protected-exam case. Restate the failure in one sentence. Write a smaller failing test. That loop is how Microsoft-level engineers actually get good: feedback, not videos.",
            },
            {
                "id": "language",
                "title": "Stage 1 — Language as a tool",
                "pages": ["Python (programming language)", "Software documentation", "Readability"],
                "drill": "Read 40 lines of JustHodl Python. Name every identifier. If you cannot, the code is the lesson.",
            },
            {
                "id": "correctness",
                "title": "Stage 2 — Correctness before cleverness",
                "pages": ["Unit testing", "Debugging", "Code coverage"],
                "drill": "A repair that does not raise the protected exam is not a skill. Green tests or it did not happen.",
            },
            {
                "id": "cs",
                "title": "Stage 3 — Structures and cost",
                "pages": ["Data structure", "Algorithm", "Time complexity"],
                "drill": "For the next factory patch, state O() of the hot path. If you cannot, you are guessing.",
            },
            {
                "id": "collab",
                "title": "Stage 4 — Work like a team that ships",
                "pages": ["Git", "Code review", "Software versioning"],
                "drill": "One bounded diff. One reason. One test. That is a Microsoft review, not a dump.",
            },
            {
                "id": "production",
                "title": "Stage 5 — Production craft",
                "pages": ["Software design", "Reliability engineering", "Site reliability engineering"],
                "drill": "Missing input stays missing. No fake zeros. That is already JustHodl doctrine; now treat it as an SLO.",
            },
            {
                "id": "caliber",
                "title": "Stage 6 — Senior-caliber taste",
                "pages": ["Software quality", "Abstraction (computer science)", "Application programming interface"],
                "drill": "Prefer a smaller interface that cannot lie. If a helper exists for one call, inline it. You are not done until a stranger can grade the patch from the exam alone.",
            },
        ],
    },
    "markets": {
        "title": "Financial markets — stocks, bonds, tape",
        "agent": "investor",
        "pages": ["Stock market", "Bond (finance)", "Yield curve"],
        "why": "Public market structure plus the warehouse delayed tape. Research only — no broker orders.",
    },
    "investing": {
        "title": "Investing doctrine",
        "agent": "investor",
        "pages": ["Jesse Livermore", "Wyckoff method", "George Soros", "Stanley Druckenmiller"],
        "why": "Principle cards from the public record. Evidence before size. No orders.",
    },
}


def _detect_tracks(text, target):
    low = " ".join((text or "").lower().split())
    asked = any(w in low for w in ("learn", "study", "teach", "curriculum", "go look", "how to code", "how to invest"))
    if not asked:
        return []
    found = []
    if target == "coder" or any(w in low for w in ("code", "coding", "python", "unit test", "git", "program")):
        found.append("code")
    if any(w in low for w in ("market", "stock", "bond", "yield", "tape", "spy", "qqq")):
        found.append("markets")
    if target in ("investor", "livermore", "wyckoff", "soros", "druckenmiller") or any(
        w in low for w in ("invest", "livermore", "wyckoff", "soros", "druckenmiller")
    ):
        found.append("investing")
    if not found:
        found = ["code", "markets", "investing"]
    out = []
    for name in ("code", "markets", "investing"):
        if name in found:
            out.append(name)
    return out[:3]


def _wiki_summary(title):
    slug = title.replace(" ", "_")
    url = "https://en.wikipedia.org/api/rest_v1/page/summary/" + urllib.parse.quote(slug, safe="()_,-")
    data = _http_json(url, timeout=5)
    extract = re.sub(r"\s+", " ", (data.get("extract") or "")).strip()
    page = ((data.get("content_urls") or {}).get("desktop") or {}).get("page") or ("https://en.wikipedia.org/wiki/" + slug)
    if not extract:
        raise RuntimeError("empty_summary")
    return {"source": "wikipedia", "title": data.get("title") or title, "url": page, "snippet": extract[:520]}


def _learn_track(store, track, state):
    spec = LEARN_TRACKS[track]
    stage = None
    stage_idx = 0
    stages = spec.get("stages") or []
    if stages:
        cur_key = "factory/fleet/learn/%s/cursor.json" % track
        row, etag = store.read(store.private, cur_key)
        stage_idx = int((row or {}).get("stage") or 0)
        if stage_idx >= len(stages):
            stage_idx = 0
        stage = stages[stage_idx]
        pages = stage.get("pages") or []
    else:
        cur_key, etag = None, None
        pages = spec.get("pages") or []
    hits = []
    t0 = time.time()
    for page in pages:
        if time.time() - t0 > 10:
            break
        try:
            hits.append(_wiki_summary(page))
        except Exception:
            try:
                hits.extend(_look_outside(page)[:1])
            except Exception:
                pass
    if track == "code" and stage_idx == 0 and time.time() - t0 < 10:
        try:
            repo = _http_json("https://api.github.com/repos/ossu/computer-science", timeout=5,
                              headers={"Accept": "application/vnd.github+json"})
            hits.append({
                "source": "github",
                "title": repo.get("full_name") or "ossu/computer-science",
                "url": repo.get("html_url") or "https://github.com/ossu/computer-science",
                "snippet": (repo.get("description") or "Open Source Society University — CS path")[:300],
            })
        except Exception:
            pass
    if spec.get("hf") and time.time() - t0 < 10:
        try:
            models = _http_json(
                "https://huggingface.co/api/models?search=%s&limit=3" % urllib.parse.quote(spec["hf"]),
                timeout=5,
            )
            if isinstance(models, list):
                for model in models[:3]:
                    mid = model.get("modelId") or model.get("id") or ""
                    if mid:
                        hits.append({
                            "source": "huggingface",
                            "title": mid,
                            "url": "https://huggingface.co/" + mid,
                            "snippet": "downloads=%s tags=%s" % (model.get("downloads"), (model.get("tags") or [])[:5]),
                        })
        except Exception:
            pass
    warehouse = _outside_facts(store, state) if track in ("markets", "investing") else ""
    now = iso(store.clock())
    title = (stage or {}).get("title") or spec["title"]
    why = (stage or {}).get("drill") or spec.get("why") or ""
    lesson = {
        "schema_version": "factory-lesson.v1",
        "track": track,
        "stage": (stage or {}).get("id"),
        "stage_idx": stage_idx if stages else None,
        "title": title,
        "why": why,
        "at": now,
        "hits": hits,
        "warehouse": warehouse[:800],
        "n": len(hits),
        "next": (stages[stage_idx + 1]["id"] if stages and stage_idx + 1 < len(stages) else "repeat-from-stage-0"),
    }
    try:
        store.immutable(store.private, "factory/fleet/learn/%s/%s.json" % (track, digest(track + now)[:16]), lesson)
    except Exception:
        pass
    if cur_key:
        try:
            store.put(store.private, cur_key, {"stage": stage_idx + 1, "last": (stage or {}).get("id"), "updated_at": now},
                      etag=etag, absent=etag is None)
        except Exception:
            pass
    return lesson


def _brain_chat(store, target, text, state, owner):
    """Inside = SageMaker Brain. Outside = live public look, then warehouse. Never Anthropic. Notes stay private."""
    if not owner:
        return "Owner Brain chat only.", "guest-blocked"
    pipe, _ = store.read(store.private, "ai/pipeline/state.json")
    pipe = pipe if isinstance(pipe, dict) else {}
    ep = pipe.get("retrieval_endpoint") or pipe.get("embedding_endpoint")
    clf = pipe.get("classifier_endpoint")
    ds_id = pipe.get("dataset_id")
    tracks = _detect_tracks(text, target)
    lessons = []
    hits = []
    if tracks:
        for name in tracks:
            try:
                lesson = _learn_track(store, name, state)
            except Exception:
                lesson = None
            if lesson:
                lessons.append(lesson)
                hits.extend(lesson.get("hits") or [])
    else:
        hits = _look_outside(text)
    try:
        _bank_research(store, text, hits)
    except Exception:
        pass
    outside = _outside_facts(store, state)
    lines = []
    model = "brain+look"
    if not ep:
        lines.append("INSIDE: Brain retrieval is not InService.")
    else:
        try:
            import brain_dataset as bd
            import sm_hub
            rt = boto3.client("sagemaker-runtime", region_name="us-east-1",
                              config=Config(connect_timeout=3, read_timeout=15, retries={"max_attempts": 2}))
            vecs = sm_hub.embed_texts(rt, ep, [text[:1500]])
            vec = vecs[0] if vecs else None
        except Exception as exc:
            vec = None
            lines.append("INSIDE: embedding endpoint %s failed (%s)." % (ep, type(exc).__name__))
        ranked = []
        notes = []
        if vec:
            model = ep
            if clf:
                try:
                    pred = sm_hub.predict_csv(rt, clf, [vec])
                    man = None
                    if ds_id:
                        man, _ = store.read(store.private, "ai/datasets/brain/%s/manifest.json" % ds_id)
                    labels = (man or {}).get("labels") or bd.CATS
                    p0 = pred[0] if pred else None
                    if isinstance(p0, list) and labels and len(p0) == len(labels):
                        ranked = sorted(zip(labels, [float(x) for x in p0]), key=lambda kv: -kv[1])
                except Exception:
                    ranked = []
            if ds_id:
                try:
                    notes = bd.nearest_notes(store.s3, store.private, ds_id, ep, vec, k=16) or []
                except Exception:
                    notes = []
        usable = [n for n in notes if _usable_note(n)][:4]
        lines.append("INSIDE — your Brain `%s`" % ep)
        if ranked:
            lines.append("Classifier: " + ", ".join("%s %.0f%%" % (lab, p * 100) for lab, p in ranked[:4]))
        if usable:
            for n in usable:
                snippet = " ".join(str(n.get("text") or "").split())[:200]
                lines.append("• [%s · %.2f] %s" % (n.get("label") or "note", float(n.get("similarity") or 0), snippet))
        else:
            lines.append("No usable notes (chat crumbs filtered).")
    if lessons:
        lines.append("LEARNED THIS TURN — left the box on purpose.")
        for lesson in lessons:
            head = (lesson.get("title") or (lesson.get("track") or "").upper())
            lines.append(head)
            if lesson.get("why"):
                lines.append(lesson["why"])
            if lesson.get("next"):
                lines.append("Next stage: %s" % lesson["next"])
            for hit in (lesson.get("hits") or [])[:4]:
                lines.append("• [%s] %s — %s %s" % (hit.get("source"), hit.get("title"), (hit.get("snippet") or "")[:220], hit.get("url") or ""))
            if lesson.get("warehouse"):
                lines.append("Warehouse for this track: " + lesson["warehouse"][:360])
    lines.append("OUTSIDE — live look beyond this system. Cited or missing, never invented.")
    if lessons:
        lines.append("Curriculum look is under LEARNED. Not a second scrape.")
    elif hits:
        for hit in hits[:6]:
            lines.append("• [%s] %s — %s %s" % (hit.get("source"), hit.get("title"), (hit.get("snippet") or "")[:180], hit.get("url") or ""))
    else:
        lines.append("Live look returned nothing this turn. Declared gap, not a fake page.")
    if outside:
        lines.append("WAREHOUSE — " + outside[:500])
    public_ctx = json.dumps(hits)[:2200] + "\n" + (outside or "")
    think = _public_think(text, public_ctx) if hits else ""
    if think:
        lines.append("THINKING")
        lines.append(think)
    elif hits:
        lines.append("THINKING: I left the box and pulled the citations above. Your notes stayed private. Next question can go narrower (a file, a ticker, a name).")
    else:
        lines.append("THINKING: Outside look was empty and I will not hallucinate. Retry with a name, ticker, paper, or library.")
    return "\n".join(lines), model


def chat_post(store, agent, owner, body, policy):
    allowed = {"text", "to", "spawn", "role", "task"}
    if set(body) - allowed:
        raise Invalid("chat_schema_required")
    text = body.get("text")
    if not isinstance(text, str) or not text.strip() or len(text) > 4000:
        raise Invalid("chat_text_required")
    try:
        target = identifier(str(body.get("to") or "model"))
    except Invalid:
        target = "model"
    if target not in ROSTER:
        target = "model"
    spawn_n = 0
    if "spawn" in body and body.get("spawn") not in (None, "", 0, "0"):
        try:
            spawn_n = max(0, int(body.get("spawn") or 0))
        except (TypeError, ValueError):
            raise Invalid("spawn_count_required")
    spawned = None
    want = spawn_n
    if owner and not want and any(w in text.lower() for w in ("spawn", "create agents", "create workers", "hire")):
        want = 8
    if want:
        if not owner:
            raise Invalid("owner_invitation_required")
        spawned = spawn_workers(store, agent, {"count": want, "role": body.get("role") or "researcher", "task": body.get("task") or text}, policy)
    state, _ = store.read(store.public, "data/student-state.json")
    key = "factory/salon/chat/" + agent + ".json"
    log, etag = store.read(store.private, key)
    history = list((log or {}).get("messages") or [])
    reply, model = _brain_chat(store, target, text, state, owner)
    now = iso(store.clock())
    user_msg = {"id": "u-" + digest(text + now)[:12], "at": now, "from": agent, "to": target, "role": "owner" if owner else "guest", "text": text.strip()}
    bot_msg = {"id": "a-" + digest(reply + now)[:12], "at": now, "from": target, "to": agent, "role": "agent", "text": reply, "model": model,
               "spawn": (spawned or {}).get("created")}
    messages = (history + [user_msg, bot_msg])[-CHAT_KEEP:]
    store.put(store.private, key, {"schema_version": "factory-chat.v1", "agent": agent, "messages": messages, "updated_at": now, "model": model},
              etag=etag, absent=etag is None)
    store.immutable(store.private, "factory/fleet/learn/chat/" + user_msg["id"] + ".json",
                    {"kind": "chat", "at": now, "from": agent, "to": target, "text": text.strip()[:500], "model": model})
    snap = spawned or chat_snapshot(store, agent, owner).get("workers")
    return {"ok": True, "to": target, "reply": reply, "model": model, "messages": messages[-16:], "workers": snap}



def handle(event, method, path, body, store):
    invites, invite_etag = store.read(store.private, 'factory/control/invites.json')
    if not invites:
        raise Invalid('factory_not_initialized')
    agent, owner = actor(event, invites)
    policy, policy_etag = store.read(store.private, 'factory/control/policy.json')
    if not isinstance(body, dict) or len(canonical(body)) > 16384:
        raise Invalid('invalid_or_oversized_body')
    action = path.removeprefix('/factory/')
    if method == 'GET' and action == 'view':
        if set(body) - {'kind', 'id'}:
            raise Invalid('unknown_view_argument')
        kind = body.get('kind')
        mapping = {'state': 'data/student-state.json', 'mirror': 'student-state.json',
            'board': 'factory/salon/board.json', 'season': 'factory/salon/season.json',
            'scoreboard': 'factory/scoreboard.json', 'exams': 'factory/exams/index.json',
            'wall': 'factory/salon/wall.jsonl'}
        if kind == 'event':
            key = 'factory/salon/events/' + identifier(body.get('id')) + '.json'
        elif kind == 'trace':
            key = 'factory/traces/code/' + identifier(body.get('id')) + '.json'
        elif kind in mapping:
            key = mapping[kind]
        else:
            raise Invalid('view_not_allowed')
        if kind == 'wall':
            raw = store.s3.get_object(Bucket=store.public, Key=key)['Body'].read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise Invalid('wall_archive_view_required')
            return {'ok': True, 'raw': raw.decode(), 'content_type': 'application/x-ndjson'}
        value, _ = store.read(store.public, key)
        if value is None:
            raise Invalid('factory_view_not_ready')
        if kind in ('state', 'mirror'):
            verify_state(value)
        return {'ok': True, 'raw': canonical(value).decode(), 'content_type': 'application/json'}
    if method == 'GET' and action == 'sandbox':
        state, _ = store.read(store.public, 'data/student-state.json')
        verify_state(state)
        outer = state['outer_status']
        return {'ok': True, 'agent': agent, 'owner': owner, 'sandbox': {'funding': outer.get('funding', {}),
                'tape': outer.get('tape', {}), 'research_only': True}, 'policy': {'enabled': policy['enabled'],
                'gear_b_enabled': False, 'max_traces_per_day': policy['max_guest_traces_per_day']}}
    if method == 'GET' and action == 'chat':
        return chat_snapshot(store, agent, owner)
    if method != 'POST':
        raise Invalid('factory_action_not_allowed')
    if action == 'chat':
        return chat_post(store, agent, owner, body, policy)
    if action == 'spawn':
        if not owner:
            raise Invalid('owner_invitation_required')
        return spawn_workers(store, agent, body, policy)
    if action == 'control':
        if not owner or set(body) != {'enabled'} or type(body['enabled']) is not bool:
            raise Invalid('owner_pause_control_required')
        policy = {**policy, 'enabled': body['enabled'], 'changed_at': iso(store.clock()), 'changed_by': agent}
        store.put(store.private, 'factory/control/policy.json', policy, etag=policy_etag)
        return {'ok': True, 'enabled': policy['enabled']}
    if action == 'invites':
        if not owner or set(body) != {'uid', 'agent', 'enabled'} or type(body['enabled']) is not bool:
            raise Invalid('owner_invitation_required')
        name = identifier(body['agent'])
        if len(name) > 40:
            raise Invalid('agent_name_maximum_40_characters')
        if name in ('owner', 'student') or name.startswith('teacher-'):
            raise Invalid('reserved_agent_name')
        uid = body['uid']
        if not isinstance(uid, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', uid):
            raise Invalid('verified_user_id_required')
        rows = list(invites['allowlist'])
        if any(r['agent'] == name and r['uid'] != uid for r in rows):
            raise Invalid('agent_name_taken')
        rows = [r for r in rows if r['uid'] != uid]
        rows.append({'uid': uid, 'agent': name, 'enabled': body['enabled'], 'invited_at': iso(store.clock())})
        if len(rows) > min(10, invites.get('capacity', 10)):
            raise Invalid('initial_invitation_cap')
        store.put(store.private, 'factory/control/invites.json', {**invites, 'allowlist': rows}, etag=invite_etag)
        return {'ok': True, 'agent': name, 'enabled': body['enabled'], 'message_sent': False}
    if policy.get('enabled') is not True:
        raise Invalid('factory_paused')
    if action == 'predictions':
        season, _ = store.read(store.private, 'factory/control/season.json')
        if season.get('calendar_review_required') is not False:
            raise Invalid('season_calendar_not_frozen')
        prediction = validate_prediction(body, season, store.clock(), agent)
        if prediction['price_source'] != season['price_sources'][prediction['symbol']]:
            raise Invalid('season_price_source_required')
        # Exactly one immutable entry per agent/week/symbol, irrespective of a supplied ID.
        event_id = prediction['week'] + '-' + agent + '-' + prediction['symbol']
        prediction['submitted_id'] = prediction['id']
        prediction['id'] = event_id
        store.immutable(store.private, 'factory/salon/accepted/' + event_id + '.json', prediction)
        return {'ok': True, 'id': event_id, 'status': 'locked', 'permalink': '/ai.html#factory-event=' + event_id}
    if action == 'traces':
        if set(body) != {'domain', 'task', 'provenance', 'solution_notes'}:
            raise Invalid('trace_schema_required')
        if body['domain'] not in ('code', 'math', 'tape', 'sec') or not isinstance(body['task'], dict):
            raise Invalid('trace_domain_required')
        provenance = body['provenance']
        if not isinstance(provenance, dict) or set(provenance) != {'license', 'source'}:
            raise Invalid('trace_provenance_required')
        if provenance['license'] not in ('CC0-1.0', 'MIT', 'Apache-2.0', 'BSD-3-Clause', 'original'):
            raise Invalid('trace_license_not_allowed')
        if not isinstance(provenance['source'], str) or len(provenance['source']) > 512:
            raise Invalid('trace_source_required')
        if not isinstance(body['solution_notes'], str) or len(body['solution_notes']) > 4000:
            raise Invalid('short_solution_notes_required')
        text = canonical(body).decode().lower()
        if any(x in text for x in ('iam:', 'putrule', 'createrole', 'api.openai.com', 'api.anthropic.com', 'access_key', 'secret_key')):
            raise Invalid('void_trace_prohibited_capability')
        trace_id = agent + '-' + digest(body)[:32]
        previous, _ = store.read(store.private, 'factory/quarantine/' + trace_id + '.json')
        if previous:
            return {'ok': True, 'id': trace_id, 'status': 'already_received'}
        admit_quota(store, agent, min(10, int(policy.get('max_guest_traces_per_day', 0))))
        store.immutable(store.private, 'factory/quarantine/' + trace_id + '.json', {'schema_version': 'factory-submission.v1',
            'id': trace_id, 'agent': agent, 'received_at': iso(store.clock()), 'trace': body})
        return {'ok': True, 'id': trace_id, 'status': 'quarantined_for_independent_check'}
    raise Invalid('factory_action_not_allowed')


def route(event, method, path, body):
    store = Store(boto3.client('s3', region_name='us-east-1', config=CFG),
        os.environ.get('AI_PRIVATE_BUCKET', 'justhodl-ai-857687956942'),
        os.environ.get('AI_PUBLIC_BUCKET', 'justhodl-dashboard-live'), lambda: datetime.now(timezone.utc))
    try:
        return 200, handle(event, method, path, body, store)
    except Invalid as exc:
        reason = str(exc)
        status = 403 if reason in ('invitation_required', 'verified_factory_identity_required', 'owner_pause_control_required', 'owner_invitation_required') else 429 if 'limit' in reason else 400
        return status, {'ok': False, 'error': reason}
    except Conflict:
        return 409, {'ok': False, 'error': 'immutable_entry_or_concurrent_update_conflict'}
    except Exception as exc:
        return 500, {'ok': False, 'error': 'factory_internal', 'detail': type(exc).__name__ + ':' + str(exc)[:160]}
