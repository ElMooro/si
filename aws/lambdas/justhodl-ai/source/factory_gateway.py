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
from factory_evidence import SCHEMA_EVIDENCE, reading_receipt, validate_evidence
from factory_store import Conflict, Store
import factory_discipline
try:
    import factory_status
except ImportError:  # loaded by path (tests); the module sits next to this file
    import importlib.util as _ilu
    import os as _os
    _spec = _ilu.spec_from_file_location("factory_status", _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "factory_status.py"))
    factory_status = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(factory_status)
try:
    import factory_inference
except ImportError:
    import importlib.util as _ilu2
    import os as _os2
    _spec2 = _ilu2.spec_from_file_location("factory_inference", _os2.path.join(_os2.path.dirname(_os2.path.abspath(__file__)), "factory_inference.py"))
    factory_inference = _ilu2.module_from_spec(_spec2)
    _spec2.loader.exec_module(factory_inference)

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


def _fleet_meta(store):
    row, etag = store.read(store.private, "factory/fleet/meta.json")
    if not isinstance(row, dict):
        row = {"schema_version": "factory-fleet.v1", "declared": 0, "materialized": 0, "inflight": 0,
               "queued": 0, "retired": 0, "learn_bytes": 0, "compute_inflight_cap": COMPUTE_INFLIGHT}
    return row, etag


def drain_fleet(store):
    """Bounded materialization of declared cards (MATERIALIZE_PER_TICK per call); compute stays COMPUTE_INFLIGHT.
    Owned by the gateway per factory-doctrine.v1; the student tick never writes factory/fleet/*."""
    meta, etag = _fleet_meta(store)
    queued = max(0, int(meta.get("queued") or 0))
    if not queued or not etag:
        return meta
    take = min(MATERIALIZE_PER_TICK, queued)
    meta = {**meta, "queued": queued - take, "materialized": int(meta.get("materialized") or 0) + take,
            "inflight": min(COMPUTE_INFLIGHT, queued), "learn_bytes": int(meta.get("learn_bytes") or 0) + take * 64,
            "updated_at": iso(store.clock())}
    try:
        store.put(store.private, "factory/fleet/meta.json", meta, etag=etag, absent=False)
    except Conflict:
        pass
    return meta


def ranks_view(store):
    """Current chain of command from the authoritative student state (aliases only)."""
    try:
        state, _ = store.load_state()
    except Exception:  # noqa: BLE001
        state = None
    return (state or {}).get("ranks") or factory_discipline.default_ranks(store.clock())


def holdout_manifest_hash(store):
    """Digest of the frozen private holdout manifest (never its content). None until it is frozen."""
    manifest, _ = store.read(store.private, "factory/holdout/manifest.json")
    return digest(manifest) if isinstance(manifest, dict) else None


def evidence_contract(store):
    return {"schema_version": SCHEMA_EVIDENCE, "attach_to": "predictions.evidence (optional)",
            "holdout_manifest_hash": holdout_manifest_hash(store),
            "required": ["claim{type=weekly_forecast,text,horizon_days=5,falsifier}", "data{keys[{bucket,key,sha256}],data_cutoff}",
                         "holdout{manifest_hash,touched=false}", "grade_after{5=window.grade_after}", "checker=grader:market_v1", "id=sha256(claim,keys,alias)[:16]"],
            "rule": "no evidence, no learning: entries without an envelope are graded on the wall but never feed the skillbook",
            "data_keys_must_start_with": ["data/", "factory/"], "author": "taken from the verified identity, never from the body"}


def attach_evidence(store, agent, prediction, envelope):
    """Validate a guest's evidence envelope against the locked entry. Identity and window come from the server."""
    if not isinstance(envelope, dict):
        raise Invalid("evidence_envelope_invalid")
    doc = dict(envelope)
    doc["schema_version"] = SCHEMA_EVIDENCE
    doc["domain"] = "market"
    doc["author"] = {"kind": "student" if agent == "student" else "guest", "alias": agent}
    doc["checker"] = "grader:market_v1"
    claim = dict(doc.get("claim") or {})
    claim["type"] = "weekly_forecast"
    claim["horizon_days"] = 5
    doc["claim"] = claim
    doc["grade_after"] = {"5": prediction["window"]["grade_after"]}
    data = dict(doc.get("data") or {})
    data["data_cutoff"] = prediction["data_cutoff"]
    doc["data"] = data
    holdout = dict(doc.get("holdout") or {})
    holdout["touched"] = False if holdout.get("touched") in (False, None) else holdout["touched"]
    doc["holdout"] = holdout
    manifest_hash = holdout_manifest_hash(store)
    if manifest_hash is None and holdout.get("manifest_hash") is not None:
        raise Invalid("evidence_holdout_not_frozen_yet")
    from factory_evidence import evidence_id
    keys = [k.get("key") for k in (data.get("keys") or []) if isinstance(k, dict)]
    doc["id"] = evidence_id(str(claim.get("text") or "").strip(), keys, agent)
    return validate_evidence(doc, received_at=prediction["received_at"], holdout_hash=manifest_hash)


def settle_pending(store, agent):
    """Deliver completed owned-model answers into the chat log from the pending index. Outbox order (B07): the log write
    commits FIRST; only then is each record marked delivered and archived. A conflict leaves nothing acknowledged, so the
    next poll delivers again; duplicate delivery is prevented by the request id already present in the log."""
    key = "factory/salon/chat/" + agent + ".json"
    log, etag = store.read(store.private, key)
    if not isinstance(log, dict):
        log, etag = {"messages": []}, None
    messages = list(log.get("messages") or [])
    settled = []
    for pkey, pending, petag in factory_inference.list_pending(store, agent):
        state, text = factory_inference.resolve(store, pending)
        if state in ("queued", "running", "unknown"):
            if pending.get("state") != state:
                try:
                    store.put(store.private, pkey, dict(pending, state=state), etag=petag, absent=False)
                except Conflict:
                    pass
            continue
        body, model = (text, pending.get("origin")) if state == "done" else ("Owned model request %s %s: %s" % (pending["id"], state, text), "owned:" + state)
        if not any(m.get("request") == pending["id"] for m in messages):
            messages.append({"id": "a-" + digest(body + pending["id"])[:12], "at": iso(store.clock()), "from": "student", "to": agent, "role": "agent",
                             "text": body, "model": model, "request": pending["id"]})
        settled.append((pkey, dict(pending, state=state), petag))
    if not settled:
        return log
    log["messages"] = messages[-CHAT_KEEP:]
    log["updated_at"] = iso(store.clock())
    try:
        store.put(store.private, key, log, etag=etag, absent=etag is None)
    except Conflict:
        return log                                                     # nothing acknowledged; the next poll re-delivers
    for pkey, pending, petag in settled:                                # acknowledged only after the commit above
        pending.update(delivered=True, delivered_at=iso(store.clock()))
        try:
            store.put(store.private, pkey, pending, etag=petag, absent=False)
            factory_inference.archive_delivered(store, pkey, pending)
        except Conflict:
            pass
    return log


def chat_snapshot(store, agent, owner):
    settle_pending(store, agent)
    log, _ = store.read(store.private, "factory/salon/chat/" + agent + ".json")
    meta = drain_fleet(store)
    ranks = ranks_view(store)
    return {
        "ok": True, "agent": agent, "owner": owner, "roster": list(ROSTER), "live_model": "brain",
        "messages": (log or {}).get("messages", [])[-CHAT_KEEP:],
        "ranks": factory_discipline.projection(ranks),
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


from factory_doctrine import can_spawn, child_card

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
    # Rank comes from the chain of command the student tick maintains, never from the request body.
    ok, why = factory_discipline.check_spawn(ranks_view(store), agent, count)
    if not ok:
        raise Invalid(why)
    count = why if isinstance(why, int) else count
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
    # The student's role reads factory/queue/*; recruits are materialized there by the tick, bounded by ROSTER_CAP.
    store.immutable(store.private, "factory/queue/spawn-" + batch_id + ".json",
                    {"schema_version": "factory-spawn-request.v1", **batch})
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
    r"(?i)(do you already|let me know if|we.?ll bypass|how do i run|would i pull|"
    r"what about|^can you|^could you|wait,?|hold on|^ok\b|^okay\b|thanks\b|i think it|"
    r"that line should not|want me to write|let it run|you.?ll see a message|"
    r"thousands of lines of code|once it.?s done|not part of the dockerfile)"
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
    """A search is a reading receipt (factory-reading.v1): citable, never a lesson, never trainable."""
    if not hits:
        return
    now = iso(store.clock())
    store.immutable(store.private, "factory/fleet/reading/research/" + digest(question + now)[:16] + ".json",
                    reading_receipt("research", question[:200], hits, now, why="owner question; outside look"))


OUTSIDE_VOICE_SYSTEM = (
    "You are JustHodl's outside reasoner. You see a live public look (Wikipedia, search, GitHub, HuggingFace) "
    "plus warehouse scores. You never see private notes. Answer Khalid. Cite sources by name. No orders, no IAM."
)


def _public_think(question, facts, history=None):
    """Owner chat voice only, through the governed router (daily budget, on-demand gate, cost attribution).

    Never a grader, never a lesson source: nothing this returns is written as evidence. An empty string
    means the voice is gated or silent and the deterministic answer stands on its own.
    """
    try:
        from llm_router import complete
    except Exception:  # noqa: BLE001
        return ""
    turns = ""
    for m in (history or [])[-6:]:
        # F05: only the owner's own words leave the box; agent turns may carry Brain snippets and are never forwarded
        if m.get("role") != "owner":
            continue
        turns += "OWNER: %s\n" % str(m.get("text") or "")[:400]
    prompt = "RECENT TURNS:\n%s\nQUESTION:\n%s\n\nPUBLIC LOOK + WAREHOUSE:\n%s" % (turns[-2400:], question[:1500], facts[:3500])
    try:
        txt = complete(prompt, tier="reason", max_tokens=500, contains_proprietary=False,
                       system=OUTSIDE_VOICE_SYSTEM, on_demand=True, no_cache=True)
    except Exception:  # noqa: BLE001
        return ""
    return str(txt or "").strip()[:2200]


LEARN_TRACKS = {
    "code": {
        "title": "How to learn to code — then get good",
        "agent": "coder",
        "why": "First learn how to learn. Then climb a public engineering ladder. Keep only what grades on the protected exam. No weight training.",
        "stages": [
            {
                "id": "learn-how-to-learn",
                "title": "Stage 0 — How to learn how to code",
                "pages": ["K. Anders Ericsson", "Worked-example effect", "Test-driven development"],
                "drill": "Do not binge tutorials. Pick one failing protected-exam case. Restate the failure in one sentence. Write a smaller failing test. That loop is how Microsoft-level engineers actually get good: feedback, not videos.",
            },
            {
                "id": "language",
                "title": "Stage 1 — Language as a tool",
                "pages": ["Python (programming language)", "Software documentation", "Programming style"],
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
                "pages": ["Git", "Code review", "Version control"],
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
        "pages": ["Jesse Livermore", "Richard Wyckoff", "George Soros", "Stanley Druckenmiller"],
        "why": "Principle cards from the public record. Evidence before size. No orders.",
    },
}


def _detect_tracks(text, target):
    low = " ".join((text or "").lower().split())
    asked = any(w in low for w in ("learn", "study", "teach", "curriculum", "go look", "how to code", "how to invest"))
    if not asked:
        return []
    found = []
    if any(w in low for w in ("code", "coding", "python", "unit test", "git", "program", "engineer")):
        found.append("code")
    if any(w in low for w in ("market", "stock", "bond", "yield", "tape", "spy", "qqq")):
        found.append("markets")
    if any(w in low for w in ("invest", "livermore", "wyckoff", "soros", "druckenmiller")):
        found.append("investing")
    if not found:
        found = ["code"] if target == "coder" else (["investing"] if target in ("investor", "livermore", "wyckoff", "soros", "druckenmiller") else ["code"])
    out = []
    for name in ("code", "markets", "investing"):
        if name in found:
            out.append(name)
    return out[:1]


CHAT_WRAP = re.compile(r"(?i)^\s*(can you|could you|would you|do you|are you able to|are you|please|hey|hi|ok|okay)\b[\s,:]*")
STOP_WORDS = set("a an the of to for and or in on is are can you do does how what this that with from just".split())


def _intent(text, target):
    low = " ".join((text or "").lower().split())
    learn = any(w in low for w in ("learn", "study", "teach", "curriculum", "how to learn"))
    codeish = target == "coder" or any(w in low for w in ("code", "coding", "python", "program", "engineer", "lambda", "bug", "script", "function"))
    marketish = any(w in low for w in ("market", "stock", "bond", "yield", "tape", "spy", "qqq"))
    investish = target in ("investor", "livermore", "wyckoff", "soros", "druckenmiller") or any(
        w in low for w in ("invest", "livermore", "wyckoff", "soros", "druckenmiller")
    )
    if learn and codeish:
        return "learn-code"
    if learn and marketish:
        return "learn-markets"
    if learn and investish:
        return "learn-investing"
    if codeish:
        return "code"
    if investish:
        return "investing"
    if marketish:
        return "markets"
    return "lookup"


def _search_query(text, intent):
    if intent in ("code", "learn-code"):
        return "software engineering Python unit testing"
    if intent in ("markets", "learn-markets"):
        return "stock market bond yield curve"
    if intent in ("investing", "learn-investing"):
        return "value investing Jesse Livermore"
    q = CHAT_WRAP.sub("", text or "")
    q = re.sub(r"[?!.]+$", "", q)
    q = re.sub(r"(?i)\b(for me|please|thanks)\b", " ", q)
    q = " ".join(q.split())
    if len(q) < 6:
        return ""
    return q[:180]


def _relevant(hit, q):
    blob = ("%s %s" % ((hit or {}).get("title") or "", (hit or {}).get("snippet") or "")).lower()
    words = [w for w in re.findall(r"[a-z0-9]{4,}", (q or "").lower()) if w not in STOP_WORDS]
    if not words:
        return False
    return sum(1 for w in words if w in blob) >= min(2, len(words))


def _compose_answer(text, intent, hits, state):
    if intent in ("code", "learn-code"):
        return ("No live model inference runs on this chat route (no always-on endpoint by design). "
                "My code is produced in graded bursts: file `task: <what you want> tests: <python asserts>` and the next burst "
                "samples solutions from the owned model; only what passes the independent verifier is kept. "
                "Ask `status` for the real numbers (bursts, pass rate, kept rows, training jobs).")
    if intent in ("markets", "investing", "learn-markets", "learn-investing"):
        return (
            "Research only. Delayed warehouse tape plus public market structure. No orders. "
            "Ask a ticker or click Learn markets / Learn investing."
        )
    if hits:
        titles = ", ".join((h.get("title") or "") for h in hits[:3] if h.get("title"))
        return "I looked outside and kept only pages that match the question: %s. Narrower name, ticker, or file if you want a deeper look." % titles
    return "I will not invent a page. Ask a name, ticker, library, or file."


def _wiki_summary(title):
    slug = title.replace(" ", "_")
    url = "https://en.wikipedia.org/api/rest_v1/page/summary/" + urllib.parse.quote(slug, safe="()_,-")
    data = _http_json(url, timeout=4)
    extract = re.sub(r"\s+", " ", (data.get("extract") or "")).strip()
    page = ((data.get("content_urls") or {}).get("desktop") or {}).get("page") or ("https://en.wikipedia.org/wiki/" + slug)
    if not extract:
        raise RuntimeError("empty_summary")
    return {"source": "wikipedia", "title": data.get("title") or title, "url": page, "snippet": extract[:520]}


def _wiki_many(titles):
    from concurrent.futures import ThreadPoolExecutor
    hits = []
    titles = [t for t in (titles or []) if t][:4]
    if not titles:
        return hits
    with ThreadPoolExecutor(max_workers=4) as pool:
        futs = [pool.submit(_wiki_summary, t) for t in titles]
        for fut in futs:
            try:
                hits.append(fut.result(timeout=5))
            except Exception:
                pass
    return hits


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
    hits = _wiki_many(pages)
    if track == "code" and stage_idx == 0:
        try:
            repo = _http_json("https://api.github.com/repos/ossu/computer-science", timeout=4,
                              headers={"Accept": "application/vnd.github+json"})
            hits.append({
                "source": "github",
                "title": repo.get("full_name") or "ossu/computer-science",
                "url": repo.get("html_url") or "https://github.com/ossu/computer-science",
                "snippet": (repo.get("description") or "Open Source Society University — CS path")[:300],
            })
        except Exception:
            pass
    warehouse = _outside_facts(store, state) if track in ("markets", "investing") else ""
    now = iso(store.clock())
    title = (stage or {}).get("title") or spec["title"]
    why = (stage or {}).get("drill") or spec.get("why") or ""
    # Reading is not learning. A track step is a citable reading receipt (factory-reading.v1); a lesson
    # only exists as factory-evidence.v1 with a checker and a grade window, and none is written here.
    lesson = reading_receipt(track, title, hits, now, stage=(stage or {}).get("id"), why=why)
    lesson.update(stage_idx=stage_idx if stages else None, warehouse=warehouse[:800],
                  next=(stages[stage_idx + 1]["id"] if stages and stage_idx + 1 < len(stages) else "repeat-from-stage-0"))
    try:
        store.immutable(store.private, "factory/fleet/reading/%s/%s.json" % (track, digest(track + now)[:16]), lesson)
    except Exception:
        pass
    if cur_key and hits:
        try:
            store.put(store.private, cur_key, {"stage": stage_idx + 1, "last": (stage or {}).get("id"), "updated_at": now},
                      etag=etag, absent=etag is None)
        except Exception:
            pass
    return lesson


def _explicit_learn_command(text):
    """A curriculum step advances only on a direct order ("learn code", "study markets", "next stage"), never
    because a question happens to contain the words learn and code."""
    low = " ".join((text or "").lower().split())
    return low.startswith(("learn ", "study ", "next stage", "teach yourself", "go learn", "read up on")) or low in ("learn", "next stage")


def _brain_chat(store, target, text, state, owner, history=None):
    """Inside = SageMaker Brain. Outside = live public look, then warehouse. Never Anthropic. Notes stay private."""
    if not owner:
        return "Owner Brain chat only.", "guest-blocked"
    pipe, _ = store.read(store.private, "ai/pipeline/state.json")
    pipe = pipe if isinstance(pipe, dict) else {}
    ep = pipe.get("retrieval_endpoint") or pipe.get("embedding_endpoint")
    clf = pipe.get("classifier_endpoint")
    ds_id = pipe.get("dataset_id")
    intent = _intent(text, target)
    tracks = _detect_tracks(text, target) if _explicit_learn_command(text) else []
    if intent.startswith("learn-") and not _explicit_learn_command(text):
        intent = intent.split("-", 1)[-1]          # a question about learning is a question, not a lesson
    if intent.startswith("learn-") and not tracks:
        tracks = [intent.split("-", 1)[-1]]
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
    elif intent == "code":
        hits = _wiki_many(["Computer programming", "Python (programming language)", "Test-driven development"])
    else:
        query = _search_query(text, intent)
        if query:
            hits = [h for h in (_look_outside(query) or []) if _relevant(h, query)]
    try:
        _bank_research(store, text, hits)
    except Exception:
        pass
    skip_brain = bool(lessons or intent == "code")
    outside = _outside_facts(store, state) if not skip_brain else ""
    lines = []
    model = "brain+look"
    if skip_brain:
        lines.append("INSIDE — nearest-notes skipped (capability question or curriculum step; not a search of chat logs).")
        model = "no-inference:honest" if intent == "code" else ("reading-receipt:" + ((lessons[0].get("track") if lessons else intent) or "look"))
    elif not ep:
        lines.append("INSIDE: Brain retrieval is not InService.")
    else:
        try:
            import brain_dataset as bd
            import sm_hub
            rt = boto3.client("sagemaker-runtime", region_name="us-east-1",
                              config=Config(connect_timeout=2, read_timeout=8, retries={"max_attempts": 1}))
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
        lines.append("READ THIS TURN — reading receipts only; reading is not learning (a skill counts when an independent test passes).")
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
        lines.append("Curriculum look is under READ. Not a second scrape.")
    elif hits:
        for hit in hits[:6]:
            lines.append("• [%s] %s — %s %s" % (hit.get("source"), hit.get("title"), (hit.get("snippet") or "")[:180], hit.get("url") or ""))
    else:
        lines.append("Live look returned nothing this turn. Declared gap, not a fake page.")
    if outside:
        lines.append("WAREHOUSE — " + outside[:500])
    public_ctx = json.dumps(hits)[:2200] + "\n" + (outside or "")
    think = ""
    if not skip_brain and hits:
        think = _public_think(text, public_ctx, history)
    lines.append("VOICE — " + ("outside reasoner via llm_router (explains; never grades, never a lesson)" if think else "deterministic (router silent or not needed)"))
    lines.append(think or _compose_answer(text, intent, hits, state))
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
    # F06: creation only on an explicit `spawn` field or an imperative that starts the message and carries no negation
    low = " ".join(text.lower().split())
    imperative = low.startswith(("spawn ", "spawn", "hire ", "create agents", "create workers")) and not any(n in low for n in ("not ", "don't", "do not", "never", "?"))
    if owner and not want and imperative:
        m_count = re.search(r"\b(\d{1,3})\b", low)
        want = max(1, min(int(m_count.group(1)), 100)) if m_count else 8    # A14: honour an explicit count
    if want:
        if not owner:
            raise Invalid("owner_invitation_required")
        spawned = spawn_workers(store, agent, {"count": want, "role": body.get("role") or "researcher", "task": body.get("task") or text}, policy)
    state, _ = store.read(store.public, "data/student-state.json")
    key = "factory/salon/chat/" + agent + ".json"
    log, etag = store.read(store.private, key)
    history = list((log or {}).get("messages") or [])
    task_card = None
    pending = None
    if factory_status.is_task_request(text) and owner:
        # "task: ..." -> immutable card, routed to what can actually be graded/executed; nothing runs from chat text
        task_text, tests = factory_status.split_tests(text)
        task_card = factory_status.intake_task(store, agent, owner, task_text, tests=tests)
        reply = "Task %s recorded (%s, %s). Route: %s" % (task_card["id"], task_card["scope"], "gradable" if task_card["gradable"] else "ungraded", task_card["route"])
        model = "factory-task-intake"
    elif factory_inference.is_status_question(text):
        reply, model = factory_status.status_text(store) + "\n\n" + factory_status.capability_text(), "factory-status:objects"
    elif owner and not _explicit_learn_command(text) and (control := factory_inference.load_control(store)) and control.get("enabled"):
        # ordinary and coding questions go to the OWNED model (async endpoint, scale-to-zero); the answer lands on the next poll
        try:
            rt = boto3.client("sagemaker-runtime", region_name="us-east-1", config=Config(connect_timeout=3, read_timeout=15, retries={"max_attempts": 1}))
            pending = factory_inference.submit(store, rt, control, agent, text, history)
            reply = ("Same request already in flight (%s)." % pending["id"]) if pending.get("replay") else (
                "Sent to the owned model (%s), request %s. Cold start can take a few minutes when the endpoint is scaled to zero; the answer appears here when it lands." % (pending["origin"], pending["id"]))
            model = "owned:queued"
        except Exception as exc:  # noqa: BLE001
            pending = None
            reply = "Owned model unavailable right now (%s: %s). No canned answer substituted." % (type(exc).__name__, str(exc)[:160])
            model = "owned:unavailable"
    elif owner and not _explicit_learn_command(text):
        pending = None
        reply = ("The owned model is not connected on this route yet (factory/control/inference.json absent or disabled) -- no canned answer. "
                 "Use `task: ... tests: ...` for a graded run, or `status` for objects.")
        model = "owned:not-connected"
    else:
        pending = None
        reply, model = _brain_chat(store, target, text, state, owner, history)
    now = iso(store.clock())
    user_msg = {"id": "u-" + digest(text + now)[:12], "at": now, "from": agent, "to": target, "role": "owner" if owner else "guest", "text": text.strip(),
                "pending": pending}
    bot_msg = {"id": "a-" + digest(reply + now)[:12], "at": now, "from": target, "to": agent, "role": "agent", "text": reply, "model": model,
               "spawn": (spawned or {}).get("created"), "task": (task_card or {}).get("id")}
    messages = (history + [user_msg, bot_msg])[-CHAT_KEEP:]
    store.put(store.private, key, {"schema_version": "factory-chat.v1", "agent": agent, "messages": messages, "updated_at": now, "model": model},
              etag=etag, absent=etag is None)
    store.immutable(store.private, "factory/fleet/learn/chat/" + user_msg["id"] + ".json",
                    {"kind": "chat", "at": now, "from": agent, "to": target, "text": text.strip()[:500], "model": model})
    snap = spawned or chat_snapshot(store, agent, owner).get("workers")
    return {"ok": True, "to": target, "reply": reply, "model": model, "messages": messages[-16:], "workers": snap}



def accept_prediction(store, agent, body, *, dry_run=False):
    """The one door onto the wall, for guests (via handle) and for the student's own Monday entries (wall_post.py).
    Identity is the caller's responsibility; window, season, price source, immutability and evidence are enforced here.
    dry_run validates everything and writes nothing (the student's rehearsal)."""
    season, _ = store.read(store.private, 'factory/control/season.json')
    if season.get('calendar_review_required') is not False:
        raise Invalid('season_calendar_not_frozen')
    body = dict(body) if isinstance(body, dict) else body
    envelope = body.pop('evidence', None) if isinstance(body, dict) else None
    prediction = validate_prediction(body, season, store.clock(), agent)
    if prediction['price_source'] != season['price_sources'][prediction['symbol']]:
        raise Invalid('season_price_source_required')
    # Exactly one immutable entry per agent/week/symbol, irrespective of a supplied ID.
    event_id = prediction['week'] + '-' + agent + '-' + prediction['symbol']
    prediction['submitted_id'] = prediction['id']
    prediction['id'] = event_id
    evidence = attach_evidence(store, agent, prediction, envelope) if envelope is not None else None
    prediction['evidence_id'] = evidence['id'] if evidence else None
    prediction['evidence_hash'] = evidence['evidence_hash'] if evidence else None
    if dry_run:
        return {'ok': True, 'id': event_id, 'status': 'rehearsed', 'evidence_id': prediction['evidence_id'], 'learnable': bool(evidence)}
    store.immutable(store.private, 'factory/salon/accepted/' + event_id + '.json', prediction)
    if evidence:
        store.immutable(store.private, 'factory/evidence/market/' + evidence['id'] + '.json', evidence)
    return {'ok': True, 'id': event_id, 'status': 'locked', 'permalink': '/ai.html#factory-event=' + event_id,
            'evidence_id': prediction['evidence_id'], 'learnable': bool(evidence)}


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
                'gear_b_enabled': False, 'max_traces_per_day': policy['max_guest_traces_per_day']},
                'evidence_contract': evidence_contract(store)}
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
        return accept_prediction(store, agent, body)
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
