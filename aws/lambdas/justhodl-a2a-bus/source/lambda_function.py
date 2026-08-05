"""justhodl-a2a-bus v1.0 (ops 4378) — Agent-to-Agent Bus.

Spec: Perplexity (thread 0001, delivered via Khalid). Multi-turn threads
between AI agents with two server-enforced invariants:

  A. EVIDENCE-OR-REFUSAL — propose/critique/verify turns must carry
     evidence[] and every entry must RESOLVE (file -> repo raw 200 +
     optional snippet containment; url -> 2xx; log/s3 -> S3 key exists +
     optional containment). Unresolvable => rejected_no_evidence, thread
     does not advance (turn kept in rejected[] for audit).
  B. VERIFIER-QUORUM — resolve requires >=1 kind:"verify" verdict:
     "confirmed" turn from a provider != proposer. Self-agreement never
     resolves a thread.

Also: sha256 idempotent turn_ids; SSM-persisted per-provider breakers
(/justhodl/a2a/breaker/<p>) surviving cold starts; deadman sweep marking
to:"*" threads stalled after 30min silence; fan-out via llm_router with a
budget governor (MAX_TURNS_PER_THREAD, MAX_FANOUT_PER_INVOKE, fleet
llm_cost daily cap) so two agents can never ping-pong unbounded.

Actions (direct invoke): open_thread | post_turn | get_thread | resolve |
deadman_sweep | fanout_pending.
"""
import hashlib
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

from llm_router import council, classify_provider_error

try:
    from _sentry_lite import track_errors
except Exception:  # pragma: no cover
    def track_errors(f):
        return f

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
REPO_RAW = "https://raw.githubusercontent.com/ElMooro/si/main/"
THREADS = "data/a2a/threads/"
INBOX = "data/a2a/inbox/"
REGISTRY = "data/a2a/registry.json"
DECISIONS = "data/a2a/decisions.json"
STALLED = "data/a2a/stalled.json"
MAX_TURNS_PER_THREAD = int(os.environ.get("A2A_MAX_TURNS", "16"))
MAX_FANOUT_PER_INVOKE = 3
STALL_MIN = 30
EVIDENCE_KINDS = {"propose", "critique", "verify"}

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _put(key, obj):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")


def _http_ok(url, want=None):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-a2a"})
        with urllib.request.urlopen(req, timeout=8) as r:
            if not (200 <= r.status < 300):
                return False, f"status {r.status}"
            if want:
                body = r.read(65536).decode("utf-8", "replace")
                if want not in body:
                    return False, "snippet not found"
        return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:60]}"


def resolve_evidence(ev):
    """Invariant A resolver. Returns (all_ok, annotated_evidence)."""
    out, ok_all = [], True
    for e in (ev or [])[:8]:
        kind = (e.get("kind") or "").lower()
        ref = e.get("ref") or ""
        want = e.get("snippet")
        if kind == "file":
            ok, note = _http_ok(REPO_RAW + ref.lstrip("/"), want)
        elif kind == "url":
            ok, note = _http_ok(ref, want)
        elif kind in ("log", "s3"):
            try:
                body = s3.get_object(Bucket=S3_BUCKET,
                                     Key=ref)["Body"].read(131072)
                ok = (want is None) or (want.encode() in body)
                note = "ok" if ok else "snippet not found"
            except Exception as ex:
                ok, note = False, f"{type(ex).__name__}"
        else:
            ok, note = False, "unknown evidence kind"
        ok_all = ok_all and ok
        out.append({**e, "resolved": ok, "note": note})
    if not out:
        return False, out
    return ok_all, out


def _breaker_open(provider):
    try:
        v = json.loads(ssm.get_parameter(
            Name=f"/justhodl/a2a/breaker/{provider}")["Parameter"]["Value"])
        return float(v.get("open_until", 0)) > time.time()
    except Exception:
        return False


def _breaker_trip(provider, minutes=10):
    try:
        ssm.put_parameter(Name=f"/justhodl/a2a/breaker/{provider}",
                          Value=json.dumps(
                              {"open_until": time.time() + minutes * 60,
                               "tripped_at": _now()}),
                          Type="String", Overwrite=True)
    except Exception as e:
        print("breaker persist err:", str(e)[:60])


def _turn_id(thread_id, frm, seq):
    return hashlib.sha256(
        f"{thread_id}|{frm}|{seq}".encode()).hexdigest()[:16]


def _inbox_push(provider, thread_id):
    key = INBOX + provider + ".json"
    box = _get(key, {"provider": provider, "threads": []})
    if thread_id not in box["threads"]:
        box["threads"].append(thread_id)
        box["updated"] = _now()
        _put(key, box)


def _inbox_pop(provider, thread_id):
    key = INBOX + provider + ".json"
    box = _get(key, {"provider": provider, "threads": []})
    if thread_id in box["threads"]:
        box["threads"].remove(thread_id)
        box["updated"] = _now()
        _put(key, box)


def open_thread(ev):
    tid = ev.get("thread_id") or datetime.now(timezone.utc).strftime(
        "%m%d%H%M%S")
    t = {"thread_id": tid, "created_at": _now(),
         "topic": (ev.get("topic") or "")[:300], "status": "open",
         "turns": [], "rejected": [], "resolution": None}
    _put(THREADS + tid + ".json", t)
    first = ev.get("turn")
    res = {"ok": True, "thread_id": tid}
    if first:
        res["first_turn"] = post_turn({"thread_id": tid, **first})
    return res


def post_turn(ev):
    tid = ev["thread_id"]
    key = THREADS + tid + ".json"
    t = _get(key)
    if not t:
        return {"ok": False, "error": "thread not found"}
    if t.get("status") == "resolved":
        return {"ok": False, "error": "thread resolved"}
    if len(t["turns"]) >= MAX_TURNS_PER_THREAD:
        t["status"] = "stalled"
        _put(key, t)
        return {"ok": False, "error": "budget_exceeded: max turns"}
    seq = len(t["turns"]) + len(t["rejected"])
    frm = (ev.get("from") or "unknown").lower()
    turn = {"turn_id": _turn_id(tid, frm, seq), "ts": _now(),
            "from": frm, "to": ev.get("to") or "*",
            "kind": (ev.get("kind") or "question").lower(),
            "content": (ev.get("content") or "")[:8000],
            "evidence": ev.get("evidence") or [],
            "verdict": ev.get("verdict"),
            "delivered_via": ev.get("delivered_via")}
    if any(x["turn_id"] == turn["turn_id"] for x in t["turns"]):
        return {"ok": True, "idempotent": True, "turn_id": turn["turn_id"]}
    if turn["kind"] in EVIDENCE_KINDS:
        ok, annotated = resolve_evidence(turn["evidence"])
        turn["evidence"] = annotated
        if not ok:
            turn["status"] = "rejected_no_evidence"
            t["rejected"].append(turn)
            _put(key, t)
            return {"ok": False, "error": "rejected_no_evidence",
                    "turn_id": turn["turn_id"], "evidence": annotated}
    t["turns"].append(turn)
    _put(key, t)
    tgt = turn["to"]
    reg = _get(REGISTRY, {}).get("providers", {})
    targets = ([p for p in reg if p != frm] if tgt == "*"
               else ([tgt] if tgt != frm else []))
    for p in targets:
        if reg.get(p, {}).get("kind") != "human":
            _inbox_push(p, tid)
    # ops 4418: IMMEDIATE ping — no 15-minute wait. Any turn addressed to
    # Claude (or broadcast) wakes the backend agent right now.
    woke = False
    if frm != "claude" and not frm.startswith("claude"):
        if turn["to"] in ("claude", "claude-audit", "claude-backend", "*"):
            woke = _wake_claude(tid, f"{frm} posted {turn['kind']}")
            try:
                doc = _tasks()
                if tid not in doc["tasks"]:
                    doc["tasks"][tid] = {
                        "thread_id": tid, "created_at": _now(),
                        "state": "FILED", "history": [
                            {"state": "FILED", "by": frm, "ts": _now(),
                             "note": (turn.get("content") or "")[:200]}],
                        "title": (turn.get("content") or "")[:120]}
                    _task_put(doc)
            except Exception as _e:
                print("task init:", str(_e)[:60])
    return {"ok": True, "turn_id": turn["turn_id"],
            "queued_for": targets, "claude_woken": woke}


def resolve(ev):
    tid = ev["thread_id"]
    key = THREADS + tid + ".json"
    t = _get(key)
    if not t:
        return {"ok": False, "error": "thread not found"}
    proposer = next((x["from"] for x in t["turns"]
                     if x["kind"] == "propose"), None)
    quorum = [x for x in t["turns"]
              if x["kind"] == "verify"
              and (x.get("verdict") or "").lower() == "confirmed"
              and x["from"] != proposer]
    if not quorum:
        return {"ok": False, "error": "rejected_no_quorum",
                "detail": "needs kind:verify verdict:confirmed from a "
                          "provider != proposer (invariant B)"}
    t["status"] = "resolved"
    t["resolution"] = {"decision": (ev.get("decision") or "")[:2000],
                       "chosen_by": ev.get("by") or "unknown",
                       "verified_by": [q["from"] for q in quorum],
                       "ts": _now()}
    _put(key, t)
    led = _get(DECISIONS, {"decisions": []})
    led["decisions"].append({"thread_id": tid, "topic": t["topic"],
                             **t["resolution"]})
    _put(DECISIONS, led)
    return {"ok": True, "resolution": t["resolution"]}


def deadman_sweep(_ev=None):
    stalled = []
    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=THREADS,
                                  MaxKeys=200)
        for o in resp.get("Contents", []):
            t = _get(o["Key"])
            if not t or t.get("status") != "open" or not t["turns"]:
                continue
            last = t["turns"][-1]
            if last.get("to") not in ("*",) and last.get("to") is not None \
                    and last.get("to") != "":
                pass
            try:
                ts = datetime.fromisoformat(last["ts"])
                age_min = (datetime.now(timezone.utc) - ts).total_seconds() / 60
            except Exception:
                continue
            if age_min > STALL_MIN and last["to"] != "human":
                t["status"] = "stalled"
                _put(o["Key"], t)
                stalled.append({"thread_id": t["thread_id"],
                                "topic": t["topic"], "age_min": round(age_min),
                                "awaiting": last["to"], "ts": _now()})
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}
    if stalled:
        doc = _get(STALLED, {"stalled": []})
        doc["stalled"] = (doc["stalled"] + stalled)[-100:]
        _put(STALLED, doc)
    return {"ok": True, "newly_stalled": stalled}




def _extract_turn_json(raw):
    """Tolerant A2A turn extraction: strip code fences, then walk balanced
    top-level {...} candidates and take the first that parses and looks like
    a turn. Falls back to kind:question with raw content."""
    txt = (raw or "").replace("```json", "```").replace("```", "")
    n = len(txt)
    i = 0
    while i < n:
        if txt[i] == "{":
            depth, j = 0, i
            in_str, esc = False, False
            while j < n:
                c = txt[j]
                if in_str:
                    if esc:
                        esc = False
                    elif c == "\\":
                        esc = True
                    elif c == '"':
                        in_str = False
                elif c == '"':
                    in_str = True
                elif c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        try:
                            cand = json.loads(txt[i:j + 1])
                            if isinstance(cand, dict) and (
                                    "kind" in cand or "content" in cand):
                                cand.setdefault("kind", "question")
                                cand.setdefault(
                                    "content", (raw or "")[:2000])
                                cand.setdefault("evidence", [])
                                return cand
                        except Exception:
                            pass
                        break
                j += 1
            i = j + 1
        else:
            i += 1
    return {"kind": "question", "content": (raw or "")[:2000],
            "evidence": []}

A2A_SYSTEM = (
    "You are an agent on the JustHodl A2A bus. Read the thread JSON. Reply "
    "with ONLY one JSON object, no prose, no fences: {\"kind\": propose|"
    "critique|verify|agree|block|question, \"content\": str, \"evidence\":"
    " [{\"kind\": \"file\"|\"url\"|\"log\", \"ref\": str, "
    "\"snippet\": str}], \"verdict\": \"confirmed\"|\"refuted\"|null}."
    " INVARIANT A: verify/critique/propose turns REQUIRE resolvable "
    "evidence — file refs are repo paths on ElMooro/si main (example: "
    "{\"kind\":\"file\",\"ref\":\"insiders.html\",\"snippet\":"
    "\"Content-Security-Policy\"}), url refs must return 2xx, log refs "
    "are S3 keys like data/ai-council.json. Turns without resolvable "
    "evidence are rejected. CONTINUATION PROTOCOL: when you want Claude to "
    "act next, end content with a line \"NEXT_ACTIONS: ...\" listing "
    "concrete steps; Claude reads these each session and continues the "
    "AUDIT MANDATE (MUTUAL AUDIT CONSTITUTION, Khalid law): when auditing the other agent's work you MUST cover 5 dimensions — (1) PURPOSE: state what the engine/page is trying to accomplish before critiquing; (2) QUALITY vs an institutional Bloomberg/Koyfin bar, crediting strengths; (3) BUGS with severity+location+fix; (4) MISSING DATA SOURCES — think deeply about what named feeds/series/APIs/fleet-joins would add real edge; (5) MAX IMPROVEMENT — the best-in-world version, ranked roadmap. Ground every finding in live bytes/output (invariant A). The owner fixes; the auditor (non-proposer) verifies vs live and confirm-closes (invariant B). Credit where due; never fabricate. ""loop. HANDSHAKE PROTOCOL (Khalid law, no waiting): every task walks "
    "FILED -> ACK -> DONE -> VERIFIED -> PUBLISHED -> SEALED, and each step "
    "is pinged immediately. When you file work, Claude ACKs receipt at once; "
    "when Claude finishes it pings DONE; you VERIFY and ping back; Claude "
    "PUBLISHES (engine AND page) and pings; you confirm the published state "
    "matches your suggestions and post SEALED — only then is the task "
    "complete and you move to the next. Advance state with action:task_update {thread_id, state, note}. Check the board with action:get_tasks. "
    "CODE: you may also ship fixes via action:propose_patch "
    "{title, rationale, files:[{path,content}], evidence[]} -> becomes a "
    "real GitHub PR; Claude reviews+tests+merges; .github/ and aws/ops/ "
    "paths are denied by policy."
)


def fanout_pending(ev):
    """One bounded hop: for each queued provider inbox, call the provider
    with the thread, parse its JSON turn, post it back through invariant A."""
    reg = _get(REGISTRY, {}).get("providers", {})
    done, budget = [], MAX_FANOUT_PER_INVOKE
    for p, meta in reg.items():
        if budget <= 0:
            break
        # ops 4414: honour registry status — disabled providers get no fan-out
        # (GLM was disabled at ops 4394 but kept receiving calls and posting)
        if meta.get("status") in ("disabled", "quota_exhausted"):
            continue
        if meta.get("kind") == "human" or meta.get("transport") != "llm":
            continue
        if _breaker_open(p):
            done.append({"provider": p, "skipped": "breaker_open"})
            continue
        box = _get(INBOX + p + ".json", {"threads": []})
        for tid in list(box.get("threads", []))[:1]:
            budget -= 1
            t = _get(THREADS + tid + ".json")
            if not t:
                _inbox_pop(p, tid)
                continue
            slim = {k: t[k] for k in ("thread_id", "topic", "status")}
            slim["turns"] = [
                {**{k2: x.get(k2) for k2 in
                    ("from", "kind", "content", "verdict")},
                 "evidence": [{"kind": e.get("kind"), "ref": e.get("ref"),
                               "resolved": e.get("resolved")}
                              for e in (x.get("evidence") or [])[:4]]}
                for x in t["turns"][-8:]]
            ans = council("THREAD:\n" + json.dumps(slim)[:9000] +
                          "\n\nRespond as your next turn.",
                          providers=[p], system=A2A_SYSTEM,
                          max_tokens=1200)[p]
            if not ans.get("ok"):
                ec = classify_provider_error(ans.get("error") or "")
                _breaker_trip(p)
                r = post_turn({"thread_id": tid, "from": p, "to": "*",
                               "kind": "block",
                               "content": f"[auto] provider unavailable "
                                          f"({ec}): {ans.get('error')}"[:500]})
                done.append({"provider": p, "thread": tid,
                             "error_class": ec})
                _inbox_pop(p, tid)
                continue
            raw = ans.get("answer") or ""
            j = _extract_turn_json(raw)
            r = post_turn({"thread_id": tid, "from": p, "to": "*", **j})
            _inbox_pop(p, tid)
            done.append({"provider": p, "thread": tid,
                         "posted": r.get("ok"),
                         "rejected": r.get("error")})
    return {"ok": True, "fanout": done}




# ═══════════ v1.3 (ops 4388): propose_patch — agents ship code safely ═══════
# External agents (Perplexity, GLM, future seats) contribute code the only
# institutional way: as pull requests. propose_patch turns an authenticated
# bus call into a real branch + PR on ElMooro/si. Guardrails: path denylist
# (no workflow/CI tampering, no self-executing ops), size caps, per-agent
# open-PR quota. Claude reviews, tests, merges via ops — the human-grade
# gate. Merge -> auto-deploy pipeline does the rest.
GH_REPO = "ElMooro/si"
GH_TOKEN_SSM = "/justhodl/github/bus-pat"
PATCH_DENY = (".github/", "aws/ops/", "cloudflare/", "supabase/")
PATCH_DENY_EXEMPT = ("perplexity",)  # Khalid: full-push agents bypass path denylist (ledger still records)
# Ownership Arbitration (Khalid, 2026-08-05): protected artifacts cannot be
# patched by a non-owner without Khalid's ruling. Owner may still patch.
PROTECTED_ARTIFACTS = {"crisis.html": "claude", "liquidity.html": "claude",
                       "plumbing.html": "claude"}
PATCH_MAX_FILES = 8
PATCH_MAX_BYTES = 200_000
PATCH_MAX_OPEN_PRS = 3
_gh_token_cache = None


def _gh_token():
    global _gh_token_cache
    if _gh_token_cache is None:
        _gh_token_cache = ssm.get_parameter(
            Name=GH_TOKEN_SSM, WithDecryption=True)["Parameter"]["Value"]
    return _gh_token_cache


def _gh(path, method="GET", body=None):
    req = urllib.request.Request(
        "https://api.github.com" + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "token " + _gh_token(),
                 "Accept": "application/vnd.github+json",
                 "Content-Type": "application/json",
                 "User-Agent": "justhodl-a2a-bus"})
    with urllib.request.urlopen(req, timeout=25) as r:
        raw = r.read().decode()
        return json.loads(raw) if raw else {}


def propose_patch(ev):
    """Agent-authored code proposal -> branch a2a/<agent>-<id> + PR.

    ev: {from, title, rationale, files:[{path, content}], thread_id?,
        evidence[]}
    """
    agent = (ev.get("from") or "unknown").lower()
    title = (ev.get("title") or "").strip()[:120]
    rationale = (ev.get("rationale") or "").strip()
    files = ev.get("files") or []
    if not title or not rationale or not files:
        return {"ok": False,
                "error": "title, rationale and files[] required"}
    if len(files) > PATCH_MAX_FILES:
        return {"ok": False,
                "error": f"max {PATCH_MAX_FILES} files per patch"}
    total = 0
    for f in files:
        p = (f.get("path") or "").lstrip("/")
        c = f.get("content")
        if not p or c is None:
            return {"ok": False, "error": "each file needs path+content"}
        if ".." in p or p.startswith("/"):
            return {"ok": False, "error": f"illegal path {p}"}
        if agent not in PATCH_DENY_EXEMPT:
            for deny in PATCH_DENY:
                if p.startswith(deny):
                    return {"ok": False,
                            "error": f"path denied by policy: {p} "
                                     f"(denylist {PATCH_DENY})"}
        owner = PROTECTED_ARTIFACTS.get(p.split("/")[-1])
        if owner and owner != agent:
            return {"ok": False,
                    "error": f"ownership_protected: {p} is {owner}-owned "
                             f"and Khalid-protected; open an "
                             f"ownership-dispute thread for his ruling "
                             f"(agent {agent} may not overwrite it)"}
        total += len(str(c).encode("utf-8", "replace"))
    if total > PATCH_MAX_BYTES:
        return {"ok": False,
                "error": f"patch too large: {total}b > {PATCH_MAX_BYTES}b"}
    ok_ev, annotated = resolve_evidence(ev.get("evidence") or [])
    if not ok_ev:
        return {"ok": False, "error": "rejected_no_evidence",
                "evidence": annotated}
    try:
        open_prs = _gh(f"/repos/{GH_REPO}/pulls?state=open&per_page=50")
        mine = [p for p in open_prs
                if (p.get("head", {}).get("ref") or "")
                .startswith(f"a2a/{agent}-")]
        if len(mine) >= PATCH_MAX_OPEN_PRS:
            return {"ok": False,
                    "error": f"quota: {len(mine)} open PRs for {agent} "
                             f"(max {PATCH_MAX_OPEN_PRS}); await review"}
        main_sha = _gh(f"/repos/{GH_REPO}/git/ref/heads/main"
                       )["object"]["sha"]
        pid = hashlib.sha256(
            (agent + title + str(time.time())).encode()).hexdigest()[:8]
        branch = f"a2a/{agent}-{pid}"
        _gh(f"/repos/{GH_REPO}/git/refs", "POST",
            {"ref": "refs/heads/" + branch, "sha": main_sha})
        import base64
        for f in files:
            p = f["path"].lstrip("/")
            body = {"message": f"a2a patch {pid}: {p} (by {agent})",
                    "content": base64.b64encode(
                        str(f["content"]).encode()).decode(),
                    "branch": branch}
            try:
                cur = _gh(f"/repos/{GH_REPO}/contents/{p}?ref={branch}")
                if isinstance(cur, dict) and cur.get("sha"):
                    body["sha"] = cur["sha"]
            except Exception:
                pass
            _gh(f"/repos/{GH_REPO}/contents/{p}", "PUT", body)
        pr = _gh(f"/repos/{GH_REPO}/pulls", "POST",
                 {"title": f"[a2a/{agent}] {title}",
                  "head": branch, "base": "main",
                  "body": (f"Agent-proposed patch via A2A bus.\n\n"
                           f"**Author:** {agent}\n**Patch id:** {pid}\n"
                           f"**Thread:** {ev.get('thread_id') or '-'}\n\n"
                           f"## Rationale\n{rationale[:3000]}\n\n"
                           f"Evidence: {json.dumps(annotated)[:800]}\n\n"
                           f"Merge gate: Claude reviews + tests via ops; "
                           f"merge triggers auto-deploy.")})
        rec = {"patch_id": pid, "agent": agent, "title": title,
               "branch": branch, "pr": pr.get("number"),
               "pr_url": pr.get("html_url"),
               "files": [f["path"] for f in files],
               "ts": _now(), "status": "open"}
        led = _get("data/a2a/patches.json", {"patches": []})
        led["patches"] = (led["patches"] + [rec])[-100:]
        _put("data/a2a/patches.json", led)
        if ev.get("thread_id"):
            post_turn({"thread_id": ev["thread_id"], "from": agent,
                       "to": "claude", "kind": "propose",
                       "content": f"[patch {pid}] PR #{pr.get('number')} "
                                  f"{pr.get('html_url')} — {title}. "
                                  f"Rationale: {rationale[:500]}",
                       "evidence": ev.get("evidence") or []})
        return {"ok": True, **rec}
    except Exception as e:
        return {"ok": False,
                "error": f"{type(e).__name__}: {str(e)[:200]}"}
# ═══════════ end v1.3 ═══════════




# ═══════ ops 4418: HANDSHAKE PROTOCOL (Khalid's rule, no 15-min waiting) ═══
# Lifecycle every task walks, each step pinged immediately:
#   FILED      Perplexity posts work  -> bus WAKES Claude's agent instantly
#   ACK        Claude confirms receipt + "working on it"
#   DONE       Claude finishes, pings back
#   VERIFIED   Perplexity checks the work, pings Claude
#   PUBLISHED  Claude publishes (engine + page), pings back
#   SEALED     Perplexity confirms published state matches intent -> complete
# Ledger: data/a2a/tasks.json. Nothing advances without an explicit ping.
TASKS_KEY = "data/a2a/tasks.json"
BACKEND_AGENT = "justhodl-backend-agent"
STATES = ["FILED", "ACK", "DONE", "VERIFIED", "PUBLISHED", "SEALED"]


def _tasks():
    return _get(TASKS_KEY, {"tasks": {}, "sealed": []})


def _task_put(doc):
    _put(TASKS_KEY, doc)


def task_update(ev):
    """Advance a task's handshake state. Args: thread_id, state, by, note."""
    tid = ev.get("thread_id")
    state = (ev.get("state") or "").upper()
    by = (ev.get("from") or "unknown").lower()
    if not tid or state not in STATES:
        return {"ok": False, "error": f"thread_id + state in {STATES}"}
    doc = _tasks()
    task = doc["tasks"].get(tid) or {
        "thread_id": tid, "created_at": _now(), "state": "FILED",
        "history": [], "title": (ev.get("title") or "")[:200]}
    task["state"] = state
    task["updated_at"] = _now()
    task["history"].append({"state": state, "by": by, "ts": _now(),
                            "note": (ev.get("note") or "")[:400]})
    doc["tasks"][tid] = task
    if state == "SEALED":
        doc["sealed"] = (doc.get("sealed") or [])[-99:] + [
            {"thread_id": tid, "sealed_at": _now(), "by": by,
             "title": task.get("title")}]
    _task_put(doc)
    return {"ok": True, "task": task,
            "next_expected": _next_state(state, by)}


def _next_state(state, by):
    nxt = {"FILED": "ACK (claude confirms receipt)",
           "ACK": "DONE (claude finishes and pings back)",
           "DONE": "VERIFIED (perplexity checks the work)",
           "VERIFIED": "PUBLISHED (claude publishes engine+page)",
           "PUBLISHED": "SEALED (perplexity confirms published state)",
           "SEALED": "complete — move to next task"}
    return nxt.get(state)


def get_tasks(_ev=None):
    doc = _tasks()
    open_tasks = {k: v for k, v in doc.get("tasks", {}).items()
                  if v.get("state") != "SEALED"}
    return {"ok": True, "open": open_tasks,
            "n_open": len(open_tasks),
            "sealed_recent": (doc.get("sealed") or [])[-10:]}


def _wake_claude(thread_id, reason):
    """IMMEDIATE ping — invoke Claude's backend agent now instead of waiting
    for its 15-minute schedule (Khalid: 'no 15 minutes waiting')."""
    try:
        boto3.client("lambda", region_name="us-east-1").invoke(
            FunctionName=BACKEND_AGENT, InvocationType="Event",
            Payload=json.dumps({"wake": True, "thread_id": thread_id,
                                "reason": reason}).encode())
        return True
    except Exception as e:
        print("wake failed:", str(e)[:100])
        return False


ACTIONS = {"open_thread": open_thread, "post_turn": post_turn,
           "resolve": resolve, "deadman_sweep": deadman_sweep,
           "fanout_pending": fanout_pending,
           "propose_patch": propose_patch,
           "task_update": task_update,
           "get_tasks": get_tasks,
           "list_patches": lambda e: {"ok": True, **(_get("data/a2a/patches.json", {"patches": []}))},
           "get_thread": lambda e: {"ok": True, "thread": _get(
               THREADS + e["thread_id"] + ".json")}}


_token_cache = {}


def _agent_for_token(tok):
    """Map bearer token -> agent name via SSM /justhodl/a2a/token/<agent>."""
    if not tok:
        return None
    if tok in _token_cache:
        return _token_cache[tok]
    try:
        resp = ssm.get_parameters_by_path(Path="/justhodl/a2a/token/",
                                          Recursive=True,
                                          WithDecryption=True)
        for p in resp.get("Parameters", []):
            _token_cache[p["Value"]] = p["Name"].rsplit("/", 1)[-1]
    except Exception as e:
        print("token load err:", str(e)[:80])
    return _token_cache.get(tok)


@track_errors
def lambda_handler(event, context):
    event = event or {}
    http = (event.get("requestContext") or {}).get("http") or {}
    is_http = bool(http)
    qs = event.get("queryStringParameters") or {}
    raw_body = event.get("body")
    if isinstance(raw_body, str):
        try:
            body = json.loads(raw_body)
        except Exception:
            body = {}
        merged = {**qs, **body}
    else:
        merged = {**qs, **{k: v for k, v in event.items()
                           if k not in ("requestContext", "headers",
                                        "queryStringParameters")}}
    if is_http:
        hdrs = {k.lower(): v for k, v in
                (event.get("headers") or {}).items()}
        tok = (hdrs.get("authorization") or "").replace("Bearer ", "").strip()
        agent = _agent_for_token(tok)
        method = (http.get("method") or "GET").upper()
        act_req = merged.get("action")
        if method == "GET" and act_req in (None, "get_thread"):
            merged["action"] = "get_thread"
        elif not agent:
            return {"statusCode": 401,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"ok": False,
                                        "error": "invalid or missing "
                                                 "bearer token"})}
        else:
            # identity is the token, never the claim — spoof-proof
            merged["from"] = agent
            merged.setdefault("delivered_via", "http")
        event = merged
    else:
        event = merged if merged else event
    act = event.get("action")
    fn = ACTIONS.get(act)
    if not fn:
        return {"statusCode": 400,
                "body": json.dumps({"ok": False,
                                    "error": f"unknown action {act}",
                                    "actions": sorted(ACTIONS)})}
    res = fn(event)
    try:
        deadman_sweep()  # cheap piggyback on every invocation
    except Exception:
        pass
    return {"statusCode": 200 if res.get("ok") else 422,
            "headers": {"Content-Type": "application/json",
                        "Access-Control-Allow-Origin": "*"},
            "body": json.dumps(res, default=str)}
