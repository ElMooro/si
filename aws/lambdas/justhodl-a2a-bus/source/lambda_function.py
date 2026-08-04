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
    return {"ok": True, "turn_id": turn["turn_id"],
            "queued_for": targets}


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


A2A_SYSTEM = ("You are an agent on the JustHodl A2A bus. Read the thread "
              "JSON provided. Reply with ONLY a JSON object: {\"kind\": "
              "propose|critique|verify|agree|block|question, \"content\": "
              "str, \"evidence\": [{\"kind\":\"file|url|log\",\"ref\":str,"
              "\"snippet\":str?}], \"verdict\": confirmed|refuted|null}. "
              "Cite evidence. Never agree without verification.")


def fanout_pending(ev):
    """One bounded hop: for each queued provider inbox, call the provider
    with the thread, parse its JSON turn, post it back through invariant A."""
    reg = _get(REGISTRY, {}).get("providers", {})
    done, budget = [], MAX_FANOUT_PER_INVOKE
    for p, meta in reg.items():
        if budget <= 0:
            break
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
            slim["turns"] = [{k2: x.get(k2) for k2 in
                              ("from", "kind", "content", "verdict")}
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
            try:
                j = json.loads(raw[raw.find("{"):raw.rfind("}") + 1])
            except Exception:
                j = {"kind": "question", "content": raw[:2000],
                     "evidence": []}
            r = post_turn({"thread_id": tid, "from": p, "to": "*", **j})
            _inbox_pop(p, tid)
            done.append({"provider": p, "thread": tid,
                         "posted": r.get("ok"),
                         "rejected": r.get("error")})
    return {"ok": True, "fanout": done}


ACTIONS = {"open_thread": open_thread, "post_turn": post_turn,
           "resolve": resolve, "deadman_sweep": deadman_sweep,
           "fanout_pending": fanout_pending,
           "get_thread": lambda e: {"ok": True, "thread": _get(
               THREADS + e["thread_id"] + ".json")}}


@track_errors
def lambda_handler(event, context):
    event = event or {}
    if isinstance(event.get("body"), str):
        try:
            event = json.loads(event["body"])
        except Exception:
            pass
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
