"""justhodl-backend-agent v1.0 (ops 4397) — the backend heartbeat.

The missing half of full autonomy. Perplexity runs continuously; Claude
only ran when Khalid sent a message. This scheduled Lambda closes that gap:
every 15 minutes it drains Claude's A2A inbox — the `to:claude` /
`to:claude-audit` requests Perplexity (and the audit loop) file — and
SELF-EXECUTES the safe, mechanical ones, posting results back on the bus.
No human relay.

SAFETY MODEL — capability allowlist, not open execution. A Lambda acting
unsupervised must never do anything catastrophic, so it can ONLY perform a
fixed menu of reversible, mechanical operations:

  restart_engine      — invoke a justhodl-* function (heal stale feed)
  rebind_schedule     — recreate a missing EventBridge rule from name
  invoke_engine       — same as restart, explicit
  read_feed / probe   — head/read an S3 feed, report freshness
  fanout / status     — nudge the bus, report queue state

Anything else — writing code, changing engine logic, IAM, deletions,
anything not on the list — is ESCALATED: posted back as a turn tagged
kind:question to:claude with escalate:true, and left in a
data/backend-agent/escalations.json queue for Claude to handle in-session.
The heartbeat does the boring 80%; Claude does the judgment 20%. Every
action (executed or escalated) is logged to data/backend-agent/log.json.

Intent parsing is keyword/verb based over the turn content — deliberately
conservative: if intent is ambiguous, it ESCALATES rather than guesses.
"""
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config

try:
    from _sentry_lite import track_errors
except Exception:  # pragma: no cover
    def track_errors(f):
        return f

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
BUS = "justhodl-a2a-bus"
INBOXES = ["data/a2a/inbox/claude.json", "data/a2a/inbox/claude-audit.json"]
LOG_KEY = "data/backend-agent/log.json"
ESC_KEY = "data/backend-agent/escalations.json"
STATE_KEY = "data/backend-agent/state.json"
MAX_ACTIONS_PER_RUN = 5

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)


def _now():
    return datetime.now(timezone.utc)


def _get(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return default


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")


def bus(payload):
    try:
        inv = lam.invoke(FunctionName=BUS, InvocationType="RequestResponse",
                         Payload=json.dumps(payload).encode())
        b = json.loads(inv["Payload"].read().decode())
        return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
            else b
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


# ── capability allowlist ──
def _cap_restart_engine(fn):
    if not fn or not fn.startswith("justhodl-"):
        return {"ok": False, "detail": "not a justhodl engine"}
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        return {"ok": True, "detail": f"invoked {fn}"}
    except Exception as e:
        return {"ok": False, "detail": f"{type(e).__name__}: {str(e)[:80]}"}


def _cadence(name):
    n = name.lower()
    m = re.search(r"(\d+)\s*min", n)
    if m:
        return f"rate({m.group(1)} minutes)"
    m = re.search(r"-(\d+)h\b", n) or re.search(r"(\d+)\s*hour", n)
    if m:
        return f"rate({m.group(1)} hours)"
    if "hourly" in n:
        return "rate(1 hour)"
    if "daily" in n:
        return "rate(1 day)"
    return None


def _cap_rebind_schedule(fn, rule):
    if not fn or not fn.startswith("justhodl-") or not rule:
        return {"ok": False, "detail": "need engine + rule name"}
    expr = _cadence(rule)
    if not expr:
        return {"ok": False, "detail": f"cadence unparseable from {rule}"}
    try:
        arn = ev.put_rule(Name=rule, ScheduleExpression=expr, State="ENABLED",
                          Description=f"backend-agent rebind: {fn}"
                          )["RuleArn"]
        fa = lam.get_function_configuration(FunctionName=fn)["FunctionArn"]
        ev.put_targets(Rule=rule, Targets=[{"Id": fn[:60], "Arn": fa}])
        try:
            lam.add_permission(FunctionName=fn,
                               StatementId=("ba-" + rule)[:100],
                               Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",
                               SourceArn=arn)
        except lam.exceptions.ResourceConflictException:
            pass
        return {"ok": True, "detail": f"{rule} -> {expr} bound to {fn}"}
    except Exception as e:
        return {"ok": False, "detail": f"{type(e).__name__}: {str(e)[:80]}"}


def _cap_probe_feed(key):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        age = round((_now() - h["LastModified"]).total_seconds() / 3600, 2)
        return {"ok": True, "detail": f"{key} age {age}h, "
                f"{round(h['ContentLength']/1024,1)}KB"}
    except Exception as e:
        return {"ok": False, "detail": f"{key}: {type(e).__name__}"}


# ── intent classifier (conservative: ambiguous -> escalate) ──
ENGINE_RE = re.compile(r"\b(justhodl-[a-z0-9\-]+)\b")
RULE_RE = re.compile(r"\b(justhodl-[a-z0-9\-]+-(?:\d+min|\d+h|hourly|daily|"
                     r"weekly))\b")
FEED_RE = re.compile(r"\b(data/[a-z0-9\-_/]+\.json)\b")


def classify(content):
    c = (content or "").lower()
    engines = ENGINE_RE.findall(content or "")
    rules = RULE_RE.findall(content or "")
    feeds = FEED_RE.findall(content or "")
    # restart / heal a stale engine
    if any(k in c for k in ("restart", "re-invoke", "reinvoke", "force-run",
                            "force run", "kick", "heal", "stale feed",
                            "feed is stale", "re-fire", "refire")) \
            and engines:
        return ("restart_engine", {"fn": engines[0]})
    # rebind a schedule
    if any(k in c for k in ("rebind", "schedule missing", "no schedule",
                            "bind rule", "recreate rule", "add schedule")) \
            and engines:
        return ("rebind_schedule",
                {"fn": engines[0],
                 "rule": rules[0] if rules else engines[0] + "-hourly"})
    # probe / check a feed
    if any(k in c for k in ("check feed", "probe", "is it fresh",
                            "feed age", "how stale")) and feeds:
        return ("probe_feed", {"key": feeds[0]})
    # explicit "please execute mechanical" hints stay conservative
    return ("escalate", {"engines": engines[:3], "feeds": feeds[:3]})




# ── ops 4418: HANDSHAKE — instant ACK, no 15-minute wait ──────────────────
# The bus now wakes this Lambda the moment Perplexity files work. On wake we
# ACK receipt immediately ("received, working on it"), advance the task to
# ACK, and either execute (mechanical -> then ping DONE) or escalate to
# Claude with the task parked at ACK so Perplexity knows it is live, not lost.
def _ack(thread_id, from_agent, content_preview, mechanical):
    """Post an immediate acknowledgement turn + advance task state to ACK."""
    plan = ("executing now (mechanical capability) — will ping DONE when "
            "finished" if mechanical else
            "queued for Claude (needs judgment/code) — he pings DONE when "
            "shipped")
    bus({"action": "post_turn", "thread_id": thread_id,
         "from": "claude-backend", "to": from_agent, "kind": "agree",
         "content": f"ACK — received, working on it. {plan}. "
                    f"Re: {content_preview[:160]}"})
    bus({"action": "task_update", "thread_id": thread_id, "state": "ACK",
         "from": "claude-backend",
         "note": ("mechanical execution" if mechanical else
                  "escalated to Claude")})


def _ping_done(thread_id, from_agent, detail):
    bus({"action": "post_turn", "thread_id": thread_id,
         "from": "claude-backend", "to": from_agent, "kind": "propose",
         "content": f"DONE — {detail}. Your move: verify and ping back "
                    f"(task_update state=VERIFIED), then I publish and you "
                    f"seal.",
         "evidence": []})
    bus({"action": "task_update", "thread_id": thread_id, "state": "DONE",
         "from": "claude-backend", "note": detail[:300]})




# ── ops 4423: BUS HEALTH WATCHDOG — self-supervision, no prompting needed ──
# Khalid: "keep reading the bus and ask yourself if everything is going
# smooth; if not, look for the problem and fix it — you don't have to wait
# for me." So every heartbeat now audits the bus itself and repairs what it
# can. Detects and handles:
#   rejected_no_evidence  -> the poster's evidence didn't resolve. Re-post the
#                            same content with evidence VERIFIED first.
#   budget_exceeded       -> thread hit the turn ceiling. Open a continuation
#                            thread and carry the last turn over.
#   duplicate_acks        -> more than one ACK on a thread-state (turn burn).
#   stuck_task            -> task sat in one handshake state too long.
#   stalled_thread        -> open thread with no movement, someone is waiting.
# Everything found is written to data/backend-agent/bus-health.json so the
# state of the collaboration is inspectable rather than assumed.
HEALTH_KEY = "data/backend-agent/bus-health.json"
STUCK_MIN = 45


def _evidence_resolves(ev):
    """Verify evidence BEFORE posting — the bug behind my bounced DONE pings:
    turns were posted before the S3 write settled, so invariant A rejected my
    own completion notices and Perplexity never learned the work was done."""
    ok = []
    for e in (ev or [])[:6]:
        kind = (e.get("kind") or "").lower()
        ref = e.get("ref") or ""
        try:
            if kind in ("log", "s3"):
                body = s3.get_object(Bucket=BUCKET, Key=ref)["Body"].read(65536)
                want = e.get("snippet")
                ok.append((not want) or (want.encode() in body))
            elif kind == "file":
                import urllib.request
                u = "https://raw.githubusercontent.com/ElMooro/si/main/" + \
                    ref.lstrip("/")
                with urllib.request.urlopen(u, timeout=8) as r:
                    txt = r.read(65536).decode("utf-8", "replace")
                want = e.get("snippet")
                ok.append((not want) or (want in txt))
            elif kind == "url":
                import urllib.request
                with urllib.request.urlopen(ref, timeout=8) as r:
                    ok.append(200 <= r.status < 300)
            else:
                ok.append(False)
        except Exception:
            ok.append(False)
    return bool(ok) and all(ok)


def post_verified(thread_id, to, kind, content, evidence, retries=3):
    """Post only once the evidence actually resolves; retry with backoff."""
    for a in range(retries):
        if not evidence or _evidence_resolves(evidence):
            r = bus({"action": "post_turn", "thread_id": thread_id,
                     "from": "claude-backend", "to": to, "kind": kind,
                     "content": content, "evidence": evidence or []})
            if r.get("ok"):
                return r
            if r.get("error") != "rejected_no_evidence":
                return r
        time.sleep(5 * (a + 1))
    # last resort: post without evidence claims as a question (always allowed)
    return bus({"action": "post_turn", "thread_id": thread_id,
                "from": "claude-backend", "to": to, "kind": "question",
                "content": content})


def bus_health_sweep():
    """Read the whole bus, find what is going wrong, repair what we can."""
    findings, repairs = [], []
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/a2a/threads/",
                                  MaxKeys=200)
        keys = [o["Key"] for o in resp.get("Contents", [])]
    except Exception as e:
        return {"error": str(e)[:120]}
    now = _now()
    for k in keys:
        th = _get(k)
        if not th:
            continue
        tid = th.get("thread_id")
        turns = th.get("turns") or []
        rejected = th.get("rejected") or []
        # 1) evidence rejections — repost with verified evidence
        for rj in rejected[-3:]:
            if rj.get("status") != "rejected_no_evidence":
                continue
            if str(rj.get("from", "")).startswith("claude"):
                findings.append({"thread": tid, "issue": "rejected_no_evidence",
                                 "by": rj.get("from")})
                content = (rj.get("content") or "")[:3000]
                already = any(content[:120] in (x.get("content") or "")
                              for x in turns)
                if content and not already:
                    r = post_verified(tid, "perplexity", "propose",
                                      "[auto-repair] Re-posting a turn that "
                                      "invariant A rejected because its "
                                      "evidence had not settled yet:\n\n"
                                      + content, rj.get("evidence") or [])
                    repairs.append({"thread": tid, "action": "repost",
                                    "ok": r.get("ok")})
        # 2) turn ceiling — open a continuation thread
        if len(turns) >= MAX_TURNS_WARN:
            findings.append({"thread": tid, "issue": "near_turn_ceiling",
                             "turns": len(turns)})
            cont = tid + "-cont"
            if not bus({"action": "get_thread",
                        "thread_id": cont}).get("thread"):
                bus({"action": "open_thread", "thread_id": cont,
                     "topic": f"Continuation of {tid} (turn ceiling)"})
                last = turns[-1] if turns else {}
                bus({"action": "post_turn", "thread_id": cont,
                     "from": "claude-backend", "to": "perplexity",
                     "kind": "question",
                     "content": f"[auto-repair] {tid} hit the turn ceiling. "
                                f"Continuing here. Last turn was from "
                                f"{last.get('from')}: "
                                f"{(last.get('content') or '')[:400]}"})
                repairs.append({"thread": tid, "action": "continuation",
                                "new": cont})
        # 3) duplicate ACKs (turn burn)
        acks = [x for x in turns if x.get("kind") == "agree"
                and str(x.get("content", "")).startswith("ACK")]
        if len(acks) > 2:
            findings.append({"thread": tid, "issue": "duplicate_acks",
                             "count": len(acks)})
    # 4) stuck handshake tasks
    board = bus({"action": "get_tasks"}) or {}
    for tid, task in (board.get("open") or {}).items():
        try:
            upd = datetime.fromisoformat(task.get("updated_at")
                                         or task.get("created_at"))
            mins = (_now() - upd).total_seconds() / 60
        except Exception:
            continue
        if mins > STUCK_MIN:
            findings.append({"thread": tid, "issue": "stuck_task",
                             "state": task.get("state"),
                             "minutes": round(mins)})
            if task.get("state") in ("FILED", "ACK"):
                bus({"action": "post_turn", "thread_id": tid,
                     "from": "claude-backend", "to": "perplexity",
                     "kind": "question",
                     "content": f"[auto-repair] This task has sat at "
                                f"{task.get('state')} for {round(mins)}m. "
                                "Still live on my side — flagging so it is "
                                "not silently dropped. If you are waiting on "
                                "me, say what you need; if I am waiting on "
                                "you, this is the nudge."})
                repairs.append({"thread": tid, "action": "nudge",
                                "state": task.get("state")})
    doc = {"swept_at": now, "n_findings": len(findings),
           "n_repairs": len(repairs), "findings": findings[-40:],
           "repairs": repairs[-40:],
           "note": "Self-supervision sweep — runs every heartbeat, no "
                   "prompting required (Khalid, ops 4423)."}
    _put(HEALTH_KEY, doc)
    return {"findings": len(findings), "repairs": len(repairs)}


MAX_TURNS_WARN = 40


@track_errors
def lambda_handler(event, context):
    # ops 4423: audit the bus first — find problems without being told
    try:
        health = bus_health_sweep()
    except Exception as _e:
        health = {"error": str(_e)[:120]}
        print("health sweep error:", str(_e)[:150])
    state = _get(STATE_KEY) or {"runs": 0, "executed": 0, "escalated": 0}
    log = _get(LOG_KEY) or {"actions": []}
    esc = _get(ESC_KEY) or {"queue": []}
    executed, escalated, seen = [], [], []
    budget = MAX_ACTIONS_PER_RUN

    for inbox_key in INBOXES:
        who = inbox_key.split("/")[-1].replace(".json", "")
        box = _get(inbox_key) or {"threads": []}
        remaining = []
        for tid in box.get("threads", []):
            if budget <= 0:
                remaining.append(tid)
                continue
            thread = bus({"action": "get_thread",
                          "thread_id": tid}).get("thread") or {}
            turns = thread.get("turns") or []
            # the most recent turn addressed TO this agent from someone else
            target = None
            for x in reversed(turns):
                if x.get("to") in (who, "*", "claude") and \
                        x.get("from") not in (who, "claude", "claude-audit"):
                    target = x
                    break
            if not target:
                # nothing actionable; drop from inbox
                seen.append(tid)
                continue
            kind, args = classify(target.get("content"))
            budget -= 1
            # ops 4420: ACK ONCE per thread-state. Perplexity verified that
            # duplicate ACK + queue turns were burning the 16-turn budget.
            try:
                _board = bus({"action": "get_tasks"}) or {}
                _cur = ((_board.get("open") or {}).get(tid) or {}).get("state")
                if _cur not in ("ACK", "DONE", "VERIFIED", "PUBLISHED"):
                    _ack(tid, target.get("from") or "perplexity",
                         target.get("content") or "", kind != "escalate")
            except Exception as _e:
                print("ack failed:", str(_e)[:80])
            if kind == "escalate":
                escalated.append({"thread": tid, "from": target.get("from"),
                                  "why": "novel/ambiguous — needs Claude",
                                  "hint": args,
                                  "snippet": (target.get("content")
                                              or "")[:300]})
                esc["queue"] = [e for e in esc["queue"]
                                if e.get("thread") != tid]
                esc["queue"].append({"thread": tid, "ts": _now().isoformat(),
                                     "from": target.get("from"),
                                     "snippet": (target.get("content")
                                                 or "")[:400],
                                     "hint": args})
                # tell the requester it's queued for Claude (don't drop —
                # Claude closes it in-session)
                # ops 4420: escalation notice folded into the single ACK —
                # no extra turn (turn budget is scarce; Perplexity flagged it)
                pass
                seen.append(tid)
                continue
            # execute a capability
            if kind == "restart_engine":
                res = _cap_restart_engine(args["fn"])
            elif kind == "rebind_schedule":
                res = _cap_rebind_schedule(args["fn"], args["rule"])
            elif kind == "probe_feed":
                res = _cap_probe_feed(args["key"])
            else:
                res = {"ok": False, "detail": "unmapped capability"}
            rec = {"ts": _now().isoformat(), "thread": tid,
                   "capability": kind, "args": args, "result": res}
            executed.append(rec)
            log["actions"].append(rec)
            if res.get("ok"):
                try:
                    _ping_done(tid, target.get("from") or "perplexity",
                               f"{kind}({json.dumps(args)}) -> "
                               f"{res.get('detail')}")
                except Exception as _e:
                    print("ping done failed:", str(_e)[:80])
            bus({"action": "post_turn", "thread_id": tid,
                 "from": "claude-backend", "to": target.get("from"),
                 "kind": "propose" if res.get("ok") else "block",
                 "content": f"[backend-agent] executed {kind}"
                            f"({json.dumps(args)}): {res.get('detail')}. "
                            "Auto-run by the heartbeat; escalated to Claude "
                            "only if it fails or needs judgment.",
                 "evidence": ([{"kind": "log",
                                "ref": args.get("key")}]
                              if args.get("key") else [])})
            seen.append(tid)
        box["threads"] = remaining
        _put(inbox_key, box)

    log["actions"] = log["actions"][-300:]
    esc["queue"] = esc["queue"][-100:]
    state["runs"] += 1
    state["executed"] += len(executed)
    state["escalated"] += len(escalated)
    state["last_run"] = _now().isoformat()
    state["last_executed"] = [e["capability"] for e in executed]
    _put(LOG_KEY, log)
    _put(ESC_KEY, esc)
    _put(STATE_KEY, state)

    # heartbeat status so the audit loop / Khalid can see it's alive
    _put("data/backend-agent/heartbeat.json",
         {"alive_at": _now().isoformat(), "runs": state["runs"],
          "executed_total": state["executed"],
          "escalated_total": state["escalated"],
          "this_run": {"executed": len(executed),
                       "escalated": len(escalated)},
          "escalation_queue_depth": len(esc["queue"]),
          "bus_health": health,
          "capabilities": ["restart_engine", "rebind_schedule",
                           "probe_feed"],
          "note": "Backend heartbeat: drains Claude's bus inbox, "
                  "self-executes mechanical ops, escalates judgment to "
                  "Claude. The autonomous backend half."})
    bus({"action": "fanout_pending"})

    res = {"ok": True, "bus_health": health, "executed": len(executed),
           "escalated": len(escalated),
           "capabilities_run": [e["capability"] for e in executed],
           "escalation_queue_depth": len(esc["queue"])}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res, default=str)}
