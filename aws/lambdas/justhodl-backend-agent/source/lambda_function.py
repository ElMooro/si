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


@track_errors
def lambda_handler(event, context):
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
                bus({"action": "post_turn", "thread_id": tid,
                     "from": "claude-backend", "to": target.get("from"),
                     "kind": "question",
                     "content": "[backend-agent] This needs Claude's "
                                "judgment (code/novel/ambiguous) — queued "
                                "in data/backend-agent/escalations.json; "
                                "Claude handles it next session. Mechanical "
                                "requests (restart engine, rebind schedule, "
                                "probe feed) I execute live."})
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
          "capabilities": ["restart_engine", "rebind_schedule",
                           "probe_feed"],
          "note": "Backend heartbeat: drains Claude's bus inbox, "
                  "self-executes mechanical ops, escalates judgment to "
                  "Claude. The autonomous backend half."})
    bus({"action": "fanout_pending"})

    res = {"ok": True, "executed": len(executed),
           "escalated": len(escalated),
           "capabilities_run": [e["capability"] for e in executed],
           "escalation_queue_depth": len(esc["queue"])}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res, default=str)}
