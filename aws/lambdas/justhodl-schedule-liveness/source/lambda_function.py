"""justhodl-schedule-liveness — failsafe scheduler + silent-death watchdog.

Every scheduled engine is supposed to refresh its data/ feed on a cadence. When
EventBridge silently stops delivering to an ENABLED rule (as happened to 18
engines for up to 19 days), nothing noticed. This watchdog runs daily and:

  1. Enumerates every ENABLED rule with a schedule -> its target Lambda -> its
     output feed (data/<name>.json by default, with overrides).
  2. Flags any feed stale beyond tolerance (age > 2.5x cadence, floor 8h).
  3. SELF-HEALS: rebuilds the rule->target->permission binding (the fix that
     revived the 18) and async re-fires the engine to refresh the feed now.
  4. ESCALATES: an engine still stale after a prior revive = genuine failure
     (binding rebuild didn't fix it -> code/data error) -> Telegram alert.
  5. Writes data/schedule-liveness.json (state board) + alerts exceptions-only.

Needs (granted via inline policy justhodl-schedule-selfheal): events:Put*/Enable,
lambda:AddPermission/RemovePermission/InvokeFunction + read APIs.
"""
import os
import re
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
EVENTS = boto3.client("events", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ACCT = "857687956942"
REGION = "us-east-1"
STATE_KEY = "data/schedule-liveness.json"

STALE_MULT = 2.5
STALE_FLOOR_H = 8.0

TG_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TG_CHAT = "8678089260"

# engines whose output key is NOT data/<name>.json
FEED_OVERRIDE = {
    "justhodl-etf-fund-flows": "etf-flows/daily.json",
    "justhodl-stock-screener": "screener/data.json",
}
# rules to never touch (self + known non-feed infra)
SKIP_FN_SUBSTR = ("telegram", "-bot", "digest", "escalation", "harvester",
                  "outcome-checker", "calibrator", "signal-logger", "ops",
                  "schedule-liveness")


def feed_for(fn):
    if fn in FEED_OVERRIDE:
        return FEED_OVERRIDE[fn]
    return "data/" + fn.replace("justhodl-", "") + ".json"


def feed_age_h(key):
    try:
        lm = S3.head_object(Bucket=BUCKET, Key=key)["LastModified"]
        return (datetime.now(timezone.utc) - lm).total_seconds() / 3600.0
    except Exception:
        return None


def cadence_h(expr):
    if not expr:
        return None
    m = re.match(r"rate\((\d+)\s+(\w+)\)", expr)
    if m:
        n, u = int(m.group(1)), m.group(2)
        if "minute" in u:
            return n / 60.0
        if "hour" in u:
            return float(n)
        if "day" in u:
            return n * 24.0
    if expr.startswith("cron"):
        body = expr[5:-1].split()
        if len(body) >= 5:
            mins, hrs, dom, mon, dow = body[0], body[1], body[2], body[3], body[4]
            if dow not in ("?", "*", ""):
                cnt = len([x for x in re.split(r"[,\-]", dow) if x]) or 1
                return 168.0 / cnt
            if "/" in hrs:
                step = int(hrs.split("/")[1]) if hrs.split("/")[1].isdigit() else 6
                return float(step)
            if "," in hrs:
                return 24.0 / (hrs.count(",") + 1)
            if hrs == "*":
                return 1.0
            if "/" in mins or "," in mins:
                return 2.0
        return 24.0
    return None


def telegram(text):
    try:
        data = urllib.parse.urlencode({
            "chat_id": TG_CHAT, "text": text, "parse_mode": "Markdown",
            "disable_web_page_preview": "true"}).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data)
        urllib.request.urlopen(req, timeout=10).read()
    except Exception:
        pass


def rebuild_binding(fn, rule, cron):
    """Idempotent rebuild of rule->target->permission. Returns True on success."""
    try:
        EVENTS.put_rule(Name=rule, ScheduleExpression=cron, State="ENABLED")
        ex = EVENTS.list_targets_by_rule(Rule=rule).get("Targets", [])
        if len(ex) > 1:
            EVENTS.remove_targets(Rule=rule, Ids=[t["Id"] for t in ex])
        EVENTS.put_targets(Rule=rule, Targets=[
            {"Id": "1", "Arn": f"arn:aws:lambda:{REGION}:{ACCT}:function:{fn}"}])
        sid = f"{rule}-invoke"
        try:
            LAM.remove_permission(FunctionName=fn, StatementId=sid)
        except Exception:
            pass
        LAM.add_permission(FunctionName=fn, StatementId=sid, Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com",
                           SourceArn=f"arn:aws:events:{REGION}:{ACCT}:rule/{rule}")
        return True
    except Exception as e:
        print(f"  rebuild {fn}/{rule} ERR {type(e).__name__}: {e}")
        return False


def lambda_handler(event, context):
    t0 = time.time()
    # prior state for escalation tracking
    try:
        prior = json.loads(S3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
        hist = {h["engine"]: h for h in prior.get("revive_history", [])}
    except Exception:
        hist = {}

    # map rule -> target fn
    checks = []
    for pg in EVENTS.get_paginator("list_rules").paginate():
        for r in pg["Rules"]:
            if not r.get("ScheduleExpression") or r.get("State") != "ENABLED":
                continue
            tgts = EVENTS.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
            fns = [t["Arn"].split(":function:")[-1] for t in tgts if ":function:" in t.get("Arn", "")]
            if fns:
                checks.append((r["Name"], r["ScheduleExpression"], fns[0]))

    healthy, revived, genuine, no_feed = [], [], [], []
    new_hist = []
    for rule, cron, fn in checks:
        if any(s in fn for s in SKIP_FN_SUBSTR):
            continue
        feed = feed_for(fn)
        age = feed_age_h(feed)
        cad = cadence_h(cron)
        if age is None or cad is None:
            no_feed.append({"fn": fn, "feed": feed, "rule": rule})
            continue
        tol = max(STALE_FLOOR_H, STALE_MULT * cad)
        rec = {"fn": fn, "feed": feed, "rule": rule, "age_h": round(age, 1),
               "cadence_h": round(cad, 1), "tol_h": round(tol, 1)}
        if age <= tol:
            healthy.append(rec)
            continue
        # STALE -> self-heal
        prev = hist.get(fn, {})
        prev_consec = prev.get("consecutive_revives", 0)
        ok = rebuild_binding(fn, rule, cron)
        try:
            LAM.invoke(FunctionName=fn, InvocationType="Event")
        except Exception as e:
            rec["invoke_err"] = f"{type(e).__name__}"
        rec["binding_rebuilt"] = ok
        rec["consecutive_revives"] = prev_consec + 1
        new_hist.append({"engine": fn, "last_revived": datetime.now(timezone.utc).isoformat(),
                         "consecutive_revives": prev_consec + 1})
        if prev_consec >= 1:
            # revived before and STILL stale -> binding fix isn't the problem
            genuine.append(rec)
        else:
            revived.append(rec)

    # carry forward history for engines still healthy (reset their counter)
    revived_fns = {h["engine"] for h in new_hist}
    out = {
        "engine": "justhodl-schedule-liveness",
        "version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_rules_checked": len(checks),
        "n_assessable": len(healthy) + len(revived) + len(genuine),
        "n_healthy": len(healthy),
        "n_revived": len(revived),
        "n_genuine_failures": len(genuine),
        "n_no_feed": len(no_feed),
        "revived": sorted(revived, key=lambda x: -x["age_h"]),
        "genuine_failures": sorted(genuine, key=lambda x: -x["age_h"]),
        "stale_healthy_sample": sorted(healthy, key=lambda x: -x["age_h"])[:8],
        "no_feed": no_feed,
        "revive_history": new_hist,
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=300")

    # exceptions-only Telegram
    if revived or genuine:
        lines = ["*Schedule Liveness Watchdog*"]
        if revived:
            lines.append(f"\n_Revived {len(revived)} silently-dead engine(s):_")
            for r in revived[:12]:
                lines.append(f"• `{r['fn']}` — {r['age_h']}h stale (cadence {r['cadence_h']}h) → rebuilt+refired")
        if genuine:
            lines.append(f"\n⚠️ *{len(genuine)} GENUINE FAILURE(S)* (revive didn't fix — needs you):")
            for r in genuine[:12]:
                lines.append(f"• `{r['fn']}` — still {r['age_h']}h stale after {r['consecutive_revives']}x revive")
        telegram("\n".join(lines))

    return {"statusCode": 200, "body": json.dumps({
        "checked": len(checks), "healthy": len(healthy), "revived": len(revived),
        "genuine_failures": len(genuine), "no_feed": len(no_feed)})}
