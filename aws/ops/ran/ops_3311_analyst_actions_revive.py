"""ops 3311 — analyst-actions.html stuck on "loading" because its feed
data/analyst-actions.json is MISSING at S3 (bare GET -> 403 AccessDenied
= no object). Engine justhodl-analyst-actions is structurally sound
(always writes, no bail-on-empty) and was created (ops 1980) with a daily
cron(45 13 * * ? *) schedule + Benzinga-via-Massive entitlement. So the
file is absent because the invocation isn't happening: schedule likely
dropped in a fleet migration, or the function errors at init/runtime
before the put_object.

This op DIAGNOSES + FIXES + VERIFIES in one shot:
  1. Snapshot: does the object exist? function config sane?
  2. Force synchronous invoke; capture statusCode / FunctionError / logs.
  3. Confirm data/analyst-actions.json now exists, is fresh (<10 min),
     has expected schema keys, and report the harvest counts (ratings/
     guidance/insights) so we can tell a real Benzinga-empty from a
     write-failure.
  4. Re-assert the daily EventBridge schedule (idempotent) so it
     self-updates going forward.

Truth bands: PASS = object present AND generated_at within 10 min AND the
counts block exists. If counts are all zero we WARN (Massive/Benzinga
entitlement) but the page will still render (page tolerates empty lists).
"""
import base64
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-analyst-actions"
OUT_KEY = "data/analyst-actions.json"
RULE = "justhodl-analyst-actions-daily"
SCHED = "cron(45 13 * * ? *)"

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
EV = boto3.client("events", region_name=REGION)


def s3_head(key):
    try:
        h = S3.head_object(Bucket=BUCKET, Key=key)
        return {"exists": True, "size": h["ContentLength"],
                "modified": h["LastModified"].isoformat()}
    except Exception as e:
        return {"exists": False, "err": type(e).__name__}


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


with report("3311_analyst_actions_revive") as rep:
    fails, warns = [], []

    # --- 1. pre-snapshot -------------------------------------------------
    rep.section("PRE-STATE")
    before = s3_head(OUT_KEY)
    rep.kv(feed_before=before)

    try:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        rep.kv(fn_state=cfg.get("State"), last_update=cfg.get("LastUpdateStatus"),
               timeout=cfg.get("Timeout"), mem=cfg.get("MemorySize"),
               has_massive_env=("MASSIVE_API_KEY" in env))
    except Exception as e:
        fails.append(f"get_function_configuration failed: {e}")
        rep.fail(f"function missing? {e}")

    # --- 2. force invoke -------------------------------------------------
    rep.section("FORCE INVOKE")
    payload = None
    fn_err = None
    try:
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       LogType="Tail", Payload=b"{}")
        fn_err = r.get("FunctionError")
        raw = r["Payload"].read().decode("utf-8", "ignore")
        rep.kv(invoke_status=r.get("StatusCode"), function_error=fn_err)
        try:
            payload = json.loads(raw)
        except Exception:
            payload = raw
        rep.kv(response=payload)
        # last 4KB of logs — where any traceback lives
        if r.get("LogResult"):
            tail = base64.b64decode(r["LogResult"]).decode("utf-8", "ignore")
            interesting = [ln for ln in tail.splitlines()
                           if any(k in ln for k in
                                  ("[analyst]", "Error", "Traceback",
                                   "Exception", "REPORT"))]
            rep.kv(log_tail="\n".join(interesting[-12:]))
        if fn_err:
            fails.append(f"FunctionError={fn_err} (traceback in log_tail)")
    except Exception as e:
        fails.append(f"invoke raised: {e}")
        rep.fail(f"invoke raised: {e}")

    # S3 is eventually consistent on new keys; give it a beat
    time.sleep(3)

    # --- 3. verify feed --------------------------------------------------
    rep.section("VERIFY FEED")
    after = s3_head(OUT_KEY)
    rep.kv(feed_after=after)
    doc = s3_json(OUT_KEY)
    if not doc:
        fails.append("feed still absent/unreadable after invoke")
        rep.fail("data/analyst-actions.json not readable post-invoke")
    else:
        gen = doc.get("generated_at", "")
        counts = doc.get("counts", {})
        rep.kv(engine=doc.get("engine"), version=doc.get("version"),
               generated_at=gen, counts=counts,
               n_most_bullish=len(doc.get("most_bullish", [])),
               n_top_picks=len(doc.get("top_picks", [])))
        # freshness
        try:
            age = (datetime.now(timezone.utc)
                   - datetime.fromisoformat(gen.replace("Z", "+00:00")))
            fresh = age.total_seconds() < 600
            rep.kv(age_seconds=int(age.total_seconds()), fresh=fresh)
            if not fresh:
                fails.append(f"feed stale — generated_at age={age}")
        except Exception:
            warns.append("could not parse generated_at")
        # schema
        if "counts" not in doc:
            fails.append("feed missing 'counts' block (schema)")
        total_signals = sum(v for v in counts.values()) if counts else 0
        if total_signals == 0:
            warns.append("Benzinga harvest returned 0 across all feeds — "
                         "check Massive entitlement (page still renders empty)")
            rep.warn("harvest empty (0 ratings/guidance/insights)")
        else:
            rep.ok(f"harvest non-empty: {counts}")

    # --- 4. re-assert schedule (idempotent) ------------------------------
    rep.section("SCHEDULE")
    try:
        acct = boto3.client("sts", REGION).get_caller_identity()["Account"]
        arn = f"arn:aws:lambda:{REGION}:{acct}:function:{FN}"
        EV.put_rule(Name=RULE, ScheduleExpression=SCHED, State="ENABLED")
        try:
            LAM.add_permission(
                FunctionName=FN, StatementId=f"{RULE}-invoke",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{acct}:rule/{RULE}")
        except LAM.exceptions.ResourceConflictException:
            pass  # permission already present
        EV.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
        desc = EV.describe_rule(Name=RULE)
        rep.kv(rule=RULE, schedule=desc.get("ScheduleExpression"),
               state=desc.get("State"))
        rep.ok(f"daily schedule asserted: {SCHED}")
    except Exception as e:
        warns.append(f"schedule assert failed: {e}")
        rep.warn(f"schedule assert failed: {e}")

    # --- verdict ---------------------------------------------------------
    rep.section("VERDICT")
    if fails:
        for f in fails:
            rep.fail(f)
        rep.kv(RESULT="FAIL", fails=len(fails), warns=len(warns))
        sys.exit(1)
    for w in warns:
        rep.warn(w)
    rep.ok("analyst-actions feed live + fresh; page will render on next load")
    rep.kv(RESULT="PASS", warns=len(warns))
