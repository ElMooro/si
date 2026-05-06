#!/usr/bin/env python3
"""Step 219 — verify step 218's Lambda update worked despite invoke timeout.

Step 218 timed out at boto3.invoke() (default 60s timeout, Lambda
runs 4 min). The Lambda likely completed successfully but we
couldn't see the response. Check S3 directly to see if the
dual-write actually happened.

If S3 shows fresh ka-*.json files, then the new Lambda works
and we can re-cut EventBridge to it.

Also: increase boto3 client read timeout for the test invoke
so we don't hit this same trap.
"""
import json, time
from datetime import datetime, timezone
from ops_report import report
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
OLD = "justhodl-khalid-metrics"
NEW = "justhodl-ka-metrics"
RULE = "justhodl-khalid-metrics-refresh"

# Use longer timeout for Lambda invokes — old Lambda has 240s timeout
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, connect_timeout=10))
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("phase4_verify_after_timeout") as r:
    r.heading("Phase 4 verify — did step 218 actually work despite timeout?")

    # 1. Check ka-*.json keys in S3 — if they exist + are fresh,
    #    the dual-write worked and the Lambda is functional
    r.section("1. Check if data/ka-*.json keys were written by step 218's invoke")
    keys = [
        "data/ka-metrics.json", "data/ka-config.json", "data/ka-analysis.json",
        "data/khalid-metrics.json", "data/khalid-config.json", "data/khalid-analysis.json",
    ]
    now = datetime.now(timezone.utc)
    n_ka_present = 0
    n_ka_fresh = 0  # < 30 min, since step 218 finished ~13:42
    for k in keys:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            age_s = (now - obj["LastModified"]).total_seconds()
            if age_s < 1800:  # 30 min
                mark = "✅ FRESH"
            elif age_s < 86400:
                mark = "⏰ today"
            else:
                mark = "⏳ old"
            r.log(f"  {mark} {k:40s}  size={obj['ContentLength']:>10}B  age={age_s:.0f}s")
            if k.startswith("data/ka-"):
                n_ka_present += 1
                if age_s < 1800: n_ka_fresh += 1
        except ClientError as e:
            if "NoSuchKey" in str(e):
                r.warn(f"  ✗ MISSING {k}")
            else:
                r.warn(f"  ✗ {k}: {e}")

    r.log(f"\n  ka_*.json present: {n_ka_present}/3   fresh (<30min): {n_ka_fresh}/3")

    # 2. Run a fresh invoke with proper timeout
    r.section(f"2. Fresh test-invoke {NEW} with 300s read timeout")
    t0 = time.time()
    try:
        inv = lam.invoke(FunctionName=NEW, InvocationType="RequestResponse",
                         Payload=json.dumps({}))
        elapsed = time.time() - t0
        err = inv.get("FunctionError")
        payload = inv["Payload"].read().decode("utf-8", errors="replace")[:500]
        if err:
            r.warn(f"  ✗ err={err} ({elapsed:.1f}s)")
            r.warn(f"  payload: {payload}")
            invoke_ok = False
        else:
            r.log(f"  ✅ OK ({elapsed:.1f}s)")
            r.log(f"  payload: {payload}")
            invoke_ok = True
    except Exception as e:
        r.warn(f"  ✗ invoke fail: {e}")
        invoke_ok = False

    time.sleep(5)

    # 3. Re-check S3 freshness post-invoke
    r.section("3. Re-check S3 freshness post-invoke")
    now = datetime.now(timezone.utc)
    n_fresh = 0
    for k in keys:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            age_s = (now - obj["LastModified"]).total_seconds()
            mark = "✅ FRESH" if age_s < 60 else f"⏰ {int(age_s)}s"
            r.log(f"  {mark} {k:40s}  size={obj['ContentLength']:>10}B")
            if age_s < 120: n_fresh += 1
        except ClientError as e:
            if "NoSuchKey" in str(e):
                r.warn(f"  ✗ MISSING {k}")

    r.log(f"\n  {n_fresh}/{len(keys)} keys fresh (<2 min)")

    # 4. If invoke worked + 5+ keys fresh, cut EventBridge to NEW
    cutover_ok = invoke_ok and n_fresh >= 5
    r.section("4. EventBridge target decision")
    if cutover_ok:
        new_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NEW}"
        try:
            targets = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
            current = [t["Arn"].split(":")[-1] for t in targets]
            r.log(f"  current: {current}")

            if NEW in current:
                r.log(f"  ✅ already targets {NEW}")
            else:
                # ensure invoke perm exists
                try:
                    lam.add_permission(
                        FunctionName=NEW,
                        StatementId="EventBridgeInvoke",
                        Action="lambda:InvokeFunction",
                        Principal="events.amazonaws.com",
                        SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE}",
                    )
                    r.log(f"  ✅ EventBridge invoke perm granted")
                except ClientError as e:
                    if "ResourceConflictException" in str(e):
                        r.log(f"  ✅ EventBridge invoke perm already exists")

                new_targets = [{**t, "Arn": new_arn} for t in targets]
                resp = events.put_targets(Rule=RULE, Targets=new_targets)
                if resp.get("FailedEntryCount", 0) == 0:
                    r.log(f"  ✅ EventBridge → {NEW}")
                else:
                    r.warn(f"  ⚠ failed: {resp.get('FailedEntries')}")

                verify = [t["Arn"].split(":")[-1] for t in events.list_targets_by_rule(Rule=RULE).get("Targets", [])]
                r.log(f"  verified: {verify}")
        except Exception as e:
            r.warn(f"  ⚠ {e}")
    else:
        r.warn(f"  Skipping cutover — invoke_ok={invoke_ok} n_fresh={n_fresh}")
        r.warn(f"  EventBridge stays on {OLD}")

    r.section("FINAL")
    r.log(f"  invoke_ok: {invoke_ok}")
    r.log(f"  S3 fresh: {n_fresh}/{len(keys)}")
    r.log(f"  EventBridge: {'NEW' if cutover_ok else 'OLD (no cutover)'}")
    r.log("Done")
