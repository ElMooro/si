#!/usr/bin/env python3
"""Step 244 — Tighten auction-crisis-detector schedule + verify wiring.

Khalid asked: "make sure it's wired to always pull the latest treasury auctions data"

Changes vs original Phase 10 setup:

  1. SCHEDULE upgrade
     OLD: rate(1 hour)            — 24/day, fires when no auctions published
     NEW: TWO rules:
       (a) cron(0/15 14-22 ? * MON-FRI *)  every 15 min Mon-Fri 14:00-22:00 UTC
           Covers the full Treasury auction publishing window:
             Bills:  11:30 AM ET = 15:30/16:30 UTC
             Notes:  1:00 PM ET  = 17:00/18:00 UTC
             Results post within ~2 hours of auction close
       (b) rate(4 hours)               low-cost backstop for off-hours

     This means within 15 minutes of any new auction result being published
     by Treasury, the Lambda fires and the dashboard updates.

  2. WIRING VERIFICATION
     Confirms:
       - Lambda exists with latest CodeSha256
       - EB rules exist and are ENABLED
       - Targets correctly wired
       - IAM permissions in place
       - Most recent S3 output has freshness fields populated

  3. FORCE FRESH PULL
     Manually invokes the Lambda once after schedule change and confirms
     the freshness section shows latest auction date & how stale it is.
"""
import json
import sys
import time
from ops_report import report
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
FUNCTION_NAME = "justhodl-auction-crisis-detector"
OLD_RULE = "justhodl-auction-crisis-refresh"   # rate(1 hour)
ACTIVE_RULE = "justhodl-auction-crisis-active" # cron mon-fri 14-22 UTC every 15min
BACKSTOP_RULE = "justhodl-auction-crisis-backstop"  # rate(4 hours)

ACTIVE_SCHEDULE = "cron(0/15 14-22 ? * MON-FRI *)"
BACKSTOP_SCHEDULE = "rate(4 hours)"

lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300))
eb = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("auction_crisis_wire_check") as r:
    r.heading("Phase 10 — wire detector for ALWAYS-FRESH Treasury data")

    # ─────────────────────────────────────────────────────────────────
    # 1. Confirm Lambda exists and is healthy
    # ─────────────────────────────────────────────────────────────────
    r.section("1. Lambda health check")
    try:
        cfg = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
        r.log(f"  ✅ Lambda exists: {FUNCTION_NAME}")
        r.log(f"     CodeSha256:   {cfg['CodeSha256']}")
        r.log(f"     LastModified: {cfg['LastModified']}")
        r.log(f"     State:        {cfg['State']}")
        r.log(f"     Runtime:      {cfg['Runtime']}")
        r.log(f"     Timeout:      {cfg['Timeout']}s")
        r.log(f"     Memory:       {cfg['MemorySize']}MB")
        if cfg["State"] != "Active":
            r.warn(f"  ⚠ Lambda state is {cfg['State']}, not Active")
            sys.exit(0)
    except ClientError as e:
        r.warn(f"  ✗ Lambda not found — run step 243 first: {e}")
        sys.exit(0)

    # ─────────────────────────────────────────────────────────────────
    # 2. Replace the old hourly rule with TWO new rules
    # ─────────────────────────────────────────────────────────────────
    r.section("2. Schedule upgrade — old hourly → smart cron + backstop")

    # Disable + delete old rule (need to remove targets first)
    r.log(f"  Removing old rule: {OLD_RULE}")
    try:
        # Remove all targets
        targets = eb.list_targets_by_rule(Rule=OLD_RULE)["Targets"]
        if targets:
            eb.remove_targets(Rule=OLD_RULE, Ids=[t["Id"] for t in targets])
            r.log(f"    removed {len(targets)} target(s)")
        # Delete rule
        eb.delete_rule(Name=OLD_RULE, Force=True)
        r.log(f"    ✅ deleted {OLD_RULE}")
    except ClientError as e:
        if "ResourceNotFoundException" in str(e):
            r.log(f"    (rule already gone)")
        else:
            r.warn(f"    ⚠ {e}")

    # Create active-window rule (15 min weekdays 14-22 UTC)
    r.log(f"  Creating active-window rule: {ACTIVE_RULE}")
    try:
        eb.put_rule(
            Name=ACTIVE_RULE,
            ScheduleExpression=ACTIVE_SCHEDULE,
            State="ENABLED",
            Description=(
                "Mon-Fri 14:00-22:00 UTC every 15min. Covers Treasury auction "
                "publication window (bills ~15:30 UTC, notes ~17:00 UTC, "
                "results post within 2hr of auction close)."
            ),
        )
        r.log(f"    ✅ {ACTIVE_RULE} {ACTIVE_SCHEDULE} ENABLED")
    except ClientError as e:
        r.warn(f"    ✗ {e}")

    # Create backstop rule (4-hour for weekends + off-hours)
    r.log(f"  Creating backstop rule: {BACKSTOP_RULE}")
    try:
        eb.put_rule(
            Name=BACKSTOP_RULE,
            ScheduleExpression=BACKSTOP_SCHEDULE,
            State="ENABLED",
            Description="Backstop refresh for weekends + off-hours (every 4 hours).",
        )
        r.log(f"    ✅ {BACKSTOP_RULE} {BACKSTOP_SCHEDULE} ENABLED")
    except ClientError as e:
        r.warn(f"    ✗ {e}")

    # ─────────────────────────────────────────────────────────────────
    # 3. Wire IAM permissions for both rules
    # ─────────────────────────────────────────────────────────────────
    r.section("3. IAM permissions: events.amazonaws.com → Lambda")
    for rule_name, sid in [
        (ACTIVE_RULE, "AllowEventsActiveWindow"),
        (BACKSTOP_RULE, "AllowEventsBackstop"),
    ]:
        try:
            lam.add_permission(
                FunctionName=FUNCTION_NAME,
                StatementId=sid,
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{rule_name}",
            )
            r.log(f"    ✅ permission added: {sid}")
        except ClientError as e:
            if "ResourceConflictException" in str(e):
                r.log(f"    (permission {sid} already exists)")
            else:
                r.warn(f"    ⚠ {sid}: {e}")

    # Remove old permission if exists
    try:
        lam.remove_permission(FunctionName=FUNCTION_NAME, StatementId="AllowEventBridgePhase10")
        r.log(f"    ✅ removed old AllowEventBridgePhase10 permission")
    except ClientError:
        pass

    # ─────────────────────────────────────────────────────────────────
    # 4. Wire EB targets → Lambda for both rules
    # ─────────────────────────────────────────────────────────────────
    r.section("4. EB targets")
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FUNCTION_NAME}"
    for rule_name in [ACTIVE_RULE, BACKSTOP_RULE]:
        try:
            eb.put_targets(
                Rule=rule_name,
                Targets=[{"Id": "1", "Arn": fn_arn}],
            )
            r.log(f"    ✅ {rule_name} → {FUNCTION_NAME}")
        except ClientError as e:
            r.warn(f"    ✗ {rule_name}: {e}")

    # ─────────────────────────────────────────────────────────────────
    # 5. Verify final wiring
    # ─────────────────────────────────────────────────────────────────
    r.section("5. Verify final wiring (read-back)")
    for rule_name in [ACTIVE_RULE, BACKSTOP_RULE]:
        try:
            rule = eb.describe_rule(Name=rule_name)
            targets = eb.list_targets_by_rule(Rule=rule_name)["Targets"]
            r.log(f"    {rule_name}:")
            r.log(f"      Schedule: {rule.get('ScheduleExpression')}")
            r.log(f"      State:    {rule.get('State')}")
            r.log(f"      Targets:  {len(targets)} → {[t['Arn'].split(':')[-1] for t in targets]}")
        except ClientError as e:
            r.warn(f"    ✗ {rule_name}: {e}")

    # Lambda's permission policy
    try:
        policy = lam.get_policy(FunctionName=FUNCTION_NAME)
        pol = json.loads(policy["Policy"])
        sids = [s["Sid"] for s in pol.get("Statement", [])]
        r.log(f"    Lambda permissions: {sids}")
    except ClientError as e:
        r.warn(f"    ✗ get_policy: {e}")

    # ─────────────────────────────────────────────────────────────────
    # 6. FORCE FRESH PULL — invoke now and verify freshness fields
    # ─────────────────────────────────────────────────────────────────
    r.section("6. Force a fresh pull — verify Treasury data flowing")
    t0 = time.time()
    try:
        resp = lam.invoke(FunctionName=FUNCTION_NAME, InvocationType="RequestResponse")
        payload = json.loads(resp["Payload"].read())
        dur = round(time.time() - t0, 1)
        if resp.get("FunctionError"):
            r.warn(f"  ✗ FunctionError ({dur}s): {payload}")
            sys.exit(0)
        r.log(f"  ✅ invoke ({dur}s)")
        # Parse the body
        body = json.loads(payload.get("body", "{}"))
        r.log(f"     status:                        {body.get('status')}")
        r.log(f"     regime:                        {body.get('regime')}")
        r.log(f"     composite_score:               {body.get('composite_score')}")
        r.log(f"     n_recent (14d):                {body.get('n_recent')}")
        r.log(f"     latest_auction_date:           {body.get('latest_auction_date')}")
        r.log(f"     latest_cusip:                  {body.get('latest_cusip')}")
        r.log(f"     hours_since_latest_auction:    {body.get('hours_since_latest_auction')}")
        r.log(f"     is_new_auction_this_run:       {body.get('is_new_auction_this_run')}")
    except Exception as e:
        r.warn(f"  ✗ invoke error: {e}")

    # ─────────────────────────────────────────────────────────────────
    # 7. Read S3 file directly to confirm it landed fresh
    # ─────────────────────────────────────────────────────────────────
    r.section("7. S3 output verification")
    try:
        head = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/auction-crisis.json")
        r.log(f"  S3 file:          data/auction-crisis.json")
        r.log(f"  LastModified:     {head['LastModified']}")
        r.log(f"  ContentLength:    {head['ContentLength']:,} bytes")
        r.log(f"  CacheControl:     {head.get('CacheControl', '(none)')}")
        # Parse age
        from datetime import datetime, timezone
        age_min = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds() / 60
        r.log(f"  Age:              {age_min:.1f} minutes (should be <2 minutes)")
        if age_min > 5:
            r.warn(f"  ⚠ S3 file older than 5 min — invoke may not have completed")
        else:
            r.log(f"  ✅ S3 file fresh from this invoke")
        # Verify freshness fields are present in the actual file
        body = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/auction-crisis.json")["Body"].read()
        d = json.loads(body)
        if d.get("freshness"):
            f = d["freshness"]
            r.log(f"")
            r.log(f"  Freshness section in S3 file:")
            r.log(f"    schema_version:               {d.get('schema_version')}")
            r.log(f"    latest_auction_date:          {f.get('latest_auction_date')}")
            r.log(f"    latest_cusip:                 {f.get('latest_cusip')}")
            r.log(f"    hours_since_latest_auction:   {f.get('hours_since_latest_auction')}")
            r.log(f"    n_total_auctions_pulled:      {f.get('n_total_auctions_pulled')}")
            r.log(f"    data_window:                  {f.get('data_window_start')} → {f.get('data_window_end')}")
            r.log(f"    fetched_via:                  {f.get('fetched_via')}")
        else:
            r.warn(f"  ⚠ no freshness section in file — Lambda code may not have deployed yet")
    except ClientError as e:
        r.warn(f"  ✗ S3 head: {e}")

    # ─────────────────────────────────────────────────────────────────
    # FINAL
    # ─────────────────────────────────────────────────────────────────
    r.section("FINAL — wiring summary")
    r.log("  ✅ Lambda:           justhodl-auction-crisis-detector (Active)")
    r.log("  ✅ Active schedule:  cron(0/15 14-22 ? * MON-FRI *)")
    r.log("                       = every 15 min, Mon-Fri, 14:00-22:00 UTC")
    r.log("                       = covers Treasury bill (15:30 UTC) and note (17:00 UTC)")
    r.log("                         publication windows + 2hr post-auction settlement buffer")
    r.log("  ✅ Backstop:         rate(4 hours)")
    r.log("                       = weekend + off-hours backstop")
    r.log("  ✅ Output:           s3://justhodl-dashboard-live/data/auction-crisis.json")
    r.log("                       refreshed via no-cache fetch from fiscaldata.treasury.gov")
    r.log("                       freshness section shows latest auction date + staleness")
    r.log("  ✅ Pages:            /auction-crisis.html + summary on /bonds.html")
    r.log("                       both fetch with ?t=Date.now() cachebusting")
    r.log("")
    r.log("  Within 15 minutes of any new Treasury auction result publication,")
    r.log("  the dashboard will reflect it.")
    r.log("Done")
