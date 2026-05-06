#!/usr/bin/env python3
"""
Step 215 — Phase 4 prep: inspect justhodl-khalid-metrics before
creating its replacement.

A. Current Lambda config: runtime, memory, timeout, env vars,
   Function URL, IAM role, code size
B. EventBridge rules targeting it: schedule + state
C. S3 keys it reads/writes (look in source for put_object Key=)
D. Anyone else's Lambda code referencing it
E. /ka/index.html and /khalid/index.html — what Lambda URL do
   they call?
"""
import io, json, time, urllib.request, zipfile
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OLD = "justhodl-khalid-metrics"
NEW = "justhodl-ka-metrics"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
iam = boto3.client("iam")
cw = boto3.client("cloudwatch", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("phase4_prep_lambda_inspect") as r:
    r.heading("Phase 4 prep — inspect justhodl-khalid-metrics")

    # ── A. Function configuration ──
    r.section(f"A. {OLD} configuration")
    try:
        info = lam.get_function(FunctionName=OLD)
        cfg = info["Configuration"]
        r.log(f"  ARN: {cfg['FunctionArn']}")
        r.log(f"  runtime: {cfg.get('Runtime')}  memory: {cfg.get('MemorySize')}MB  timeout: {cfg.get('Timeout')}s")
        r.log(f"  state: {cfg.get('State')}  last_modified: {cfg.get('LastModified', '')[:19]}")
        r.log(f"  role: {cfg.get('Role', '?').split('/')[-1]}")
        r.log(f"  code size: {cfg.get('CodeSize')}B")
        r.log(f"  handler: {cfg.get('Handler')}")
        env = cfg.get('Environment', {}).get('Variables', {})
        r.log(f"  env vars ({len(env)}): {list(env.keys())}")
        # Don't dump values — they may contain API keys
        info_ok = True
    except ClientError as e:
        r.warn(f"  fail: {e}")
        info_ok = False

    if not info_ok:
        r.log("Lambda not found — Phase 4 may already be complete or Lambda was deleted")
    else:
        pass  # continue with all subsequent sections (kept inside with-block)
    # ── A2. Function URL? ──
    try:
        url_cfg = lam.get_function_url_config(FunctionName=OLD)
        r.log(f"  Function URL: {url_cfg.get('FunctionUrl')} auth={url_cfg.get('AuthType')}")
    except ClientError:
        r.log(f"  Function URL: none")

    # ── A3. Concurrency / reserved ──
    try:
        rc = lam.get_function_concurrency(FunctionName=OLD)
        r.log(f"  reserved concurrency: {rc.get('ReservedConcurrentExecutions', 'unset')}")
    except ClientError:
        r.log(f"  reserved concurrency: unset")

    # ── B. EventBridge rules targeting it ──
    r.section(f"B. EventBridge rules targeting {OLD}")
    rules_targeting = []
    next_token = None
    while True:
        kwargs = {"Limit": 100}
        if next_token: kwargs["NextToken"] = next_token
        rules_resp = events.list_rules(**kwargs)
        for rule in rules_resp.get("Rules", []):
            try:
                tgts = events.list_targets_by_rule(Rule=rule["Name"])
                for t in tgts.get("Targets", []):
                    arn = t.get("Arn", "")
                    if ":lambda:" in arn and arn.split(":")[-1] == OLD:
                        rules_targeting.append({
                            "name": rule["Name"],
                            "state": rule.get("State"),
                            "schedule": rule.get("ScheduleExpression", ""),
                            "target_id": t.get("Id"),
                            "input": t.get("Input", "")[:200],
                        })
            except Exception:
                continue
        next_token = rules_resp.get("NextToken")
        if not next_token: break
    r.log(f"  {len(rules_targeting)} rules:")
    for rule in rules_targeting:
        r.log(f"    name={rule['name']:50}  state={rule['state']:10}  sched={rule['schedule']}")
        r.log(f"      target_id={rule['target_id']}  input={rule['input']}")

    # ── C. Inspect deployed source for S3 writes ──
    r.section(f"C. {OLD} source — S3 keys written/read")
    try:
        with urllib.request.urlopen(info["Code"]["Location"], timeout=30) as resp:
            zb = resp.read()
        with zipfile.ZipFile(io.BytesIO(zb)) as zf:
            files = zf.namelist()
            r.log(f"  zip files: {files}")
            try:
                src = zf.read("lambda_function.py").decode("utf-8", errors="replace")
            except KeyError:
                # Try other handler names
                for f in files:
                    if f.endswith(".py"):
                        src = zf.read(f).decode("utf-8", errors="replace")
                        r.log(f"  (using {f})")
                        break
        # Find S3 keys
        import re
        s3_writes = re.findall(r'put_object\([^)]*Key\s*=\s*[\'"]([^\'"]+)[\'"]', src)
        s3_reads = re.findall(r'get_object\([^)]*Key\s*=\s*[\'"]([^\'"]+)[\'"]', src)
        r.log(f"  S3 writes ({len(s3_writes)}): {sorted(set(s3_writes))[:8]}")
        r.log(f"  S3 reads  ({len(s3_reads)}): {sorted(set(s3_reads))[:8]}")
        r.log(f"  source line count: {len(src.splitlines())}")
    except Exception as e:
        r.warn(f"  source fetch fail: {e}")

    # ── D. Anyone else referencing the old function URL? ──
    r.section("D. Frontend pages referencing old Lambda URL")
    # Old function URL we pulled in step C
    try:
        url_cfg = lam.get_function_url_config(FunctionName=OLD)
        old_url = url_cfg.get("FunctionUrl", "")
        # Strip the protocol+slash for grep
        url_token = old_url.replace("https://", "").rstrip("/")
        r.log(f"  searching repo for token: {url_token}")
    except ClientError:
        r.log("  no Function URL")

    # ── E. Recent invocations (was it actually running?) ──
    r.section(f"E. {OLD} invocations last 7 days")
    now = datetime.now(timezone.utc)
    metrics = cw.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName="Invocations",
        Dimensions=[{"Name":"FunctionName","Value":OLD}],
        StartTime=now - timedelta(days=7),
        EndTime=now,
        Period=86400,
        Statistics=["Sum"],
    )
    total_inv = sum(d.get("Sum",0) for d in metrics.get("Datapoints",[]))
    r.log(f"  total invocations 7d: {int(total_inv)}")
    errors_metrics = cw.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName="Errors",
        Dimensions=[{"Name":"FunctionName","Value":OLD}],
        StartTime=now - timedelta(days=7),
        EndTime=now,
        Period=86400,
        Statistics=["Sum"],
    )
    total_err = sum(d.get("Sum",0) for d in errors_metrics.get("Datapoints",[]))
    r.log(f"  total errors 7d: {int(total_err)}")

    # ── F. Does NEW already exist? ──
    r.section(f"F. Does {NEW} already exist?")
    try:
        new_info = lam.get_function(FunctionName=NEW)
        r.warn(f"  ⚠ {NEW} already exists — Phase 4 already partially run?")
        r.log(f"  arn={new_info['Configuration']['FunctionArn']}")
    except ClientError as e:
        if "ResourceNotFoundException" in str(e):
            r.log(f"  ✅ {NEW} does not yet exist — safe to create")
        else:
            r.warn(f"  unexpected: {e}")

    r.log("Done")
