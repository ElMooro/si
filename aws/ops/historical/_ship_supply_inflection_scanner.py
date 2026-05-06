"""
Ship Layer 2 of nobrainer hunter pipeline: justhodl-supply-inflection-scanner

Steps:
  1. Create or update Lambda from aws/lambdas/justhodl-supply-inflection-scanner/source/
  2. Schedule daily 07:00 UTC via EventBridge
  3. Smoke-invoke and parse response
  4. Verify S3 output written to data/supply-inflection.json
"""
import io
import json
import time
import zipfile
import os
import sys

import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-supply-inflection-scanner"
SCHEDULE_NAME = "justhodl-supply-inflection-scanner-daily"
SCHEDULE_EXPR = "cron(0 7 * * ? *)"  # daily 07:00 UTC
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
SOURCE_FILE = "aws/lambdas/justhodl-supply-inflection-scanner/source/lambda_function.py"

REPORT = []


def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"- `{ts}`   {msg}")
    REPORT.append(f"- `{ts}`   {msg}")


def section(title):
    print(f"\n# {title}\n")
    REPORT.append(f"\n# {title}\n")


def main():
    section("1) Build deployment zip")
    src = open(SOURCE_FILE, "r", encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zip_bytes = buf.getvalue()
    log(f"zip size: {len(zip_bytes):,}b")

    lam = boto3.client("lambda", region_name=REGION)

    section("2) Create or update Lambda")
    exists = False
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        exists = True
        log(f"Lambda {LAMBDA_NAME} exists — updating code")
    except lam.exceptions.ResourceNotFoundException:
        log(f"Lambda {LAMBDA_NAME} does not exist — creating")

    if exists:
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        # wait for code update to settle
        for _ in range(20):
            cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        # ensure config aligned
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=1024,
            Timeout=300,
            Environment={"Variables": {
                "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
                "FRED_KEY": "2f057499936072679d8843d7fce99989",
            }},
        )
        for _ in range(20):
            cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
    else:
        lam.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            MemorySize=1024,
            Timeout=300,
            Environment={"Variables": {
                "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
                "FRED_KEY": "2f057499936072679d8843d7fce99989",
            }},
        )
        for _ in range(20):
            cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
            if cfg.get("State") == "Active":
                break
            time.sleep(1)
    log("✅ Lambda deployed")

    section("3) Schedule daily 07:00 UTC")
    events = boto3.client("events", region_name=REGION)
    try:
        events.put_rule(
            Name=SCHEDULE_NAME,
            ScheduleExpression=SCHEDULE_EXPR,
            State="ENABLED",
            Description="Daily run of supply-inflection-scanner at 07:00 UTC",
        )
        log(f"Rule put: {SCHEDULE_NAME} ({SCHEDULE_EXPR})")
        # Permission for events to invoke lambda
        try:
            lam.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId=f"{SCHEDULE_NAME}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{SCHEDULE_NAME}",
            )
            log("Lambda invoke permission added")
        except lam.exceptions.ResourceConflictException:
            log("Permission already exists (ok)")
        # Target
        events.put_targets(
            Rule=SCHEDULE_NAME,
            Targets=[{
                "Id": "1",
                "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}",
            }]
        )
        log("Target attached")
    except Exception as e:
        log(f"⚠️ schedule warning: {type(e).__name__} {e}")

    section("4) Smoke invoke (LogType=Tail)")
    invoke_started = time.time()
    resp = lam.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=b"{}",
    )
    invoke_dur = round(time.time() - invoke_started, 1)
    log(f"status: {resp['StatusCode']}, duration: {invoke_dur}s")

    payload = json.loads(resp["Payload"].read())
    body = json.loads(payload.get("body", "{}")) if isinstance(payload, dict) else {}

    log("")
    log("── Response body ──")
    for k, v in body.items():
        log(f"  {k}: {v}")

    if "LogResult" in resp:
        import base64
        log_text = base64.b64decode(resp["LogResult"]).decode("utf-8", errors="replace")
        log("")
        log("── Log tail ──")
        for line in log_text.splitlines()[-25:]:
            log(f"  {line}")

    section("5) Verify S3 output")
    s3 = boto3.client("s3", region_name=REGION)
    try:
        head = s3.head_object(Bucket=BUCKET, Key="data/supply-inflection.json")
        log(f"S3 size: {head['ContentLength']:,}b")
        log(f"S3 last_modified: {head['LastModified']}")
    except Exception as e:
        log(f"⚠️ S3 read failed: {e}")
        return

    obj = s3.get_object(Bucket=BUCKET, Key="data/supply-inflection.json")
    data = json.loads(obj["Body"].read())
    log(f"v: {data.get('schema_version')}")
    log(f"method: {data.get('method')}")
    log(f"n_signals: {data.get('summary', {}).get('n_signals_scored')}")
    log(f"n_strong_tightening: {data.get('summary', {}).get('n_strong_tightening')}")
    log(f"n_tightening: {data.get('summary', {}).get('n_tightening')}")
    log(f"n_easing: {data.get('summary', {}).get('n_easing')}")

    log("")
    log("── Top 8 tightening signals ──")
    for s in (data.get("summary", {}).get("top_signals") or [])[:8]:
        log(f"  {s['name']:<25} {s['symbol']:<8} score={s['score']:>5.1f} flag={s['flag']:<20} themes={s['themes']}")

    log("")
    log("── Top 8 inflecting themes ──")
    for t in (data.get("summary", {}).get("top_inflecting_themes") or [])[:8]:
        log(f"  {t['theme']:<6} score={t['score']:>5.1f} n_strong={t['n_strong']} n_tightening={t['n_tightening']}")

    log("")
    log("── Sample signal detail (top 1) ──")
    top = (data.get("summary", {}).get("top_signals") or [])[0] if data.get("summary", {}).get("top_signals") else None
    if top:
        full = data["signals"].get(top["name"], {})
        m = full.get("metrics", {})
        log(f"  {top['name']} ({full.get('symbol')}): score={full.get('score')} flag={full.get('flag')}")
        log(f"  metrics: pct_30d={m.get('pct_change_30d')} pct_90d={m.get('pct_change_90d')} pct_180d={m.get('pct_change_180d')}")
        log(f"           pctl_365d={m.get('percentile_365d')} vol_90d={m.get('realized_vol_90d')} latest={m.get('latest_value')}")


if __name__ == "__main__":
    main()
    # Persist report next to ops infra
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "ship_supply_inflection_scanner.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print(f"\n[report written] {out_path}")
