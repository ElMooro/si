"""
Ship Layer 6 of nobrainer hunter: justhodl-nobrainer-tracker
Schedules every 1 hour; logs each Layer 4 nobrainer call to justhodl-signals
DDB so the existing horizon-aware calibrator measures performance over
30/60/90/180 day windows.
"""
import io
import json
import time
import zipfile

import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-nobrainer-tracker"
SCHEDULE_NAME = "justhodl-nobrainer-tracker-hourly"
SCHEDULE_EXPR = "rate(1 hour)"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
SOURCE_FILE = "aws/lambdas/justhodl-nobrainer-tracker/source/lambda_function.py"


def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"- `{ts}`   {msg}")


def section(title):
    print(f"\n# {title}\n")


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

    env_vars = {
        "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
        "MIN_TRACK_SCORE": "60",
        "MAX_LOGS_PER_RUN": "20",
        "SCORE_DELTA_TRIGGER": "5",
        "RECONFIRM_HOURS": "168",
    }

    if exists:
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
        for _ in range(30):
            cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=512,
            Timeout=300,
            Environment={"Variables": env_vars},
        )
        for _ in range(30):
            cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        log(f"✓ updated, code hash: {cfg.get('CodeSha256')[:16]}...")
    else:
        lam.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            MemorySize=512,
            Timeout=300,
            Description="Layer 6 of nobrainer hunter — logs each top call as DDB signal for calibrator",
            Environment={"Variables": env_vars},
        )
        time.sleep(3)
        cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
        log(f"✓ created, code hash: {cfg.get('CodeSha256')[:16]}...")

    section("3) Schedule rate(1 hour)")
    events = boto3.client("events", region_name=REGION)
    try:
        events.put_rule(
            Name=SCHEDULE_NAME,
            ScheduleExpression=SCHEDULE_EXPR,
            State="ENABLED",
            Description="Hourly Layer 6 nobrainer-tracker — logs to DDB justhodl-signals",
        )
        log(f"✓ rule {SCHEDULE_NAME} ({SCHEDULE_EXPR})")
    except Exception as e:
        log(f"rule put err (non-fatal): {e}")

    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId=f"{SCHEDULE_NAME}-perm",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{SCHEDULE_NAME}",
        )
        log("✓ EventBridge invoke permission added")
    except lam.exceptions.ResourceConflictException:
        log("✓ permission already present")

    try:
        events.put_targets(
            Rule=SCHEDULE_NAME,
            Targets=[{
                "Id": "1",
                "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}",
            }],
        )
        log("✓ EventBridge target wired")
    except Exception as e:
        log(f"target wire err (non-fatal): {e}")

    section("4) Smoke-invoke")
    t0 = time.time()
    resp = lam.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=b"{}",
    )
    dur = time.time() - t0
    payload = json.loads(resp["Payload"].read())
    log(f"status: {resp['StatusCode']}, duration: {round(dur,1)}s")
    log(f"payload: {payload}")

    if resp.get("LogResult"):
        import base64
        logs = base64.b64decode(resp["LogResult"]).decode("utf-8", errors="replace")
        for line in logs.splitlines():
            if any(tag in line for tag in ("[track]", "[track-LOG]", "[poly]", "[regime]", "ERR", "ERROR")):
                log(f"  {line}")

    section("5) Verify state file + count signals in DDB")
    s3 = boto3.client("s3", region_name=REGION)
    try:
        head = s3.head_object(Bucket=BUCKET, Key="portfolio/nobrainer-tracker-state.json")
        log(f"State file size: {head['ContentLength']:,}b")
        log(f"State last_modified: {head['LastModified'].isoformat()}")
        obj = s3.get_object(Bucket=BUCKET, Key="portfolio/nobrainer-tracker-state.json")
        state = json.loads(obj["Body"].read())
        log(f"n_runs: {state.get('n_runs')}")
        log(f"n_logs_total: {state.get('n_logs_total')}")
        log(f"unique (ticker,theme) tracked: {len(state.get('last_logged') or {})}")

        # Show top 5 most-recently logged
        last = state.get("last_logged") or {}
        if last:
            sorted_last = sorted(last.items(), key=lambda kv: kv[1].get("logged_at", ""), reverse=True)
            log("")
            log("── Most-recently logged (top 5) ──")
            for k, v in sorted_last[:5]:
                log(f"  {k}: score={v.get('score')} at {v.get('logged_at')}")
    except Exception as e:
        log(f"state verify err: {e}")

    # DDB query: count nobrainer_* signals
    try:
        ddb = boto3.resource("dynamodb", region_name=REGION)
        table = ddb.Table("justhodl-signals")
        # Scan with filter on signal_type prefix
        from boto3.dynamodb.conditions import Attr
        n = 0
        sample = []
        scan_kwargs = {"FilterExpression": Attr("signal_type").begins_with("nobrainer_")}
        while True:
            r = table.scan(**scan_kwargs)
            n += len(r.get("Items", []))
            for it in r.get("Items", [])[:5]:
                if len(sample) < 5:
                    sample.append({
                        "stype": it.get("signal_type"),
                        "ticker": it.get("signal_value"),
                        "logged": it.get("logged_at"),
                    })
            if "LastEvaluatedKey" not in r:
                break
            scan_kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]
        log("")
        log(f"DDB nobrainer_* signal count: {n}")
        for s in sample:
            log(f"  {s['stype']} {s['ticker']} @ {s['logged']}")
    except Exception as e:
        log(f"DDB scan err: {e}")


if __name__ == "__main__":
    main()
