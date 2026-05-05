"""
Ship Layer 5: justhodl-nobrainer-rationale (Claude haiku-4-5 thesis writer).

Schedules daily 13:45 UTC (15min after Layer 4).
Fetches Anthropic key from SSM /justhodl/anthropic/api-key, plus Telegram creds.
Smoke-invokes with SKIP_TELEGRAM=1 first time so we don't spam the channel
during deploy verification, then verifies S3 output. Schedule will use the
default env (Telegram enabled) once verified.
"""
import io
import json
import time
import zipfile

import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-nobrainer-rationale"
SCHEDULE_NAME = "justhodl-nobrainer-rationale-daily"
SCHEDULE_EXPR = "cron(45 13 * * ? *)"  # daily 13:45 UTC, 15min after Layer 4
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
SOURCE_FILE = "aws/lambdas/justhodl-nobrainer-rationale/source/lambda_function.py"


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
    ssm = boto3.client("ssm", region_name=REGION)

    # Pre-pull anthropic key from SSM and set as env var (faster cold start)
    anthropic_key = ""
    try:
        anthropic_key = ssm.get_parameter(
            Name="/justhodl/anthropic/api-key", WithDecryption=True
        )["Parameter"]["Value"]
        log(f"✓ pulled Anthropic key from SSM ({len(anthropic_key)} chars)")
    except Exception as e:
        log(f"WARN — could not pull anthropic key from SSM: {e}")
        log("Lambda will fall back to SSM lookup at runtime")

    env_vars = {
        "N_THESES": "10",
        "N_DIGEST": "3",
        "MIN_SCORE": "55",
    }
    if anthropic_key:
        env_vars["ANTHROPIC_KEY"] = anthropic_key

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
            Timeout=600,
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
            Timeout=600,
            Description="Layer 5 of nobrainer hunter — Claude-written theses + Telegram digest",
            Environment={"Variables": env_vars},
        )
        time.sleep(3)
        cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
        log(f"✓ created, code hash: {cfg.get('CodeSha256')[:16]}...")

    section("3) Schedule daily 13:45 UTC")
    events = boto3.client("events", region_name=REGION)
    try:
        events.put_rule(
            Name=SCHEDULE_NAME,
            ScheduleExpression=SCHEDULE_EXPR,
            State="ENABLED",
            Description="Daily Layer 5 nobrainer-rationale, 15min after Layer 4",
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

    section("4) Smoke-invoke with SKIP_TELEGRAM=1")
    # First invocation — skip telegram so the verification doesn't spam Khalid's chat
    log("Setting SKIP_TELEGRAM=1 for smoke invoke")
    smoke_env = dict(env_vars)
    smoke_env["SKIP_TELEGRAM"] = "1"
    lam.update_function_configuration(
        FunctionName=LAMBDA_NAME,
        Environment={"Variables": smoke_env},
    )
    for _ in range(30):
        cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
        if cfg.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)

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
            if any(tag in line for tag in ("[rationale]", "[tg]", "ERR", "ERROR")):
                log(f"  {line}")

    # Restore env (Telegram enabled for scheduled runs)
    log("Restoring env (Telegram re-enabled for scheduled runs)")
    lam.update_function_configuration(
        FunctionName=LAMBDA_NAME,
        Environment={"Variables": env_vars},
    )

    section("5) Verify S3 output")
    s3 = boto3.client("s3", region_name=REGION)
    try:
        head = s3.head_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
        log(f"S3 size: {head['ContentLength']:,}b")
        log(f"S3 last_modified: {head['LastModified'].isoformat()}")
        obj = s3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
        data = json.loads(obj["Body"].read())
        log(f"v: {data.get('schema_version')}")
        log(f"method: {data.get('method')}")
        log(f"n_theses: {data.get('n_theses')}")
        log(f"n_claude_ok: {data.get('n_claude_ok')}")
        log(f"n_claude_fail: {data.get('n_claude_fail')}")
        log(f"model: {data.get('model')}")
        log(f"skipped_claude: {data.get('skipped_claude')}")
        log("")
        log("── First 2 thesis previews ──")
        for t in data.get("theses", [])[:2]:
            log(f"  {t.get('ticker')} ({t.get('theme_etf')}) score={t.get('asymmetric_score')}")
            thesis = (t.get("thesis") or "").strip()
            for line in thesis.split("\n")[:8]:
                log(f"    │ {line[:140]}")
            log("    └─")
    except Exception as e:
        log(f"S3 verify ERR: {e}")
        import traceback
        log(traceback.format_exc())


if __name__ == "__main__":
    main()
