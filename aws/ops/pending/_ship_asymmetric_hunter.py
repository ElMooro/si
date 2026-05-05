"""
Ship Layer 4 of nobrainer hunter pipeline: justhodl-asymmetric-hunter

Steps:
  1. Build deployment zip from aws/lambdas/justhodl-asymmetric-hunter/source/
  2. Create or update Lambda
  3. Schedule daily 13:30 UTC via EventBridge (after Layers 1, 2, 3 complete)
  4. Smoke-invoke and parse response
  5. Verify S3 output at data/nobrainers.json
"""
import io
import json
import time
import zipfile

import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-asymmetric-hunter"
SCHEDULE_NAME = "justhodl-asymmetric-hunter-daily"
SCHEDULE_EXPR = "cron(30 13 * * ? *)"  # daily 13:30 UTC (after Layer 3 at 08:00 + buffer)
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
SOURCE_FILE = "aws/lambdas/justhodl-asymmetric-hunter/source/lambda_function.py"


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
            MemorySize=1024,
            Timeout=600,
            Environment={"Variables": {
                "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
            }},
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
            MemorySize=1024,
            Timeout=600,
            Description="Layer 4 of nobrainer hunter — fuses Layers 1+2+3 into 5-factor asymmetric score",
            Environment={"Variables": {
                "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
            }},
        )
        time.sleep(3)
        cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
        log(f"✓ created, code hash: {cfg.get('CodeSha256')[:16]}...")

    section("3) Schedule daily 13:30 UTC")
    events = boto3.client("events", region_name=REGION)
    try:
        events.put_rule(
            Name=SCHEDULE_NAME,
            ScheduleExpression=SCHEDULE_EXPR,
            State="ENABLED",
            Description="Daily Layer 4 asymmetric-hunter run after L1/L2/L3 complete",
        )
        log(f"✓ rule {SCHEDULE_NAME} ({SCHEDULE_EXPR})")
    except Exception as e:
        log(f"rule put error (non-fatal): {e}")

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
        log(f"target wire error (non-fatal): {e}")

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
            if any(tag in line for tag in ("[hunter]", "TOP", "ERROR", "ERR")):
                log(f"  {line}")

    section("5) Verify S3 output at data/nobrainers.json")
    s3 = boto3.client("s3", region_name=REGION)
    try:
        head = s3.head_object(Bucket=BUCKET, Key="data/nobrainers.json")
        log(f"S3 size: {head['ContentLength']:,}b")
        log(f"S3 last_modified: {head['LastModified'].isoformat()}")
        obj = s3.get_object(Bucket=BUCKET, Key="data/nobrainers.json")
        data = json.loads(obj["Body"].read())
        log(f"v: {data.get('schema_version')}")
        log(f"method: {data.get('method')}")
        log(f"n_candidates_scored: {data.get('n_candidates_scored')}")
        log(f"n_unique_tickers: {data.get('n_unique_tickers')}")
        log(f"layers_loaded: {data.get('layers_loaded')}")

        summ = data.get("summary", {})
        log(f"n_tier_a_nobrainer:    {summ.get('n_tier_a_nobrainer')}")
        log(f"n_tier_b_high_conv:    {summ.get('n_tier_b_high_conviction')}")
        log(f"n_tier_c_watchlist:    {summ.get('n_tier_c_watchlist')}")
        log(f"n_mu_grade:            {summ.get('n_mu_grade')}")

        log("")
        log("── Top 10 Overall ──")
        for x in summ.get("top_25_overall", [])[:10]:
            f = x.get("factors", {})
            fund = x.get("fundamentals", {})
            log(f"  {x['ticker']:6s} ({x['theme_etf']:5s} t={x['tier']} {x['theme_phase']:13s}) "
                f"score={x['asymmetric_score']:5.1f} {x['flag']}")
            log(f"     factors: theme={f.get('theme_attribution'):.1f} infl={f.get('primary_inflated'):.1f} "
                f"supply={f.get('supply_inflection'):.1f} val={f.get('valuation_asym'):.1f} "
                f"cat={f.get('catalyst_prox'):.1f}")
            mtr = fund.get("mcap_to_rev")
            ps = fund.get("p_s")
            if mtr is not None:
                log(f"     fundamentals: mcap_to_rev={mtr:.2f}  P/S={ps if ps else 'n/a'}  "
                    f"earnings={x.get('next_earnings') or 'n/a'}")

        log("")
        log("── MU-grade top 10 (low mcap_to_rev tier-2/3) ──")
        for x in summ.get("mu_grade_top_15", [])[:10]:
            fund = x.get("fundamentals", {})
            log(f"  {x['ticker']:6s} ({x['theme_etf']:5s}) score={x['asymmetric_score']:5.1f} "
                f"mcap_to_rev={fund.get('mcap_to_rev'):.2f} P/S={fund.get('p_s')}")

    except Exception as e:
        log(f"S3 verify ERR: {e}")
        import traceback
        log(traceback.format_exc())


if __name__ == "__main__":
    main()
