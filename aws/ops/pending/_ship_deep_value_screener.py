"""
Ship justhodl-deep-value-screener Lambda — net-cash + revenue + cash flow screener.
Steps:
  1. Build deployment zip
  2. Create or update Lambda
  3. Schedule daily 09:00 UTC
  4. Smoke-invoke and parse response
  5. Verify S3 output
"""
import io, json, os, time, zipfile, base64
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-deep-value-screener"
SCHEDULE_NAME = "justhodl-deep-value-screener-daily"
SCHEDULE_EXPR = "cron(0 9 * * ? *)"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
SOURCE_FILE = "aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py"

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Build deployment zip")
    src = open(SOURCE_FILE, "r", encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    log(f"  zip size: {len(zb):,}b")

    L = boto3.client("lambda", region_name=REGION)
    EB = boto3.client("events", region_name=REGION)
    S3 = boto3.client("s3", region_name=REGION)

    section("2) Create or update Lambda")
    exists = False
    try:
        L.get_function(FunctionName=LAMBDA_NAME)
        exists = True
        log(f"  exists — updating code")
    except L.exceptions.ResourceNotFoundException:
        log(f"  creating new")

    if exists:
        L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
        for _ in range(30):
            cfg = L.get_function_configuration(FunctionName=LAMBDA_NAME)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        L.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=1024,
            Timeout=300,
            Environment={"Variables": {
                "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
                "MAX_TICKERS": "500",
                "TIMEOUT_BUDGET_S": "240",
                "MIN_MCAP": "200000000",
                "NET_CASH_RATIO": "0.50",
                "REV_RATIO": "0.40",
                "N_WORKERS": "8",
            }},
        )
    else:
        L.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN,
            Code={"ZipFile": zb},
            Timeout=300,
            MemorySize=1024,
            Environment={"Variables": {
                "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
                "MAX_TICKERS": "500",
                "TIMEOUT_BUDGET_S": "240",
                "MIN_MCAP": "200000000",
                "NET_CASH_RATIO": "0.50",
                "REV_RATIO": "0.40",
                "N_WORKERS": "8",
            }},
        )
    for _ in range(30):
        cfg = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if cfg.get("LastUpdateStatus") == "Successful" and cfg.get("State") == "Active":
            break
        time.sleep(1)
    log(f"  ✓ ready, state={cfg.get('State')}")

    section("3) Schedule EventBridge daily 09:00 UTC")
    rule_arn = EB.put_rule(
        Name=SCHEDULE_NAME, ScheduleExpression=SCHEDULE_EXPR, State="ENABLED",
        Description="justhodl-deep-value-screener — daily 09:00 UTC",
    )["RuleArn"]
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}"
    EB.put_targets(Rule=SCHEDULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        L.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId=f"{SCHEDULE_NAME}-eb",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
        log(f"  ✓ permission added")
    except L.exceptions.ResourceConflictException:
        log(f"  permission already exists")
    log(f"  rule: {SCHEDULE_NAME}  expr={SCHEDULE_EXPR}")

    section("4) Smoke invoke")
    t0 = time.time()
    r = L.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                 LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log(f"  status: {r['StatusCode']}  duration: {dur:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {json.dumps(body)[:500]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail ──")
        for ln in tail.splitlines()[-15:]:
            log(f"    {ln.rstrip()}")

    section("5) S3 output")
    obj = S3.get_object(Bucket=BUCKET, Key="data/deep-value.json")
    data = json.loads(obj["Body"].read())
    log(f"  size: {len(obj['Body'].read() if False else json.dumps(data)):,}b")
    log(f"  schema: {data.get('schema_version')}")
    log(f"  generated_at: {data.get('generated_at')}")
    log(f"  stats: {json.dumps(data.get('stats', {}))}")

    log("")
    log("  ── Top 15 deep-value setups ──")
    log(f"  {'#':>2} {'Symbol':<8} {'Score':>6} {'Flag':<22} {'%NC':>6} {'%Rev':>6} {'M/R':>5} {'%52H':>6} {'Sector':<22}")
    for i, c in enumerate(data.get("summary", {}).get("top_25_overall", [])[:15], 1):
        log(f"  {i:>2} {c['symbol']:<8} {c['score']:>6.1f} {c['flag']:<22} "
            f"{c['net_cash_pct']*100:>5.0f}% {c['rev_yield']*100:>5.0f}% "
            f"{c['mcap_to_rev']:>5.2f} {c['pct_from_52w_high']:>+5.0f}% "
            f"{c.get('sector', '')[:22]:<22}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "ship_deep_value_screener.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
