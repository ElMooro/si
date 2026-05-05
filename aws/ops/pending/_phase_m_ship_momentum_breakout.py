"""Phase M — Ship momentum-breakout Lambda + verify pump-list catch."""
import io, json, time, base64, zipfile, os
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-momentum-breakout"
SCHEDULE_NAME = "justhodl-momentum-breakout-daily"
SCHEDULE_EXPR = "cron(0 13 * * ? *)"  # 13:00 UTC daily — before nobrainer 13:30
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"

L = boto3.client("lambda", region_name=REGION)
EB = boto3.client("events", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    src = open("aws/lambdas/justhodl-momentum-breakout/source/lambda_function.py").read()
    log(f"  source: {len(src)} chars")

    section("1) Build zip + create/update Lambda")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    log(f"  zip: {len(zb):,}b")

    env = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
        "MAX_TICKERS": "600",
        "TIMEOUT_BUDGET_S": "260",
        "MIN_DOLLAR_VOL": "5000000",
        "N_WORKERS": "12",
    }
    exists = False
    try:
        L.get_function(FunctionName=LAMBDA_NAME)
        exists = True
        log("  exists — updating")
    except L.exceptions.ResourceNotFoundException:
        log("  creating new")

    if exists:
        L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
        for _ in range(30):
            c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        L.update_function_configuration(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=1024, Timeout=300,
            Environment={"Variables": env},
        )
    else:
        L.create_function(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN, Code={"ZipFile": zb},
            Timeout=300, MemorySize=1024,
            Environment={"Variables": env},
        )

    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ ready, mem={c['MemorySize']}MB to={c['Timeout']}s")

    section("2) Schedule daily 13:00 UTC")
    rule_arn = EB.put_rule(Name=SCHEDULE_NAME, ScheduleExpression=SCHEDULE_EXPR, State="ENABLED")["RuleArn"]
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}"
    EB.put_targets(Rule=SCHEDULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        L.add_permission(FunctionName=LAMBDA_NAME, StatementId=f"{SCHEDULE_NAME}-eb",
                          Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                          SourceArn=rule_arn)
        log("  ✓ permission added")
    except L.exceptions.ResourceConflictException:
        log("  ✓ permission already exists")

    section("3) Smoke invoke (will take ~120-200s — pulls 90d history for 600 tickers)")
    from botocore.config import Config
    L2 = boto3.client("lambda", region_name=REGION,
                       config=Config(read_timeout=600, connect_timeout=10))
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log(f"  status: {r['StatusCode']}, dur: {dur:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {json.dumps(body)[:300]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-15:]:
            log(f"    {ln.rstrip()}")

    section("4) Verify output + pump-list coverage")
    obj = S3.get_object(Bucket=BUCKET, Key="data/momentum-breakout.json")
    d = json.loads(obj["Body"].read())
    log(f"  schema: {d.get('schema_version')}")
    log(f"  generated_at: {d.get('generated_at')}")
    log(f"  stats: {json.dumps(d.get('stats', {}))}")
    log("")
    log("  ── top 15 momentum picks ──")
    for c in d.get("summary", {}).get("top_25_overall", [])[:15]:
        log(f"    {c['symbol']:<6} {c['score']:>5.1f} {c['tier']:<20}  20d={c.get('ret_20d','?')}% 60d={c.get('ret_60d','?')}% volR={c.get('vol_ratio','?')}")

    section("5) Pump-list — what would have been caught EARLY")
    targets = ["AXTI","LWLG","AAOI","AEHR","SNDK","ICHR","MRVL","INTC",
               "VIAV","LITE","CRDO","MU","TER","WOLF","ON","QRVO"]
    by_sym = {r["symbol"]: r for r in d.get("all_qualifying", [])}
    for t in targets:
        if t in by_sym:
            r = by_sym[t]
            log(f"    {t:<6} score={r['score']:>5.1f} {r['tier']:<22} ret60d={r['metrics'].get('ret_60d_pct','?')}% rs60d={r['metrics'].get('rs_vs_spy_60d_pct','?')}")
        else:
            log(f"    {t:<6} not in momentum (probably not in universe yet)")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_m_ship_momentum.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
