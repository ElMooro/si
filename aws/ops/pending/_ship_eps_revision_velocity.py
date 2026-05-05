"""Ship justhodl-eps-revision-velocity Lambda."""
import io, json, os, time, zipfile, base64
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-eps-revision-velocity"
SCHEDULE_NAME = "justhodl-eps-revision-velocity-daily"
SCHEDULE_EXPR = "cron(30 9 * * ? *)"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
SOURCE_FILE = "aws/lambdas/justhodl-eps-revision-velocity/source/lambda_function.py"

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Build zip")
    src = open(SOURCE_FILE, "r", encoding="utf-8").read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    log(f"  zip: {len(zb):,}b")

    L = boto3.client("lambda", region_name=REGION)
    EB = boto3.client("events", region_name=REGION)
    S3 = boto3.client("s3", region_name=REGION)

    section("2) Create or update Lambda")
    try:
        L.get_function(FunctionName=LAMBDA_NAME)
        log("  exists — updating")
        L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
    except L.exceptions.ResourceNotFoundException:
        log("  creating new")
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
                "MAX_TICKERS": "400",
                "TIMEOUT_BUDGET_S": "240",
                "MIN_MCAP": "300000000",
                "N_WORKERS": "10",
                "MIN_VELOCITY_PCT": "5.0",
            }},
        )

    for _ in range(30):
        cfg = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if cfg.get("LastUpdateStatus") == "Successful" and cfg.get("State") == "Active":
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
            "MAX_TICKERS": "400",
            "TIMEOUT_BUDGET_S": "240",
            "MIN_MCAP": "300000000",
            "N_WORKERS": "10",
            "MIN_VELOCITY_PCT": "5.0",
        }},
    )
    for _ in range(30):
        cfg = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if cfg.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ ready")

    section("3) Schedule daily 09:30 UTC")
    rule_arn = EB.put_rule(Name=SCHEDULE_NAME, ScheduleExpression=SCHEDULE_EXPR, State="ENABLED")["RuleArn"]
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}"
    EB.put_targets(Rule=SCHEDULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        L.add_permission(FunctionName=LAMBDA_NAME, StatementId=f"{SCHEDULE_NAME}-eb",
                          Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                          SourceArn=rule_arn)
        log(f"  ✓ permission added")
    except L.exceptions.ResourceConflictException:
        log("  permission already exists")

    section("4) Smoke invoke")
    t0 = time.time()
    r = L.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                  LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log(f"  status: {r['StatusCode']}  duration: {dur:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {json.dumps(body)[:400]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail ──")
        for ln in tail.splitlines()[-15:]:
            log(f"    {ln.rstrip()}")

    section("5) S3 output")
    obj = S3.get_object(Bucket=BUCKET, Key="data/eps-revision-velocity.json")
    data = json.loads(obj["Body"].read())
    log(f"  generated_at: {data.get('generated_at')}")
    log(f"  stats: {json.dumps(data.get('stats', {}))}")
    log("")
    log("  ── Top 15 EPS-velocity setups ──")
    log(f"  {'#':>2} {'Sym':<6} {'Score':>6} {'Flag':<22} {'Lift%':>6} {'RevG%':>6} {'Up%':>5} {'NEst':>4} {'Sector':<22}")
    for i, c in enumerate(data.get("summary", {}).get("top_25_overall", [])[:15], 1):
        log(f"  {i:>2} {c['symbol']:<6} {c['score']:>6.1f} {c['flag']:<22} "
            f"{c['fy2_lift_pct']:>+6.1f} {c['fwd_rev_growth_pct']:>+6.1f} "
            f"{c['upgrade_pct']*100:>4.0f}% {c['n_estimates']:>4} "
            f"{c.get('sector', '')[:22]:<22}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "ship_eps_revision_velocity.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
