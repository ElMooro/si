"""
Ship justhodl-insider-cluster-scanner.
1. Create or update Lambda
2. Schedule daily 14:30 UTC (9:30 ET, after market open, after overnight Form 4 filings)
3. Smoke-invoke and parse top clusters
4. Verify S3 output
"""
import io, json, os, time, zipfile
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-insider-cluster-scanner"
SCHEDULE_NAME = "justhodl-insider-cluster-scanner-daily"
SCHEDULE_EXPR = "cron(30 14 * * ? *)"  # daily 14:30 UTC = 9:30 ET / 10:30 EDT
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
SOURCE_FILE = "aws/lambdas/justhodl-insider-cluster-scanner/source/lambda_function.py"

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
    
    lam = boto3.client("lambda", region_name=REGION)
    
    section("2) Create or update Lambda")
    try:
        lam.get_function(FunctionName=LAMBDA_NAME)
        exists = True
        log(f"  Lambda exists — updating code")
    except lam.exceptions.ResourceNotFoundException:
        exists = False
        log(f"  Lambda does not exist — creating")
    
    env = {"Variables": {
        "S3_BUCKET": BUCKET,
        "S3_KEY": "data/insider-clusters.json",
        "SEC_USER_AGENT": "JustHodl Research raafouis@gmail.com",
        "LOOKBACK_DAYS": "30",
        "MIN_BUY_VALUE_USD": "10000",
        "CLUSTER_MIN_INSIDERS": "2",
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
    }}
    
    if exists:
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
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
            Environment=env,
        )
    else:
        lam.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zb},
            MemorySize=1024,
            Timeout=600,
            Environment=env,
        )
    
    for _ in range(30):
        cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
        if cfg.get("LastUpdateStatus") == "Successful" and cfg.get("State") == "Active":
            break
        time.sleep(1)
    log(f"  ✓ deployed: state={cfg.get('State')}  mod={cfg.get('LastModified')}")
    
    section("3) Schedule via EventBridge")
    eb = boto3.client("events", region_name=REGION)
    try:
        eb.put_rule(
            Name=SCHEDULE_NAME,
            ScheduleExpression=SCHEDULE_EXPR,
            State="ENABLED",
            Description=f"Daily insider cluster scan for {LAMBDA_NAME}"
        )
        log(f"  ✓ rule {SCHEDULE_NAME} = {SCHEDULE_EXPR}")
        
        target_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}"
        eb.put_targets(
            Rule=SCHEDULE_NAME,
            Targets=[{"Id": "1", "Arn": target_arn}]
        )
        try:
            lam.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId=f"{SCHEDULE_NAME}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{SCHEDULE_NAME}",
            )
            log(f"  ✓ permission granted to events.amazonaws.com")
        except lam.exceptions.ResourceConflictException:
            log(f"  ✓ permission already exists")
    except Exception as e:
        log(f"  ⚠ schedule setup: {e}")
    
    section("4) Smoke invoke (this will hit SEC EDGAR for ~10 days of indices)")
    log(f"  invoking {LAMBDA_NAME}...")
    t0 = time.time()
    r = lam.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=b"{}"
    )
    dt = time.time() - t0
    body = json.loads(r["Payload"].read())
    log(f"  status: {r['StatusCode']}  duration: {dt:.1f}s")
    
    if body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  inner: {json.dumps(inner)}")
    else:
        log(f"  raw body: {json.dumps(body)[:1500]}")
    
    if "LogResult" in r:
        import base64
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail logs (last 4kb) ──")
        for ln in tail.splitlines()[-25:]:
            log(f"    {ln.rstrip()}")
    
    section("5) Verify S3 output")
    s3 = boto3.client("s3", region_name=REGION)
    try:
        head = s3.head_object(Bucket=BUCKET, Key="data/insider-clusters.json")
        log(f"  S3 size: {head['ContentLength']:,}b  modified: {head['LastModified']}")
        obj = s3.get_object(Bucket=BUCKET, Key="data/insider-clusters.json")
        data = json.loads(obj["Body"].read())
        log(f"  schema_version: {data.get('schema_version')}")
        log(f"  method: {data.get('method')}")
        stats = data.get("stats", {})
        log(f"  stats: {json.dumps(stats)}")
        
        clusters = data.get("clusters", [])
        log(f"  total clusters: {len(clusters)}")
        if clusters:
            log("")
            log("  ── Top 12 by score ──")
            for c in clusters[:12]:
                fund = c.get("fundamentals", {})
                ticker = c.get("ticker", "?")
                score = c.get("score", 0)
                sig = c.get("signal_type", "")
                n_ins = c.get("n_insiders", 0)
                val = c.get("total_value", 0)
                pct_high = fund.get("pct_from_52w_high", 0)
                mcap = fund.get("market_cap", 0)
                mcap_str = f"${mcap/1e9:.1f}B" if mcap >= 1e9 else f"${mcap/1e6:.0f}M" if mcap else "?"
                log(f"    {ticker:<8} {score:>5.1f} {sig:<22} {n_ins}ins  ${val/1e6:>5.2f}M  high:{pct_high:>+6.1f}%  mcap:{mcap_str}")
            log("")
            log("  ── Sample top cluster — full detail ──")
            c = clusters[0]
            log(f"    ticker: {c.get('ticker')}")
            log(f"    company: {c.get('company')}")
            log(f"    score: {c.get('score')}  signal_type: {c.get('signal_type')}")
            log(f"    rationale: {c.get('rationale')}")
            log(f"    n_insiders: {c.get('n_insiders')}  n_transactions: {c.get('n_transactions')}")
            log(f"    total_value: ${c.get('total_value', 0):,.0f}  avg_price: ${c.get('avg_price', 0):.2f}")
            log(f"    window: {c.get('first_buy')} → {c.get('last_buy')}")
            log(f"    has_ceo: {c.get('has_ceo')}  has_cfo: {c.get('has_cfo')}  has_chairman: {c.get('has_chairman')}")
            log(f"    insiders ({len(c.get('insiders',[]))}):")
            for i in c.get("insiders", [])[:6]:
                log(f"      - {i.get('name'):<30} {i.get('role')[:30]:<30} ${i.get('total_value',0):>10,.0f}  {i.get('n_buys')}-buys")
            f = c.get("fundamentals", {})
            log(f"    fundamentals:")
            for k in ["market_cap", "price_now", "high_52w", "low_52w", "pct_from_52w_high", "sector", "industry"]:
                if k in f:
                    log(f"      {k}: {f[k]}")
    except Exception as e:
        log(f"  ❌ S3 read: {e}")
        import traceback
        log(traceback.format_exc()[:1500])


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "ship_insider_cluster_scanner.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
