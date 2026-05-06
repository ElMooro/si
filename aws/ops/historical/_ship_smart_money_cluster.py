"""
Ship justhodl-smart-money-cluster Lambda:
  1. Build zip
  2. Create Lambda (or update if exists)
  3. Schedule daily 09:00 UTC (after 13F refresh)
  4. Smoke-invoke and dump top 25
"""
import io, json, os, time, zipfile
from botocore.config import Config
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
LAMBDA_NAME = "justhodl-smart-money-cluster"
SCHEDULE_NAME = "justhodl-smart-money-cluster-daily"
SCHEDULE_EXPR = "cron(0 9 * * ? *)"  # daily 09:00 UTC
SOURCE = "aws/lambdas/justhodl-smart-money-cluster/source/lambda_function.py"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"

cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 1})
L = boto3.client("lambda", region_name=REGION, config=cfg)
EB = boto3.client("events", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Build zip")
    src = open(SOURCE).read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    log(f"  zip size: {len(zb):,}b")

    section("2) Create or update Lambda")
    try:
        L.get_function(FunctionName=LAMBDA_NAME)
        log(f"  exists — updating code")
        L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
        for _ in range(30):
            c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        L.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=512,
            Timeout=300,
            Environment={"Variables": {
                "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
                "S3_BUCKET": "justhodl-dashboard-live",
                "S3_KEY": "data/smart-money-clusters.json",
            }},
        )
    except L.exceptions.ResourceNotFoundException:
        log(f"  creating new Lambda")
        L.create_function(
            FunctionName=LAMBDA_NAME,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zb},
            MemorySize=512,
            Timeout=300,
            Environment={"Variables": {
                "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
                "S3_BUCKET": "justhodl-dashboard-live",
                "S3_KEY": "data/smart-money-clusters.json",
            }},
        )

    for _ in range(30):
        cfg_resp = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if cfg_resp.get("State") == "Active" and cfg_resp.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ deployed, mod={cfg_resp['LastModified']}")

    section("3) Schedule daily 09:00 UTC")
    try:
        EB.put_rule(Name=SCHEDULE_NAME, ScheduleExpression=SCHEDULE_EXPR, State="ENABLED")
        target_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}"
        EB.put_targets(Rule=SCHEDULE_NAME, Targets=[{"Id": "1", "Arn": target_arn}])
        try:
            L.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId=f"{SCHEDULE_NAME}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{SCHEDULE_NAME}",
            )
        except L.exceptions.ResourceConflictException:
            pass
        log(f"  ✓ {SCHEDULE_NAME} scheduled {SCHEDULE_EXPR}")
    except Exception as e:
        log(f"  ⚠ schedule: {e}")

    section("4) Smoke-invoke")
    t0 = time.time()
    r = L.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=b"{}",
    )
    dur = time.time() - t0
    log(f"  status: {r['StatusCode']}  duration: {dur:.1f}s")
    body = json.loads(r["Payload"].read().decode())
    log(f"  body: {json.dumps(body)[:500]}")

    if "LogResult" in r:
        import base64
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail ──")
        for ln in tail.splitlines()[-15:]:
            log(f"    {ln.rstrip()}")

    section("5) Read S3 + dump top 25 clusters")
    import boto3
    S3 = boto3.client("s3", region_name=REGION)
    obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/smart-money-clusters.json")
    data = json.loads(obj["Body"].read())
    log(f"  generated_at: {data.get('generated_at')}")
    log(f"  stats: {json.dumps(data.get('stats', {}))}")
    clusters = data.get("clusters", [])
    log(f"  n_clusters: {len(clusters)}")
    log("")
    log(f"    {'#':>2} {'Ticker':<8} {'Score':>5} {'Flag':<22} {'#Buy':>4} {'#Sell':>5} {'#New':>4} {'%52H':>5} {'Legends':<25}")
    for i, c in enumerate(clusters[:25], 1):
        legends_s = ",".join(c.get("legend_buyers", []))[:25]
        ph = c.get("pct_from_52w_high")
        ph_s = f"{ph:+.0f}%" if ph is not None else "?"
        log(f"    {i:>2} {c['ticker']:<8} {c['score']:>5.1f} {c['flag']:<22} {c['n_buyers']:>4} {c['n_sellers']:>5} {c['n_new']:>4} {ph_s:>5} {legends_s:<25}")

    section("6) Detailed view of top 3")
    for c in clusters[:3]:
        log("")
        log(f"  ── {c['ticker']} ({(c.get('name') or '?')[:40]}) ──")
        log(f"    score: {c['score']}  flag: {c['flag']}")
        log(f"    signal types: {c['signal_types']}")
        log(f"    rationale: {c['rationale']}")
        log(f"    {c['n_buyers']} buyers / {c['n_sellers']} sellers / {c['n_new']} new init")
        log(f"    legend buyers: {c.get('legend_buyers')}  quant buyers: {c.get('quant_buyers')}")
        log(f"    pct from 52w high: {c.get('pct_from_52w_high')}")
        log(f"    fundamentals: {c.get('fundamentals')}")
        log(f"    fund actions ({len(c.get('fund_actions', []))}):")
        for a in (c.get("fund_actions") or [])[:8]:
            log(f"      {a.get('fund'):<14} {a.get('change'):<6} ${(a.get('value') or 0)/1e6:>6.1f}M  {a.get('pct_of_portfolio') or 0:>5.2f}% port  Δ {a.get('delta_pct')}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "ship_smart_money_cluster.md"), "w") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
