"""Phase Y2 — Deploy revenue-acceleration Lambda + smoke test."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-revenue-acceleration"
SCHEDULE_NAME = "justhodl-revenue-acceleration-daily"
SCHEDULE_EXPR = "cron(45 9 * * ? *)"  # 9:45 UTC, after EPS-velocity
ROLE_ARN = "arn:aws:iam::" + ACCOUNT + ":role/lambda-execution-role"

L = boto3.client("lambda", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=600, connect_timeout=10))
EB = boto3.client("events", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def main():
    src = open("aws/lambdas/justhodl-revenue-acceleration/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

    section("1) Deploy")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()

    env = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
        "MAX_TICKERS": "600",
        "TIMEOUT_BUDGET_S": "550",
        "N_WORKERS": "12",
    }
    exists = False
    try:
        L.get_function(FunctionName=LAMBDA_NAME)
        exists = True
    except L.exceptions.ResourceNotFoundException:
        pass

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
            MemorySize=1024, Timeout=600,
            Environment={"Variables": env},
        )
    else:
        L.create_function(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN, Code={"ZipFile": zb},
            Timeout=600, MemorySize=1024,
            Environment={"Variables": env},
        )
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed")

    section("2) Schedule daily 9:45 UTC")
    rule_arn = EB.put_rule(Name=SCHEDULE_NAME, ScheduleExpression=SCHEDULE_EXPR, State="ENABLED")["RuleArn"]
    fn_arn = "arn:aws:lambda:" + REGION + ":" + ACCOUNT + ":function:" + LAMBDA_NAME
    EB.put_targets(Rule=SCHEDULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        L.add_permission(FunctionName=LAMBDA_NAME, StatementId=SCHEDULE_NAME + "-eb",
                          Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                          SourceArn=rule_arn)
        log("  ✓ permission added")
    except L.exceptions.ResourceConflictException:
        log("  ✓ permission exists")

    section("3) Smoke invoke (heavy — fetches quarterly income for 600 stocks)")
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(dur) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + json.dumps(body)[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-12:]:
            log("    " + ln.rstrip())

    section("4) Inspect output")
    obj = S3.get_object(Bucket=BUCKET, Key="data/revenue-acceleration.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TIER_S INFLECTION (4Q+ accelerating, score >= 80) ──")
    tier_s = d.get("summary", {}).get("tier_s", [])
    if not tier_s:
        log("    (none today — these are rare)")
    else:
        for sym in tier_s:
            log("    " + sym)
    log("")
    log("  ── TOP 15 OVERALL ──")
    for c in d.get("summary", {}).get("top_25_overall", [])[:15]:
        accel_streak = c.get("consec_accel", 0)
        log("    {:<6} score={:>5.1f}  {:<24}  growth={:+.0f}%  Δ={:+.1f}pp  streak={}Q  GM_Δ={:+.1f}pp".format(
            c["symbol"], c["score"], c["tier"][:24],
            c["growth"] or 0, c["acceleration"] or 0, accel_streak,
            c.get("gm_trend") or 0))
        log("      flags: " + ",".join(c.get("flags") or []))
    log("")
    log("  ── MICROCAP PICKS (mcap < $500M, growth > 30%, accelerating) ──")
    for p in d.get("summary", {}).get("microcap_picks", [])[:10]:
        mcap_str = "${:.0f}M".format((p.get("market_cap") or 0) / 1_000_000) if p.get("market_cap") else "?"
        rev_str = "${:.0f}M".format((p.get("annualized_rev") or 0) / 1_000_000) if p.get("annualized_rev") else "?"
        log("    {:<6} score={:>5.1f}  growth={:+.0f}%  Δ={:+.1f}pp  streak={}Q  rev_ann={}  mcap={}".format(
            p["symbol"], p["score"], p["growth"], p["acceleration"],
            p["consec_accel"], rev_str, mcap_str))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
        for ln in traceback.format_exc().splitlines():
            log("    " + ln)
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_y2_revenue_acceleration.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
