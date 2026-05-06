"""Phase Y1 — Deploy volatility-squeeze-hunter Lambda + smoke test."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-volatility-squeeze-hunter"
SCHEDULE_NAME = "justhodl-volatility-squeeze-hunter-daily"
SCHEDULE_EXPR = "cron(30 11 * * ? *)"  # 11:30 UTC
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
    src = open("aws/lambdas/justhodl-volatility-squeeze-hunter/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

    section("1) Build zip + create Lambda")
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
    log("  ✓ deployed at " + str(c["LastModified"]))

    section("2) Schedule daily 11:30 UTC")
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

    section("3) Smoke invoke (heavy — 600 stocks × 300 day history)")
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
    obj = S3.get_object(Bucket=BUCKET, Key="data/volatility-squeeze.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TIER_S (5+ signals firing — RARE) ──")
    tier_s = d.get("summary", {}).get("tier_s", [])
    if not tier_s:
        log("    (none today — these are very rare)")
    else:
        for sym in tier_s:
            log("    " + sym)
    log("")
    log("  ── TIER_A (4 of 6 signals firing — strong setup) ──")
    log("  (top 12)")
    for c in d.get("summary", {}).get("top_25_overall", [])[:15]:
        if c.get("n_signals", 0) < 3:
            continue
        log("    {:<6} score={:>5.1f} n_sig={}  {}  base={}d  bb={:.0f}%  atr={:.0f}%  nr7={}  inside={:.0f}%".format(
            c["symbol"], c["score"], c["n_signals"], c["tier"][:18],
            c["base_days"], c["bb_pct"], c["atr_pct"],
            c["nr7"], c["inside_pct"]))
        log("      flags: " + ",".join(c.get("flags") or []))


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
    with open(os.path.join(out, "phase_y1_squeeze_hunter.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
