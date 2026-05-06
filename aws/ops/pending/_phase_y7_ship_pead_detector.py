"""Phase Y7 — Deploy PEAD detector + smoke test."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-pead-detector"
SCHEDULE_NAME = "justhodl-pead-detector-daily"
SCHEDULE_EXPR = "cron(0 8 * * ? *)"  # 8 UTC
ROLE_ARN = "arn:aws:iam::" + ACCOUNT + ":role/lambda-execution-role"

L = boto3.client("lambda", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=900, connect_timeout=10))
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
    src = open("aws/lambdas/justhodl-pead-detector/source/lambda_function.py").read()
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
        "MAX_TICKERS": "1500",
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
            MemorySize=2048, Timeout=600,
            Environment={"Variables": env},
        )
    else:
        L.create_function(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN, Code={"ZipFile": zb},
            Timeout=600, MemorySize=2048,
            Environment={"Variables": env},
        )
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed")

    section("2) Schedule daily 8 UTC")
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

    section("3) Smoke invoke (heavy — fetches earnings for 1500 stocks)")
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
    obj = S3.get_object(Bucket=BUCKET, Key="data/pead-signals.json")
    d = json.loads(obj["Body"].read())
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TIER_S DRIFTING (4Q+ streak, big beats, recent earnings) ──")
    for sym in d.get("summary", {}).get("tier_s", [])[:15]:
        log("    " + sym)
    log("")
    log("  ── TOP 15 OVERALL ──")
    for c in d.get("summary", {}).get("top_30_overall", [])[:15]:
        days_since = c.get("days_since_earnings")
        days_since_str = ("{}d ago".format(days_since) if days_since is not None else "?")
        drift_str = ("{:+.1f}%".format(c["drift_pct"]) if c.get("drift_pct") is not None else "?")
        log("    {:<6} score={:>5.1f}  {}  {:<6}  streak={}Q  avg_beat={:+.1f}%  Δ_beats={:+.1f}pp  drift={}  earned={}".format(
            c["symbol"], c["score"], c["tier"][:18], c["cap_bucket"][:6],
            c["streak"], c["avg_beat_pct"], c["beat_accel"], drift_str, days_since_str))
        log("      flags: " + ",".join(c.get("flags") or []))

    log("")
    log("  ── BEST MICROCAP/NANO PEAD ──")
    for c in d.get("summary", {}).get("best_microcap", [])[:10]:
        log("    {:<6} score={:>5.1f}  streak={}Q  avg_beat={:+.1f}%".format(
            c["symbol"], c["score"], c["streak"], c["avg_beat"]))
    log("")
    log("  ── BEST SMALLCAP PEAD ──")
    for c in d.get("summary", {}).get("best_smallcap", [])[:10]:
        log("    {:<6} score={:>5.1f}  streak={}Q  avg_beat={:+.1f}%".format(
            c["symbol"], c["score"], c["streak"], c["avg_beat"]))
    log("")
    log("  ── PRE-EARNINGS SETUPS (2-14 days out, 3+Q streak) ──")
    for c in d.get("summary", {}).get("pre_earnings_setups", [])[:10]:
        log("    {:<6} score={:>5.1f}  streak={}Q  next={}  ({}d)".format(
            c["symbol"], c["score"], c["streak"], c["next_earnings"], c["days_to_next"]))


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
    with open(os.path.join(out, "phase_y7_pead_detector.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
