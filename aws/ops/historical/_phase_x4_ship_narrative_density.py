"""Phase X4 — Deploy narrative-density-tracker + smoke test."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-narrative-density-tracker"
SCHEDULE_NAME = "justhodl-narrative-density-tracker-daily"
SCHEDULE_EXPR = "cron(0 11 * * ? *)"  # 11 UTC daily
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
    src = open("aws/lambdas/justhodl-narrative-density-tracker/source/lambda_function.py").read()
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
        "NEWS_KEY": "17d36cdd13c44e139853b3a6876cf940",
        "POLY_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
        "TIMEOUT_BUDGET_S": "550",
        "N_WORKERS": "6",
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
            MemorySize=512, Timeout=600,
            Environment={"Variables": env},
        )
    else:
        L.create_function(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN, Code={"ZipFile": zb},
            Timeout=600, MemorySize=512,
            Environment={"Variables": env},
        )

    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed")

    section("2) Schedule daily 11:00 UTC")
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

    section("3) Smoke invoke (~3-5 min — NewsAPI rate limited)")
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
    obj = S3.get_object(Bucket=BUCKET, Key="data/narrative-density.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TOP 15 NARRATIVE THEMES BY DENSITY ──")
    for t in d.get("summary", {}).get("top_15_themes", []):
        log("    {:<30} score={:>5.1f} {:<22}  today={:<5}  7d={:<6}  30d={:<6}  accel_t/7={:>5.2f}x  accel_7/30={:>5.2f}x".format(
            t["name"][:30], t["score"], t["tier"][:22],
            str(t["n_today"]), str(t["n_7d"]), str(t["n_30d"]),
            t["accel_today_vs_7d"], t["accel_7d_vs_30d"]))
        if t.get("flags"):
            log("      flags: " + ",".join(t["flags"]))
        if t.get("tickers"):
            log("      related tickers: " + ",".join(t["tickers"]))


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
    with open(os.path.join(out, "phase_x4_narrative_density.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
