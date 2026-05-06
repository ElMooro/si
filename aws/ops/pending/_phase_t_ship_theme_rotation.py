"""
PHASE T — Deploy theme-rotation-engine Lambda + smoke test it.
This is the institutional money-flow tracker.
"""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-theme-rotation-engine"
SCHEDULE_NAME = "justhodl-theme-rotation-engine-daily"
SCHEDULE_EXPR = "cron(45 13 * * ? *)"  # 13:45 UTC daily — after L4 hunter
ROLE_ARN = "arn:aws:iam::" + ACCOUNT + ":role/lambda-execution-role"

L = boto3.client("lambda", region_name=REGION)
EB = boto3.client("events", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=600, connect_timeout=10))
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def main():
    src = open("aws/lambdas/justhodl-theme-rotation-engine/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

    section("1) Build zip + deploy")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    log("  zip: " + str(len(zb)) + "b")

    env = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
        "TIMEOUT_BUDGET_S": "260",
        "N_WORKERS": "12",
    }
    exists = False
    try:
        L.get_function(FunctionName=LAMBDA_NAME)
        exists = True
        log("  exists — updating")
    except L.exceptions.ResourceNotFoundException:
        log("  creating")

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
    log("  ✓ deployed at " + str(c["LastModified"]))

    section("2) Schedule daily 13:45 UTC")
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

    section("3) Smoke invoke (~120-180s for 105 ETFs + breadth)")
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(dur) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + json.dumps(body)[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-15:]:
            log("    " + ln.rstrip())

    section("4) Inspect output — top themes by momentum")
    obj = S3.get_object(Bucket=BUCKET, Key="data/theme-rotation.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    log("  spy_ret_20d: " + str(d.get("spy_ret_20d")) + "%")
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TOP 10 THEMES BY MOMENTUM ──")
    for t in d.get("summary", {}).get("top_10_momentum", []):
        breadth = t.get("breadth_pct")
        breadth_str = "{:>5.1f}%".format(breadth) if breadth is not None else "  N/A"
        log("    {:<6} {:>5.1f}  {:<22}  RS_5d={:>+5.1f}% RS_20d={:>+5.1f}% RS_60d={:>+5.1f}%  breadth={}".format(
            t["ticker"], t["momentum_score"], t["category"][:22],
            t["rs_5d"], t["rs_20d"], t["rs_60d"], breadth_str))

    log("")
    log("  ── BOTTOM 10 THEMES (rotation OUT) ──")
    for t in d.get("summary", {}).get("bottom_10_momentum", []):
        log("    {:<6} {:>5.1f}  {:<22}  RS_20d={:>+5.1f}% RS_60d={:>+5.1f}%".format(
            t["ticker"], t["momentum_score"], t["category"][:22],
            t["rs_20d"], t["rs_60d"]))

    section("5) Category-level aggregation")
    for c in d.get("summary", {}).get("category_summary", [])[:15]:
        log("    {:<22} n={:>2}  avg_RS_20d={:>+5.1f}%  avg_momentum={:>5.1f}  top={}".format(
            c["category"][:22], c["n_themes"], c["avg_rs_20d"], c["avg_momentum"], c["top_ticker"]))

    section("6) Convergent breadth themes (institutional buy signal)")
    for c in d.get("summary", {}).get("convergent_breadth", []):
        log("    {:<6} {:<24} momentum={:>5.1f}  RS_20d={:>+5.1f}%  breadth={:>5.1f}%".format(
            c["ticker"], c["name"][:24], c["momentum_score"], c["rs_20d"], c["breadth_pct"]))

    section("7) Alerts to fire")
    for a in d.get("summary", {}).get("alerts", [])[:10]:
        log("    [" + a["type"] + "] " + a["msg"])


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
    with open(os.path.join(out, "phase_t_theme_rotation.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
