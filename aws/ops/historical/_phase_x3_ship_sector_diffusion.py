"""Phase X3 — Deploy sector-earnings-diffusion Lambda + smoke test."""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-sector-earnings-diffusion"
SCHEDULE_NAME = "justhodl-sector-earnings-diffusion-daily"
SCHEDULE_EXPR = "cron(0 10 * * ? *)"  # 10 UTC daily — after EPS-velocity (9:30)
ROLE_ARN = "arn:aws:iam::" + ACCOUNT + ":role/lambda-execution-role"

L = boto3.client("lambda", region_name=REGION)
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
    src = open("aws/lambdas/justhodl-sector-earnings-diffusion/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

    section("1) Build zip + create/update Lambda")
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
        "MAX_TICKERS": "600",
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

    section("2) Schedule daily 10:00 UTC")
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

    section("3) Smoke invoke")
    t0 = time.time()
    r = L.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
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
    obj = S3.get_object(Bucket=BUCKET, Key="data/sector-earnings-diffusion.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TOP 15 SECTORS BY DIFFUSION ──")
    for s in d.get("summary", {}).get("sectors_top_diffusion", [])[:15]:
        regime = s["regime"]
        flag = "🔥" if "ALL_IN" in regime else "✓" if "BULLISH" in regime else "○"
        log("    {} {:<32} n={:<3}  up={:>5.1f}%  strong={:>5.1f}%  avg_lift={:>+5.1f}%  regime={}".format(
            flag, s["group"][:32], s["n_constituents"],
            s["diffusion_up_pct"], s["diffusion_strong_up_pct"],
            s["avg_fy2_lift_pct"], regime))
    log("")
    log("  ── TOP 15 INDUSTRIES BY DIFFUSION ──")
    for s in d.get("summary", {}).get("industries_top_diffusion", [])[:15]:
        log("    {:<40} n={:<3}  up={:>5.1f}%  strong={:>5.1f}%  avg_lift={:>+5.1f}%  regime={}".format(
            s["group"][:40], s["n_constituents"],
            s["diffusion_up_pct"], s["diffusion_strong_up_pct"],
            s["avg_fy2_lift_pct"], s["regime"]))


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
    with open(os.path.join(out, "phase_x3_sector_diffusion.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
