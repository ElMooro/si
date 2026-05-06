"""Phase X6 — Deploy justhodl-activist-filings-scanner Lambda + smoke test.

Schedule: cron(0 12 * * ? *) — 12 UTC daily. SEC posts 13D/G filings throughout
business hours but daily-index closes around 6pm ET, so 12 UTC catches yesterday's
late filings + today's morning batch.
"""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-activist-filings-scanner"
SCHEDULE_NAME = "justhodl-activist-filings-daily"
SCHEDULE_EXPR = "cron(0 12 * * ? *)"
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
    src = open("aws/lambdas/justhodl-activist-filings-scanner/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

    # Verify v1 markers
    markers = [
        "ACTIVIST_TIERS",
        "TIER_S_LEGENDARY",
        "icahn",
        "berkshire hathaway",
        "starboard",
        "import urllib.parse",
        "fetch_atom_feed",
        "cik_to_ticker_map",
    ]
    for m in markers:
        log("    " + ("✓" if m in src else "❌") + " " + m[:50])

    section("1) Build zip + create Lambda")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    log("  zip: " + str(len(zb)) + "b")

    env = {
        "S3_BUCKET": BUCKET,
        "SEC_USER_AGENT": "JustHodl-AI raafouis@gmail.com",
        "TIMEOUT_BUDGET_S": "240",
        "DAYS_BACK": "30",
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
            MemorySize=512, Timeout=300,
            Environment={"Variables": env},
        )
    else:
        L.create_function(
            FunctionName=LAMBDA_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN, Code={"ZipFile": zb},
            Timeout=300, MemorySize=512,
            Environment={"Variables": env},
        )

    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed at " + str(c["LastModified"]) + ", mem=" + str(c["MemorySize"]) + "MB to=" + str(c["Timeout"]) + "s")

    section("2) Schedule daily 12:00 UTC")
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

    section("4) Inspect output")
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/activist-filings.json")
        d = json.loads(obj["Body"].read())
        log("  generated_at: " + str(d.get("generated_at")))
        log("  stats: " + json.dumps(d.get("stats", {})))
        log("")
        log("  ── TOP 15 RECENT FILINGS ──")
        for f in d.get("summary", {}).get("top_25_filings", [])[:15]:
            tier = (f.get("filer_tier") or "")[:18]
            ftype = (f.get("form_type") or "")[:8]
            tk = f.get("subject_ticker") or "?"
            filer = (f.get("filer_name") or "?")[:32]
            log("    {:<5}  {:<10}  {:<32}  filer_tier={:<18}  score={:>3}".format(
                tk, ftype, filer, tier, f.get("score", 0)))

        ma = d.get("summary", {}).get("multi_activist_setups", [])
        if ma:
            log("")
            log("  ── MULTI-ACTIVIST SETUPS (≥2 filers same ticker) ──")
            for m in ma[:10]:
                log("    {:<5} n_filings={}  filers={}  forms={}".format(
                    m["ticker"], m["n_filings"],
                    ",".join(m["filers"][:3])[:50],
                    ",".join(m["form_types"])[:30]))
    except Exception as e:
        log("  ❌ output read: " + str(e))


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
    with open(os.path.join(out, "phase_x6_activist_filings.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
