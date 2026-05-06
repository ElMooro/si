"""Phase X6j — Force-deploy activist scanner v3 + verify with smoke test."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-activist-filings-scanner"
SCHEDULE_NAME = "justhodl-activist-filings-scanner-daily"
SCHEDULE_EXPR = "cron(0 12 * * ? *)"  # 12 UTC daily — after market opens

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
    src = open("aws/lambdas/justhodl-activist-filings-scanner/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

    section("1) Verify v3 markers")
    markers = [
        "v3.0 starting",
        "activist_filings_v3_atom_plus_efts",
        "fetch_efts_search",
        "parse_efts_hit",
        "fetch_atom_feed",
    ]
    for m in markers:
        log("    " + ("✓" if m in src else "❌") + " " + m)

    # Wait for any in-flight update
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)

    section("2) Force-deploy")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=buf.getvalue())
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed at " + str(c["LastModified"]))

    section("3) Smoke invoke (~30-60s — fetches RSS + EFTS)")
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
    obj = S3.get_object(Bucket=BUCKET, Key="data/activist-filings.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    log("  stats: " + json.dumps(d.get("stats", {})))

    log("")
    log("  ── TOP 12 FILINGS BY SCORE ──")
    for f in d.get("summary", {}).get("top_25_overall", [])[:12]:
        ticker = f.get("subject_ticker") or "?"
        ucm = " 🎯" if f.get("in_universe") else ""
        log("    {:<6}{}  {:<8}  {:<5}  {:<22}  filer={}".format(
            ticker, ucm,
            (f.get("form_type") or "?")[:8],
            f.get("score"),
            (f.get("level") or "?")[:22],
            (f.get("filer_name") or "?")[:55]))
        log("           subject: {}  date: {}".format(
            (f.get("subject_company") or f.get("subject_name") or "?")[:55],
            f.get("filing_date")))

    log("")
    log("  ── TIER-A (HOT) classified filings ──")
    for f in d.get("summary", {}).get("tier_a_classified", []):
        log("    {:<6}  {:<8}  score={}  pattern={}  filer={}".format(
            f.get("subject_ticker") or "?",
            (f.get("form_type") or "?")[:8],
            f.get("score"),
            f.get("matched_pattern"),
            (f.get("filer_name") or "?")[:60]))

    log("")
    log("  ── IN-UNIVERSE filings (most actionable) ──")
    for f in d.get("summary", {}).get("in_universe_filings", [])[:10]:
        log("    {:<6}  {:<8}  score={}  tier={}  filer={}".format(
            f.get("subject_ticker") or "?",
            (f.get("form_type") or "?")[:8],
            f.get("score"),
            f.get("filer_tier") or "?",
            (f.get("filer_name") or "?")[:50]))

    log("")
    log("  ── MULTI-ACTIVIST tickers ──")
    for ma in d.get("summary", {}).get("multi_activist_setups", [])[:10]:
        log("    {:<6}  n={}  max_score={}  tiers={}".format(
            ma.get("ticker"), ma.get("n_filings"), ma.get("max_score"),
            ma.get("tiers")))

    section("5) Schedule daily 12 UTC")
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
    with open(os.path.join(out, "phase_x6j_activist_v3.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
