"""Phase X6l — Redeploy activist v3 with retry logic + bump env DAYS_BACK + retest."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-activist-filings-scanner"

L = boto3.client("lambda", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=900, connect_timeout=10))
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

    section("1) Verify retry markers")
    markers = ["max_retries=3", "500 retry", "max_retries - 1"]
    for m in markers:
        log("    " + ("✓" if m in src else "❌") + " " + m)

    # Wait for any in-flight
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)

    section("2) Force-deploy + ensure env DAYS_BACK=30")
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

    # Update env DAYS_BACK to 30
    cur_env = (c.get("Environment") or {}).get("Variables", {}) or {}
    cur_env["DAYS_BACK"] = "30"
    cur_env["TIMEOUT_BUDGET_S"] = "550"
    cur_env["S3_BUCKET"] = "justhodl-dashboard-live"
    cur_env["SEC_USER_AGENT"] = "JustHodl-AI raafouis@gmail.com"
    L.update_function_configuration(
        FunctionName=LAMBDA_NAME,
        Environment={"Variables": cur_env},
        MemorySize=1024, Timeout=600,
    )
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed at " + str(c["LastModified"]))
    log("  env DAYS_BACK=" + cur_env.get("DAYS_BACK", "?"))

    section("3) Smoke invoke (with retries should now succeed)")
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
    log("  ── TOP 15 BY SCORE ──")
    for f in d.get("summary", {}).get("top_25_overall", [])[:15]:
        ucm = " 🎯" if f.get("in_universe") else ""
        log("    {:<6}{}  {:<10}  score={:<3}  {:<22}  {}".format(
            (f.get("subject_ticker") or "?")[:6], ucm,
            (f.get("form_type") or "?")[:10], f.get("score"),
            (f.get("level") or "?")[:22],
            (f.get("filer_name") or "?")[:55]))
        log("        subject: {} ({}) date: {}".format(
            (f.get("subject_company") or "?")[:50],
            f.get("subject_ticker") or "?",
            f.get("filing_date")))

    log("")
    log("  ── TIER-A activist filings ──")
    tier_a = d.get("summary", {}).get("tier_a_classified", [])
    if not tier_a:
        log("    (none today)")
    for f in tier_a[:8]:
        log("    {:<6}  {:<10}  score={}  {}: {}".format(
            f.get("subject_ticker") or "?",
            (f.get("form_type") or "?")[:10],
            f.get("score"),
            f.get("filer_tier"),
            (f.get("filer_name") or "?")[:55]))

    log("")
    log("  ── IN-UNIVERSE filings (most actionable) ──")
    in_univ = d.get("summary", {}).get("in_universe_filings", [])
    if not in_univ:
        log("    (none today)")
    for f in in_univ[:8]:
        log("    {:<6}  {:<10}  score={}  {}".format(
            f.get("subject_ticker") or "?",
            (f.get("form_type") or "?")[:10],
            f.get("score"),
            (f.get("filer_name") or "?")[:55]))

    log("")
    log("  ── Multi-activist setups ──")
    ma_list = d.get("summary", {}).get("multi_activist_setups", [])
    if not ma_list:
        log("    (none today)")
    for ma in ma_list[:8]:
        log("    {:<6}  n={}  max_score={}  tiers={}".format(
            ma.get("ticker"), ma.get("n_filings"), ma.get("max_score"),
            ma.get("tiers")))


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
    with open(os.path.join(out, "phase_x6l_activist_v3_retries.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
