"""Phase X6e — force-redeploy activist scanner v2 (daily-index edition) + smoke test."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
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
    log("  source: " + str(len(src)) + " chars (v2)")

    markers = [
        "v2.0 — daily-index edition",
        "fetch_master_idx",
        "fetch_subject_from_filing",
        "SUBJECT[\\s\\-]*COMPANY",
        "cascade investment",
        "RESOLVE_SUBJECTS",
    ]
    for m in markers:
        log("    " + ("✓" if m in src else "❌") + " " + m[:60])

    # Wait for any in-flight updates
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)

    section("1) Force-deploy v2 + bump memory/timeout (subject resolution is HTTP-heavy)")
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

    L.update_function_configuration(
        FunctionName=LAMBDA_NAME,
        MemorySize=1024,
        Timeout=600,
        Environment={"Variables": {
            "S3_BUCKET": BUCKET,
            "SEC_USER_AGENT": "JustHodl-AI raafouis@gmail.com",
            "TIMEOUT_BUDGET_S": "550",
            "DAYS_BACK": "5",
            "N_WORKERS": "10",
            "RESOLVE_SUBJECTS": "1",
        }},
    )
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ deployed at " + str(c["LastModified"]) + " mem=" + str(c["MemorySize"]) + "MB to=" + str(c["Timeout"]) + "s")

    section("2) Smoke invoke (parses 5 days of master.idx + resolves subjects)")
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

    section("3) Inspect output")
    obj = S3.get_object(Bucket=BUCKET, Key="data/activist-filings.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TOP 15 FILINGS BY SCORE ──")
    for f in d.get("summary", {}).get("top_25_filings", [])[:15]:
        tier = (f.get("filer_tier") or "")[:18]
        ftype = (f.get("form_type") or "")[:18]
        tk = (f.get("subject_ticker") or "?")[:6]
        filer = (f.get("filer_name") or "?")[:30]
        in_uni = "✓" if f.get("in_universe") else " "
        log("    {} {:<6}  {:<18}  {:<30}  tier={:<18}  score={:>3}  level={}".format(
            in_uni, tk, ftype, filer, tier, f.get("score", 0), f.get("level", "")[:14]))

    log("")
    log("  ── TOP 10 FILINGS WITH IN-UNIVERSE TICKER ──")
    for f in d.get("summary", {}).get("top_25_in_universe", [])[:10]:
        log("    {:<6}  {:<18}  {:<30}  tier={:<18}  score={:>3}".format(
            f.get("subject_ticker") or "?",
            (f.get("form_type") or "")[:18],
            (f.get("filer_name") or "?")[:30],
            (f.get("filer_tier") or "")[:18],
            f.get("score", 0)))

    ma = d.get("summary", {}).get("multi_activist_setups", [])
    if ma:
        log("")
        log("  ── MULTI-ACTIVIST SETUPS ──")
        for m in ma[:10]:
            in_uni = "✓" if m.get("in_universe") else " "
            log("    {} {:<6} n={} filers={}".format(
                in_uni, m["ticker"], m["n_filings"],
                ",".join(m["filers"][:3])[:60]))


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
    with open(os.path.join(out, "phase_x6e_activist_v2.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
