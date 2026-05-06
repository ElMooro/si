"""Phase X6o — Final activist v3 deploy: link regex fix + drop EFTS + count=100."""
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
    log("  source: " + str(len(src)) + " chars")

    section("1) Verify markers")
    markers = [
        "count=100",
        'EFTS full-text search abandoned',
        '<link>([^<]+)</link>',
    ]
    for m in markers:
        log("    " + ("✓" if m in src else "❌") + " " + m[:60])

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

    section("3) Smoke invoke")
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(time.time()-t0) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + json.dumps(body)[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-12:]:
            log("    " + ln.rstrip())

    section("4) Inspect output")
    obj = S3.get_object(Bucket=BUCKET, Key="data/activist-filings.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    log("  stats: " + json.dumps(d.get("stats", {})))

    log("")
    log("  ── ALL filings detected ──")
    for f in d.get("all_filings", [])[:25]:
        ucm = " 🎯" if f.get("in_universe") else ""
        link_str = "(no link)" if not f.get("filing_link") else "✓"
        log("    {:<6}{}  {:<10}  score={:<3}  {:<22}  link={}  filer={}".format(
            (f.get("subject_ticker") or "?")[:6], ucm,
            (f.get("form_type") or "?")[:10],
            f.get("score"),
            (f.get("level") or "?")[:22],
            link_str,
            (f.get("filer_name") or "?")[:50]))

    log("")
    log("  ── In universe (most actionable) ──")
    in_univ = d.get("summary", {}).get("in_universe_filings", [])
    if not in_univ:
        log("    (none today)")
    for f in in_univ[:8]:
        log("    {:<6}  {:<10}  filer: {}".format(
            f.get("subject_ticker"), (f.get("form_type") or "?")[:10],
            (f.get("filer_name") or "?")[:60]))

    log("")
    log("  Note: this Lambda runs daily 12 UTC; coverage will increase substantially")
    log("  during US business hours when most 13D/G filings happen.")


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
    with open(os.path.join(out, "phase_x6o_activist_final.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
