"""Phase X4c — Redeploy narrative-density v2 (Polygon news edition) + smoke test."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-narrative-density-tracker"

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
    src = open("aws/lambdas/justhodl-narrative-density-tracker/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars (v2)")

    # Wait for any in-flight updates
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)

    section("1) Force-deploy v2")
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
    log("  ✓ deployed")

    section("2) Smoke invoke")
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(dur) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + json.dumps(body)[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-10:]:
            log("    " + ln.rstrip())

    section("3) Inspect output")
    obj = S3.get_object(Bucket=BUCKET, Key="data/narrative-density.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TOP 15 NARRATIVE THEMES BY DENSITY ──")
    for t in d.get("summary", {}).get("top_15_themes", []):
        log("    {:<32} score={:>5.1f} {:<18}  today={:<3}  7d={:<4}  30d={:<5}  accel_t/7={:>4.2f}x  accel_7/30={:>4.2f}x".format(
            t["name"][:32], t["score"], t["tier"][:18],
            t["metrics"]["n_today"], t["metrics"]["n_7d"], t["metrics"]["n_30d"],
            t["metrics"]["accel_today_vs_7d"], t["metrics"]["accel_7d_vs_30d"]))
        if t.get("flags"):
            log("      flags: " + ",".join(t["flags"]))
        if t.get("top_co_mentioned_tickers"):
            top_tk = ",".join(tc["ticker"] + "(" + str(tc["n"]) + ")" for tc in t["top_co_mentioned_tickers"][:6])
            log("      top tickers: " + top_tk)
        if t.get("sample_titles"):
            for st in t["sample_titles"][:1]:
                log("      sample: " + st["title"][:140])


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
    with open(os.path.join(out, "phase_x4c_narrative_v2.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
