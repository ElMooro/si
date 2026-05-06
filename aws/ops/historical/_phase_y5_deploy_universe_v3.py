"""Phase Y5 — Deploy universe-builder v3 (multi-cap) + invoke + verify."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-universe-builder"

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
    src = open("aws/lambdas/justhodl-universe-builder/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

    section("1) Wait for in-flight + force-deploy v3")
    for _ in range(45):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    
    deployed = False
    for attempt in range(5):
        try:
            L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=buf.getvalue())
            deployed = True
            log("  ✓ deploy accepted (attempt " + str(attempt+1) + ")")
            break
        except L.exceptions.ResourceConflictException:
            time.sleep(15)
    if not deployed:
        log("  ❌ couldn't deploy")
        return
    
    for _ in range(45):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    
    # Bump memory + timeout to handle 2400+ stocks
    cur_env = (c.get("Environment") or {}).get("Variables", {}) or {}
    cur_env["S3_BUCKET"] = "justhodl-dashboard-live"
    cur_env["FMP_KEY"] = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
    L.update_function_configuration(
        FunctionName=LAMBDA_NAME, MemorySize=1024, Timeout=300,
        Environment={"Variables": cur_env},
    )
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    log("  ✓ deployed at " + str(c["LastModified"]))

    section("2) Invoke (~30-60s — fetches 6 cap buckets in parallel)")
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

    section("3) Inspect new universe")
    obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
    d = json.loads(obj["Body"].read())
    log("  generated_at: " + str(d.get("generated_at")))
    stats = d.get("stats", {})
    log("  total_stocks: " + str(stats.get("total_stocks")))
    log("")
    log("  ── BY CAP BUCKET ──")
    bucket_order = ["mega", "large", "mid", "small", "micro", "nano", "unknown"]
    bb = stats.get("by_cap_bucket", {})
    for b in bucket_order:
        if b in bb:
            log("    {:<8}  {:>5} stocks".format(b, bb[b]))
    log("")
    log("  ── BY SECTOR (top 10) ──")
    for sector, n in stats.get("by_sector_top_10", {}).items():
        log("    {:<35}  {:>5}".format(sector[:35], n))
    log("")
    log("  ── SAMPLE STOCKS PER BUCKET ──")
    stocks = d.get("stocks", [])
    by_bucket_samples = {}
    for s in stocks:
        b = s.get("cap_bucket")
        if b not in by_bucket_samples:
            by_bucket_samples[b] = []
        if len(by_bucket_samples[b]) < 4:
            by_bucket_samples[b].append(s)
    for b in bucket_order:
        if b in by_bucket_samples:
            log("    " + b + ":")
            for s in by_bucket_samples[b]:
                mc = s.get("market_cap") or 0
                mc_str = ("${:.0f}M".format(mc / 1e6) if mc < 1e9
                           else "${:.1f}B".format(mc / 1e9))
                log("      {:<6} {:<8} {:<35} | {}".format(
                    s.get("symbol") or "?", mc_str,
                    (s.get("name") or "")[:35],
                    (s.get("industry") or "")[:25]))
    log("")
    log("  ── CURATED SEED CHECK ──")
    pump_list_check = ["AAOI", "LITE", "COHR", "AXTI", "INFN", "MU", "SNDK",
                        "CRDO", "ICHR", "FCEL", "AGIO", "BCRX"]
    sym_set = {s["symbol"] for s in stocks}
    for sym in pump_list_check:
        log("    " + sym + ": " + ("✓ in universe" if sym in sym_set else "❌ MISSING"))


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
    with open(os.path.join(out, "phase_y5_universe_v3.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
