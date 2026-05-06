"""Phase Y9 — Redeploy revenue-acceleration with rate-limit fixes + diagnose."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-revenue-acceleration"

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
    src = open("aws/lambdas/justhodl-revenue-acceleration/source/lambda_function.py").read()

    section("1) Wait + force-deploy")
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
    
    for attempt in range(5):
        try:
            L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=buf.getvalue())
            log("  ✓ accepted (attempt " + str(attempt+1) + ")")
            break
        except L.exceptions.ResourceConflictException:
            time.sleep(15)
    
    for _ in range(45):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    
    # Update env to use new defaults
    cur_env = (c.get("Environment") or {}).get("Variables", {}) or {}
    cur_env["N_WORKERS"] = "6"
    cur_env["MAX_TICKERS"] = "1200"
    cur_env["TIMEOUT_BUDGET_S"] = "550"
    L.update_function_configuration(
        FunctionName=LAMBDA_NAME, MemorySize=1024, Timeout=600,
        Environment={"Variables": cur_env},
    )
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    log("  ✓ deployed at " + str(c["LastModified"]))

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
        for ln in tail.splitlines()[-15:]:
            log("    " + ln.rstrip())

    section("3) Inspect output")
    obj = S3.get_object(Bucket=BUCKET, Key="data/revenue-acceleration.json")
    d = json.loads(obj["Body"].read())
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TIER_S INFLECTION ──")
    for sym in d.get("summary", {}).get("tier_s", []):
        log("    " + sym)
    log("")
    log("  ── TOP 15 OVERALL ──")
    for c in d.get("summary", {}).get("top_25_overall", [])[:15]:
        log("    {:<6} score={:>5.1f}  {:<24}  growth={:+.0f}%  Δ={:+.0f}pp  streak={}Q  GM_Δ={:+.1f}pp".format(
            c["symbol"], c["score"], c["tier"][:24],
            c.get("growth") or 0, c.get("acceleration") or 0,
            c.get("consec_accel", 0), c.get("gm_trend") or 0))
        log("      flags: " + ",".join(c.get("flags") or []))
    log("")
    log("  ── MICROCAP PICKS ──")
    for p in (d.get("summary", {}).get("microcap_picks") or [])[:10]:
        mc = "${:.0f}M".format((p.get("market_cap") or 0) / 1_000_000) if p.get("market_cap") else "?"
        log("    {:<6} score={}  growth={:+.0f}%  Δ={:+.0f}pp  streak={}Q  mcap={}".format(
            p["symbol"], p["score"], p["growth"], p["acceleration"],
            p["consec_accel"], mc))


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
    with open(os.path.join(out, "phase_y9_rev_accel_fix.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
