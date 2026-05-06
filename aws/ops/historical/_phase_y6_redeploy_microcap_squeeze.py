"""Phase Y6 — Redeploy microcap-float-squeeze with multi-cap universe + derived shares."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-microcap-float-squeeze"

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
    src = open("aws/lambdas/justhodl-microcap-float-squeeze/source/lambda_function.py").read()
    log("  source: " + str(len(src)) + " chars")

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
    
    deployed = False
    for attempt in range(5):
        try:
            L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=buf.getvalue())
            deployed = True
            break
        except L.exceptions.ResourceConflictException:
            time.sleep(15)
    
    for _ in range(45):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    log("  ✓ deployed at " + str(c["LastModified"]))

    section("2) Smoke invoke (1300+ stocks for nano/micro/small/mid)")
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
    obj = S3.get_object(Bucket=BUCKET, Key="data/microcap-float-squeeze.json")
    d = json.loads(obj["Body"].read())
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TIER_S PARABOLIC SETUPS (rare) ──")
    for sym in d.get("summary", {}).get("tier_s", []):
        log("    " + sym)
    log("")
    log("  ── TOP 15 OVERALL ──")
    for c in d.get("summary", {}).get("top_25_overall", [])[:15]:
        mc = "${:.0f}M".format(c["market_cap"] / 1_000_000)
        ft = "{:.1f}%".format(c.get("float_turnover") or 0)
        sp = "{:.0f}%".format(c["short_pct"]) if c.get("short_pct") is not None else "?"
        d2c = "{:.1f}d".format(c["days_to_cover"]) if c.get("days_to_cover") is not None else "?"
        sch = "{:+.1f}".format(c["short_change"]) if c.get("short_change") is not None else "?"
        log("    {:<6} score={:>5.1f}  mcap={}  float_turn={}  short={}  d2c={}  Δshort={}".format(
            c["symbol"], c["score"], mc, ft, sp, d2c, sch))
        log("      flags: " + ",".join(c.get("flags") or []))


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
    with open(os.path.join(out, "phase_y6_microcap_v2.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
