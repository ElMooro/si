"""Phase Y7c — Redeploy PEAD with /stable/earnings + smoke test."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-pead-detector"

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
    src = open("aws/lambdas/justhodl-pead-detector/source/lambda_function.py").read()

    # Wait for in-flight from auto-deploy
    for _ in range(45):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)

    section("1) Force-deploy")
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
    if not deployed:
        log("  ❌ couldn't deploy")
        return
    
    for _ in range(45):
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
        for ln in tail.splitlines()[-12:]:
            log("    " + ln.rstrip())

    section("3) Inspect output")
    obj = S3.get_object(Bucket=BUCKET, Key="data/pead-signals.json")
    d = json.loads(obj["Body"].read())
    log("  stats: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TIER_S DRIFTING (4Q+ streak, big beats, recent earnings) ──")
    for sym in d.get("summary", {}).get("tier_s", [])[:15]:
        log("    " + sym)
    log("")
    log("  ── TOP 20 OVERALL ──")
    for c in d.get("summary", {}).get("top_30_overall", [])[:20]:
        days_since = c.get("days_since_earnings")
        days_since_str = "{}d".format(days_since) if days_since is not None else "?"
        drift_str = "{:+.1f}%".format(c["drift_pct"]) if c.get("drift_pct") is not None else "?"
        log("    {:<6} score={:>5.1f}  {:<18}  {:<6}  streak={}Q  avg_beat={:+.1f}%  Δ_beats={:+.1f}pp  drift={}  earned={}_ago".format(
            c["symbol"], c["score"], c["tier"][:18], c["cap_bucket"][:6],
            c["streak"], c["avg_beat_pct"], c["beat_accel"], drift_str, days_since_str))
        log("      flags: " + ",".join(c.get("flags") or [])[:120])

    log("")
    log("  ── BEST MICROCAP/NANO PEAD ──")
    for c in d.get("summary", {}).get("best_microcap", [])[:10]:
        log("    {:<6} score={:>5.1f}  streak={}Q  avg_beat={:+.1f}%".format(
            c["symbol"], c["score"], c["streak"], c["avg_beat"]))
    log("")
    log("  ── BEST SMALLCAP PEAD ──")
    for c in d.get("summary", {}).get("best_smallcap", [])[:10]:
        log("    {:<6} score={:>5.1f}  streak={}Q  avg_beat={:+.1f}%".format(
            c["symbol"], c["score"], c["streak"], c["avg_beat"]))
    log("")
    log("  ── PRE-EARNINGS SETUPS (2-14d, streak >= 3) ──")
    for c in d.get("summary", {}).get("pre_earnings_setups", [])[:10]:
        log("    {:<6} score={:>5.1f}  streak={}Q  next={}  ({}d)".format(
            c["symbol"], c["score"], c["streak"], c["next_earnings"], c["days_to_next"]))


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
    with open(os.path.join(out, "phase_y7c_pead_redeploy.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
