"""Phase Y7d — Reread source, force update_function_code with explicit zip,
wait for ready, then re-invoke."""
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


def main():
    src = open("aws/lambdas/justhodl-pead-detector/source/lambda_function.py").read()
    log("source: " + str(len(src)) + " chars")
    log("contains 'stable/earnings': " + str("stable/earnings?" in src))
    log("contains 'past = [e for e in d if e.get(\"epsActual\")': " + str("epsActual" in src))

    # Wait
    for _ in range(45):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)

    # Force redeploy
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    
    deployed = False
    for attempt in range(8):
        try:
            r = L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=buf.getvalue())
            log("  ✓ deploy accepted (attempt " + str(attempt+1) + "), Code SHA: " + r.get("CodeSha256", "?")[:20])
            deployed = True
            break
        except L.exceptions.ResourceConflictException:
            log("  conflict, waiting 15s")
            time.sleep(15)
    if not deployed:
        log("  ❌ deploy failed")
        return
    
    # Wait for activation
    for _ in range(60):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    log("  ready, last modified: " + str(c["LastModified"]))

    # Now invoke
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log("  invoke status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(dur) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + json.dumps(body)[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        log("")
        log("  ── LOG TAIL ──")
        for ln in tail.splitlines()[-15:]:
            log("    " + ln.rstrip())

    # Read output
    obj = S3.get_object(Bucket=BUCKET, Key="data/pead-signals.json")
    d = json.loads(obj["Body"].read())
    log("")
    log("  STATS: " + json.dumps(d.get("stats", {})))
    log("")
    log("  ── TOP 20 OVERALL ──")
    for c in d.get("summary", {}).get("top_30_overall", [])[:20]:
        days_since = c.get("days_since_earnings")
        days_since_str = "{}d".format(days_since) if days_since is not None else "?"
        drift_str = "{:+.1f}%".format(c["drift_pct"]) if c.get("drift_pct") is not None else "?"
        log("    {:<6} score={:>5.1f}  {:<18}  {:<6}  streak={}Q  avg_beat={:+.1f}%  drift={}  {}_ago".format(
            c["symbol"], c["score"], c["tier"][:18], c["cap_bucket"][:6],
            c["streak"], c["avg_beat_pct"], drift_str, days_since_str))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
        for ln in traceback.format_exc().splitlines():
            log("  " + ln)
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_y7d_pead_force.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
