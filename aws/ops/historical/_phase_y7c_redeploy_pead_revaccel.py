"""Phase Y7c — Redeploy fixed PEAD + rev-accel + invoke."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
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


def wait_ready(name, max_s=120):
    t0 = time.time()
    while time.time() - t0 < max_s:
        try:
            c = L.get_function_configuration(FunctionName=name)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
                return
        except Exception:
            pass
        time.sleep(2)


def deploy(name, src_path):
    src = open(src_path).read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    wait_ready(name)
    for attempt in range(5):
        try:
            L.update_function_code(FunctionName=name, ZipFile=buf.getvalue())
            break
        except L.exceptions.ResourceConflictException:
            time.sleep(15)
    wait_ready(name)


def main():
    section("1) Redeploy + invoke PEAD")
    deploy("justhodl-earnings-pead",
            "aws/lambdas/justhodl-earnings-pead/source/lambda_function.py")
    log("  ✓ deployed")
    
    log("  invoking PEAD...")
    t0 = time.time()
    r = L2.invoke(FunctionName="justhodl-earnings-pead", InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(time.time()-t0) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + str(body.get("body"))[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-10:]:
            log("    " + ln.rstrip())

    section("2) Redeploy + invoke rev-accel (workers=6)")
    deploy("justhodl-revenue-acceleration",
            "aws/lambdas/justhodl-revenue-acceleration/source/lambda_function.py")
    
    # Reduce workers to avoid rate limits
    cur = L.get_function_configuration(FunctionName="justhodl-revenue-acceleration")
    cur_env = (cur.get("Environment") or {}).get("Variables", {}) or {}
    cur_env["N_WORKERS"] = "6"
    cur_env["MAX_TICKERS"] = "1500"
    L.update_function_configuration(
        FunctionName="justhodl-revenue-acceleration",
        Environment={"Variables": cur_env},
    )
    wait_ready("justhodl-revenue-acceleration")
    log("  ✓ deployed with N_WORKERS=6")
    
    log("  invoking rev-accel...")
    t0 = time.time()
    r = L2.invoke(FunctionName="justhodl-revenue-acceleration",
                   InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(time.time()-t0) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + str(body.get("body"))[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-10:]:
            log("    " + ln.rstrip())

    section("3) Inspect outputs")
    for key, label in [("data/earnings-pead.json", "PEAD"),
                          ("data/revenue-acceleration.json", "Rev-Accel")]:
        try:
            obj = S3.get_object(Bucket=BUCKET, Key=key)
            d = json.loads(obj["Body"].read())
            log("")
            log("  ── " + label + " ──")
            log("  stats: " + json.dumps(d.get("stats", {}))[:300])
            top = d.get("summary", {}).get("top_25_overall", [])[:12]
            for t in top:
                if "beat_streak" in t:
                    log("    {:<6} score={:>5.1f}  streak={}Q  surprise={:+.1f}%  cap={:<6}  drift={}".format(
                        t["symbol"], t["score"], t.get("beat_streak", 0),
                        t.get("latest_surprise_pct", 0),
                        t.get("cap_bucket", "?"),
                        "✓" if t.get("drift_active") else "—"))
                else:
                    log("    {:<6} score={:>5.1f}  growth={:+.0f}%  Δ={:+.1f}pp  streak={}Q".format(
                        t["symbol"], t["score"], t.get("growth", 0) or 0,
                        t.get("acceleration", 0) or 0,
                        t.get("consec_accel", 0)))
        except Exception as e:
            log("  ❌ " + label + ": " + str(e))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_y7c_pead_revaccel_fixed.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
