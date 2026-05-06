"""Phase Y7 — Deploy earnings-pead Lambda + redeploy squeeze + rev-accel
with multi-cap universe."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
ROLE_ARN = "arn:aws:iam::" + ACCOUNT + ":role/lambda-execution-role"

L = boto3.client("lambda", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=900, connect_timeout=10))
EB = boto3.client("events", region_name=REGION)
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
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def deploy_lambda(name, src_path, env, mem=1024, timeout=600):
    src = open(src_path).read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    
    exists = False
    try:
        L.get_function(FunctionName=name)
        exists = True
    except L.exceptions.ResourceNotFoundException:
        pass
    
    if exists:
        wait_ready(name)
        for attempt in range(5):
            try:
                L.update_function_code(FunctionName=name, ZipFile=zb)
                break
            except L.exceptions.ResourceConflictException:
                time.sleep(15)
        wait_ready(name)
        L.update_function_configuration(
            FunctionName=name, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=mem, Timeout=timeout,
            Environment={"Variables": env},
        )
    else:
        L.create_function(
            FunctionName=name, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role=ROLE_ARN, Code={"ZipFile": zb},
            Timeout=timeout, MemorySize=mem,
            Environment={"Variables": env},
        )
    wait_ready(name)


def schedule(name, expr):
    rule = name + "-daily"
    arn = EB.put_rule(Name=rule, ScheduleExpression=expr, State="ENABLED")["RuleArn"]
    fn_arn = "arn:aws:lambda:" + REGION + ":" + ACCOUNT + ":function:" + name
    EB.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        L.add_permission(FunctionName=name, StatementId=rule + "-eb",
                         Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                         SourceArn=arn)
    except L.exceptions.ResourceConflictException:
        pass


def main():
    section("1) Deploy earnings-pead (NEW)")
    env = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
        "MAX_TICKERS": "1500",
        "TIMEOUT_BUDGET_S": "550",
        "N_WORKERS": "12",
    }
    deploy_lambda("justhodl-earnings-pead",
                   "aws/lambdas/justhodl-earnings-pead/source/lambda_function.py",
                   env, mem=1024, timeout=600)
    schedule("justhodl-earnings-pead", "cron(0 8 * * ? *)")
    log("  ✓ deployed + scheduled")

    # Smoke
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

    section("2) Redeploy volatility-squeeze with multi-cap universe")
    env_sq = dict(env)
    env_sq["MAX_TICKERS"] = "1500"
    deploy_lambda("justhodl-volatility-squeeze-hunter",
                   "aws/lambdas/justhodl-volatility-squeeze-hunter/source/lambda_function.py",
                   env_sq, mem=1024, timeout=600)
    log("  ✓ redeployed")
    log("  invoking squeeze...")
    t0 = time.time()
    r = L2.invoke(FunctionName="justhodl-volatility-squeeze-hunter",
                   InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(time.time()-t0) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + str(body.get("body"))[:300])

    section("3) Redeploy revenue-acceleration with multi-cap universe")
    env_ra = dict(env)
    env_ra["MAX_TICKERS"] = "1500"
    deploy_lambda("justhodl-revenue-acceleration",
                   "aws/lambdas/justhodl-revenue-acceleration/source/lambda_function.py",
                   env_ra, mem=1024, timeout=600)
    log("  ✓ redeployed")
    log("  invoking rev-accel...")
    t0 = time.time()
    r = L2.invoke(FunctionName="justhodl-revenue-acceleration",
                   InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(time.time()-t0) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + str(body.get("body"))[:300])

    section("4) Inspect all 3 outputs")
    for key in ["data/earnings-pead.json", "data/volatility-squeeze.json",
                 "data/revenue-acceleration.json"]:
        try:
            obj = S3.get_object(Bucket=BUCKET, Key=key)
            d = json.loads(obj["Body"].read())
            log("")
            log("  ── " + key + " ──")
            log("  stats: " + json.dumps(d.get("stats", {}))[:300])
            top = d.get("summary", {}).get("top_25_overall", [])[:8]
            for t in top:
                if "beat_streak" in t:
                    log("    {:<6} score={:>5.1f}  streak={}Q  surprise={:+.1f}%  cap={}  drift_active={}".format(
                        t["symbol"], t["score"], t.get("beat_streak", 0),
                        t.get("latest_surprise_pct", 0),
                        t.get("cap_bucket", "?"),
                        t.get("drift_active", False)))
                elif "n_signals" in t:
                    log("    {:<6} score={:>5.1f}  n_sig={}  base={}d  bb={:.0f}%".format(
                        t["symbol"], t["score"], t["n_signals"],
                        t.get("base_days", 0), t.get("bb_pct", 0)))
                else:
                    log("    {:<6} score={:>5.1f}  growth={:+.0f}%  Δ={:+.1f}pp  streak={}Q".format(
                        t["symbol"], t["score"], t.get("growth", 0) or 0,
                        t.get("acceleration", 0) or 0,
                        t.get("consec_accel", 0)))
        except Exception as e:
            log("  ❌ " + key + ": " + str(e))


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
    with open(os.path.join(out, "phase_y7_pead_plus_rebuild.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
