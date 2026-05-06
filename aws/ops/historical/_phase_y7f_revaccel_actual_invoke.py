"""Phase Y7f — Invoke the real rev-accel Lambda but with verbose tracing
on a SINGLE symbol to find the production bug."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=600))

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)


def main():
    LAMBDA_NAME = "justhodl-revenue-acceleration"
    
    # Get current cfg
    c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
    log("  current env: " + json.dumps((c.get("Environment") or {}).get("Variables", {}), default=str))
    log("  last modified: " + str(c.get("LastModified")))
    log("  size: " + str(c.get("CodeSize")))
    
    # Set MAX_TICKERS=10 to keep it fast and verbose
    cur_env = (c.get("Environment") or {}).get("Variables", {}) or {}
    cur_env["MAX_TICKERS"] = "10"
    cur_env["N_WORKERS"] = "2"
    L.update_function_configuration(
        FunctionName=LAMBDA_NAME,
        Environment={"Variables": cur_env},
    )
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    
    log("  invoking with MAX_TICKERS=10, N_WORKERS=2...")
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    log("  status: " + str(r["StatusCode"]) + ", dur: " + "{:.1f}".format(time.time()-t0) + "s")
    body = json.loads(r["Payload"].read())
    log("  body: " + str(body)[:300])
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        log("")
        log("  ── FULL LOG TAIL ──")
        for ln in tail.splitlines():
            log("    " + ln.rstrip())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_y7f_revaccel_invoke.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
