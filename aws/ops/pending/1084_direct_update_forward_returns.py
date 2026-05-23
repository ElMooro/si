"""ops 1084 — directly update forward-returns Lambda code from repo,
   then invoke and verify Bogle model is producing live per-asset ERs.

ops 1083 showed code_sha unchanged after our Bogle patch — deploy-lambdas.yml
either hasn't run yet or filtered it out. Direct boto3 update is faster.
"""
import io, json, os, time, zipfile, base64
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
FN = "justhodl-forward-returns"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/forward-returns.json"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
SRC_DIR = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")


def zip_dir(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                full = os.path.join(root, f)
                z.write(full, os.path.relpath(full, d))
    return buf.getvalue()


def wait_idle(lam, max_wait=180):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") in ("Successful", None):
                return cfg
            if cfg.get("LastUpdateStatus") == "Failed":
                return None
        except Exception:
            pass
        time.sleep(3)
    return None


def main():
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    cfg0 = wait_idle(lam)
    report["before_sha"] = cfg0.get("CodeSha256", "")[:12] if cfg0 else None
    report["before_modified"] = cfg0.get("LastModified") if cfg0 else None

    zip_bytes = zip_dir(SRC_DIR)
    report["zip_kb"] = round(len(zip_bytes) / 1024, 1)

    r = lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes, Publish=False)
    report["update_status"] = r.get("LastUpdateStatus")
    cfg1 = wait_idle(lam)
    report["after_sha"] = cfg1.get("CodeSha256", "")[:12] if cfg1 else None
    report["after_modified"] = cfg1.get("LastModified") if cfg1 else None
    report["sha_changed"] = report["before_sha"] != report["after_sha"]

    # Invoke
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail")
    report["invoke_status"] = inv["StatusCode"]
    report["fn_err"] = inv.get("FunctionError")
    log = base64.b64decode(inv.get("LogResult", "")).decode("utf-8", errors="replace")
    report["log_tail"] = log[-1200:]

    time.sleep(3)
    o = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)
    data = json.loads(o["Body"].read())

    # Detail per asset
    detail = {}
    for sym, a in data.get("assets", {}).items():
        detail[sym] = {
            "fwd_er": a.get("forward_er_10y_pct"),
            "trailing_dy": a.get("trailing_dividend_yield_pct"),
            "buyback": a.get("buyback_yield_assumption_pct"),
            "nominal_g": a.get("nominal_growth_assumption_pct"),
            "pctile": a.get("current_vs_history_percentile"),
            "verdict": a.get("verdict"),
            "ten_k": a.get("ten_k_in_10yr_usd", {}).get("central"),
        }
    report["assets"] = detail
    report["headlines"] = data.get("headlines", [])
    report["portfolios"] = {
        k: {"er": p["forward_er_pct"], "ten_k": p["ten_k_10yr"]}
        for k, p in data.get("benchmark_portfolios", {}).items()
    }
    report["rankings"] = data.get("rankings", {})

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1084.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:4500])


if __name__ == "__main__":
    main()
