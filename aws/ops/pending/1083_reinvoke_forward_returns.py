"""ops 1083 — re-invoke justhodl-forward-returns after Bogle-model patch.

Expected change vs ops 1079:
  Before: percentile=50 for SPY/QQQ/IWM/EFA/EEM/VNQ (all fell back to hist median
          because FMP /stable/ratios-ttm returns [] for ETFs).
  After:  percentile reflects actual current trailing dividend yield + buyback +
          growth assumption. Each ETF gets a live ER, not a fallback.
"""
import json, os, time, base64
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-forward-returns"
OUT_KEY = "data/forward-returns.json"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())


def wait_idle(lam, fn, max_wait=180):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
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

    cfg = wait_idle(lam, FN)
    if not cfg:
        report["err"] = "function not idle"
        return _save(report)
    report["code_sha"] = cfg.get("CodeSha256", "")[:12]
    report["last_modified"] = cfg.get("LastModified")

    # Invoke
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail")
    report["invoke_status"] = inv["StatusCode"]
    report["fn_err"] = inv.get("FunctionError")
    log = base64.b64decode(inv.get("LogResult", "")).decode("utf-8", errors="replace")
    report["log_tail"] = log[-1500:]

    time.sleep(3)
    o = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)
    data = json.loads(o["Body"].read())
    report["s3_size"] = o["ContentLength"]

    # Detail check on each asset
    detail = {}
    for sym, a in data.get("assets", {}).items():
        detail[sym] = {
            "fwd_er": a.get("forward_er_10y_pct"),
            "trailing_div_yield": a.get("trailing_dividend_yield_pct"),
            "buyback_assumption": a.get("buyback_yield_assumption_pct"),
            "growth_assumption": a.get("nominal_growth_assumption_pct"),
            "percentile": a.get("current_vs_history_percentile"),
            "verdict": a.get("verdict"),
            "ten_k_central": a.get("ten_k_in_10yr_usd", {}).get("central"),
            "price": a.get("current_price"),
        }
    report["asset_detail"] = detail
    report["headlines"] = data.get("headlines", [])
    report["rankings"] = data.get("rankings", {})
    report["portfolios"] = {
        k: {"er": p["forward_er_pct"], "ten_k": p["ten_k_10yr"]}
        for k, p in data.get("benchmark_portfolios", {}).items()
    }

    return _save(report)


def _save(report):
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1083.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:4000])


if __name__ == "__main__":
    main()
