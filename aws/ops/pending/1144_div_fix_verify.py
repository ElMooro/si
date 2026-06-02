"""1144 — re-verify capital allocation after FMP field-rename fix."""
import json, pathlib, time
from datetime import datetime, timezone
import urllib.request

REPORT = "aws/ops/reports/1144_div_fix_verify.json"
LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"


def smoke(ticker):
    url = f"{LAMBDA_URL}?ticker={ticker}&refresh=1"
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1144/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=180) as r:
        body = r.read()
        elapsed = round(time.time() - t0, 1)
    d = json.loads(body)
    ca = d.get("capital_allocation") or {}
    return {
        "ticker":            ticker,
        "elapsed_s":         elapsed,
        "total_divs_10y":    ca.get("total_dividends_10y"),
        "total_buybacks_10y":ca.get("total_buybacks_10y"),
        "total_returned":    ca.get("total_returned_10y"),
        "shareholder_yield": ca.get("shareholder_yield_pct"),
        "buyback_share_pct": ca.get("buyback_share_of_return_pct"),
        "payout_ratio":      ca.get("latest_payout_ratio_pct"),
        "ai_assessment":     (d.get("capital_allocation_assessment") or "")[:300],
        "n_years":           len(ca.get("timeline") or []),
        "latest_year":       (ca.get("timeline") or [{}])[0],
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "tickers": {}}
    for t in ["AAPL", "KO"]:
        try:
            out["tickers"][t] = smoke(t)
        except Exception as e:
            out["tickers"][t] = {"error": str(e)[:200]}
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1144] DONE")


if __name__ == "__main__":
    main()
