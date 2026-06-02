"""1145 — verify the new bull/base/bear scenario output."""
import json, pathlib, time
from datetime import datetime, timezone
import urllib.request

REPORT = "aws/ops/reports/1145_scenarios_verify.json"
LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"


def smoke(ticker):
    url = f"{LAMBDA_URL}?ticker={ticker}&refresh=1"
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1145/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=180) as r:
        body = r.read()
        elapsed = round(time.time() - t0, 1)
    d = json.loads(body)
    s = d.get("scenarios") or {}
    return {
        "ticker":          ticker,
        "elapsed_s":       elapsed,
        "current_price":   (d.get("quote") or {}).get("price"),
        "verdict_pt":      (d.get("verdict") or {}).get("price_target_12m"),
        "bull": {
            "target":     (s.get("bull_case") or {}).get("price_target_12m"),
            "prob":       (s.get("bull_case") or {}).get("probability_pct"),
            "upside":     (s.get("bull_case") or {}).get("upside_pct"),
            "thesis":     ((s.get("bull_case") or {}).get("thesis_1liner") or "")[:90],
            "n_drivers":  len((s.get("bull_case") or {}).get("drivers") or []),
        },
        "base": {
            "target":     (s.get("base_case") or {}).get("price_target_12m"),
            "prob":       (s.get("base_case") or {}).get("probability_pct"),
            "upside":     (s.get("base_case") or {}).get("upside_pct"),
            "thesis":     ((s.get("base_case") or {}).get("thesis_1liner") or "")[:90],
        },
        "bear": {
            "target":     (s.get("bear_case") or {}).get("price_target_12m"),
            "prob":       (s.get("bear_case") or {}).get("probability_pct"),
            "upside":     (s.get("bear_case") or {}).get("upside_pct"),
            "thesis":     ((s.get("bear_case") or {}).get("thesis_1liner") or "")[:90],
        },
        "probability_sum":    s.get("probability_sum"),
        "expected_value":     s.get("expected_value_12m"),
        "expected_upside":    s.get("expected_value_upside_pct"),
        "risk_reward":        s.get("risk_reward_ratio"),
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "tickers": {}}
    # AAPL (HOLD, narrow spread), NVDA (STRONG_BUY, wide spread expected)
    for t in ["AAPL", "NVDA"]:
        try:
            out["tickers"][t] = smoke(t)
        except Exception as e:
            out["tickers"][t] = {"error": str(e)[:200]}
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1145] DONE")


if __name__ == "__main__":
    main()
