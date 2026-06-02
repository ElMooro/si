"""1146 — re-verify scenarios after max_tokens bump."""
import json, pathlib, time
from datetime import datetime, timezone
import urllib.request

REPORT = "aws/ops/reports/1146_scenarios_v2.json"
LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"


def smoke(ticker):
    url = f"{LAMBDA_URL}?ticker={ticker}&refresh=1"
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1146/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=180) as r:
        body = r.read()
        elapsed = round(time.time() - t0, 1)
    d = json.loads(body)
    s = d.get("scenarios") or {}
    md = d.get("metadata") or {}
    v = d.get("verdict") or {}
    return {
        "ticker":        ticker,
        "elapsed_s":     elapsed,
        # Diagnostic
        "claude_raw_chars":    md.get("claude_raw_chars"),
        "claude_parsed_keys":  md.get("claude_parsed_keys"),
        "claude_parse_error":  md.get("claude_parse_error"),
        "claude_elapsed":      md.get("claude_elapsed_sec"),
        # Verdict
        "verdict_rating":    v.get("rating"),
        "verdict_pt":        v.get("price_target_12m"),
        # Scenarios summary
        "scenarios_present": bool(s),
        "bull_target":       (s.get("bull_case") or {}).get("price_target_12m"),
        "bull_prob":         (s.get("bull_case") or {}).get("probability_pct"),
        "bull_upside":       (s.get("bull_case") or {}).get("upside_pct"),
        "base_target":       (s.get("base_case") or {}).get("price_target_12m"),
        "base_prob":         (s.get("base_case") or {}).get("probability_pct"),
        "bear_target":       (s.get("bear_case") or {}).get("price_target_12m"),
        "bear_prob":         (s.get("bear_case") or {}).get("probability_pct"),
        "probability_sum":   s.get("probability_sum"),
        "expected_value":    s.get("expected_value_12m"),
        "expected_upside":   s.get("expected_value_upside_pct"),
        "risk_reward":       s.get("risk_reward_ratio"),
        "current_price":     (d.get("quote") or {}).get("price"),
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "tickers": {}}
    for t in ["AAPL", "NVDA", "JPM"]:
        try:
            out["tickers"][t] = smoke(t)
        except Exception as e:
            out["tickers"][t] = {"error": str(e)[:300]}
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1146] DONE")


if __name__ == "__main__":
    main()
