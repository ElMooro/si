"""1147 — final verification of scenarios. AAPL fresh + page-load check."""
import json, pathlib, time, re
from datetime import datetime, timezone
import urllib.request

REPORT = "aws/ops/reports/1147_final_scenarios.json"
LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
WHY_URL = "https://justhodl.ai/why.html"


def smoke(ticker, refresh=True):
    url = f"{LAMBDA_URL}?ticker={ticker}" + ("&refresh=1" if refresh else "")
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1147/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=180) as r:
        body = r.read()
        elapsed = round(time.time() - t0, 1)
    d = json.loads(body)
    s = d.get("scenarios") or {}
    v = d.get("verdict") or {}
    md = d.get("metadata") or {}
    bull = s.get("bull_case") or {}
    base = s.get("base_case") or {}
    bear = s.get("bear_case") or {}
    return {
        "ticker":          ticker,
        "elapsed_s":       elapsed,
        "claude_s":        md.get("claude_elapsed_sec"),
        "claude_chars":    md.get("claude_raw_chars"),
        "current_price":   (d.get("quote") or {}).get("price"),
        "verdict":         v.get("rating"),
        "verdict_pt":      v.get("price_target_12m"),
        "bull_target":     bull.get("price_target_12m"),
        "bull_prob":       bull.get("probability_pct"),
        "bull_upside":     bull.get("upside_pct"),
        "bull_thesis":     (bull.get("thesis_1liner") or "")[:80],
        "bull_drivers_n":  len(bull.get("drivers") or []),
        "base_target":     base.get("price_target_12m"),
        "base_prob":       base.get("probability_pct"),
        "base_upside":     base.get("upside_pct"),
        "base_thesis":     (base.get("thesis_1liner") or "")[:80],
        "bear_target":     bear.get("price_target_12m"),
        "bear_prob":       bear.get("probability_pct"),
        "bear_upside":     bear.get("upside_pct"),
        "bear_thesis":     (bear.get("thesis_1liner") or "")[:80],
        "prob_sum":        s.get("probability_sum"),
        "ev":              s.get("expected_value_12m"),
        "ev_upside":       s.get("expected_value_upside_pct"),
        "rr":              s.get("risk_reward_ratio"),
    }


def check_page():
    req = urllib.request.Request(WHY_URL, headers={"User-Agent": "ops-1147/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        html = r.read().decode("utf-8", errors="replace")
    return {
        "size_kb":               round(len(html)/1024, 1),
        "has_renderScenarios":   "function renderScenarios" in html,
        "has_renderEarningsTrack": "function renderEarningsTrack" in html,
        "has_renderCapitalAllocation": "function renderCapitalAllocation" in html,
        "has_renderEarningsCall": "function renderEarningsCall" in html,
        "has_renderShortInterest": "function renderShortInterest" in html,
        "has_renderInstitutionalActivity": "function renderInstitutionalActivity" in html,
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "tickers": {}}
    out["page"] = check_page()
    for t in ["AAPL"]:   # just AAPL since NVDA + JPM already verified
        try:
            out["tickers"][t] = smoke(t)
        except Exception as e:
            out["tickers"][t] = {"error": str(e)[:300]}
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1147] DONE")


if __name__ == "__main__":
    main()
