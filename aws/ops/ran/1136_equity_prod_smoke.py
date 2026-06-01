"""1136 — production smoke test:
  - GET https://justhodl.ai/why.html → verify the page loads + has the right URL embedded
  - Pre-warm popular tickers: AAPL (already cached), MSFT, NVDA, GOOGL, BRK-B
"""
import json
import pathlib
import time
import traceback
from datetime import datetime, timezone

import urllib.request

REPORT = "aws/ops/reports/1136_equity_prod_smoke.json"
LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
WHY_URL = "https://justhodl.ai/why.html"

PREWARM_TICKERS = ["MSFT", "NVDA", "GOOGL", "BRK-B", "TSLA"]


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR",
                                "error": str(e)[:300],
                                "traceback": traceback.format_exc()[:800]})
        return None


def check_why_html():
    """Fetch live why.html and confirm the Lambda URL is embedded."""
    req = urllib.request.Request(WHY_URL, headers={"User-Agent": "ops-1136/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        body = r.read().decode("utf-8", errors="replace")
    return {
        "status_code":         r.status,
        "size_kb":              round(len(body)/1024, 1),
        "has_research_url":     LAMBDA_URL in body,
        "has_placeholder":      "CONFIGURE_AFTER_DEPLOY" in body,
        "has_picker":           'id="tickerInput"' in body,
        "has_quick_buttons":    'AAPL' in body and 'MSFT' in body,
        "has_fetchAndRender":   "fetchAndRender" in body,
        "has_renderReport":     "function renderReport" in body,
        "title":                _between(body, "<title>", "</title>"),
    }


def _between(s, a, b):
    i = s.find(a)
    if i < 0: return None
    j = s.find(b, i + len(a))
    return s[i+len(a):j] if j > i else None


def smoke_ticker(ticker, timeout=180):
    """Hit the Lambda URL for one ticker. Cached after first call."""
    url = f"{LAMBDA_URL}?ticker={ticker}"
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1136/1.0"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            elapsed = round(time.time() - t0, 1)
        d = json.loads(body)
        v = d.get("verdict") or {}
        return {
            "ticker":      ticker,
            "ok":          True,
            "elapsed_s":   elapsed,
            "size_kb":     round(len(body)/1024, 1),
            "from_cache":  d.get("from_cache"),
            "company":     (d.get("company") or {}).get("name"),
            "price":       (d.get("quote") or {}).get("price"),
            "rating":      v.get("rating"),
            "conviction":  v.get("conviction_grade"),
            "price_target": v.get("price_target_12m"),
            "upside_pct":  v.get("upside_pct"),
            "income_yrs":  len((d.get("statements") or {}).get("income_annual") or []),
            "claude_s":    (d.get("metadata") or {}).get("claude_elapsed_sec"),
            "fmp_ok":      (d.get("metadata") or {}).get("data_sources_loaded"),
            "rationale":   (v.get("verdict_rationale") or "")[:160],
        }
    except Exception as e:
        return {"ticker": ticker, "ok": False, "error": str(e)[:200],
                  "elapsed_s": round(time.time()-t0, 1)}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    phase(out, "fetch_why_html", check_why_html)

    # Pre-warm popular tickers — first call generates + caches; subsequent users get instant.
    # Sequentially because each call takes ~30s and Lambda has timeout=180.
    for t in PREWARM_TICKERS:
        phase(out, f"prewarm_{t}", lambda t=t: smoke_ticker(t))

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1136] DONE")


if __name__ == "__main__":
    main()
