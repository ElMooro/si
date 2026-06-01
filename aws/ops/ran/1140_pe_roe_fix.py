"""1140 — verify P/E + ROE now populate after field-name fix."""
import json, pathlib, time
from datetime import datetime, timezone
import urllib.request

REPORT = "aws/ops/reports/1140_pe_roe_fix.json"
LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"


def smoke(ticker):
    url = f"{LAMBDA_URL}?ticker={ticker}&refresh=1"
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1140/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=180) as r:
        body = r.read()
        elapsed = round(time.time() - t0, 1)
    d = json.loads(body)
    pc = d.get("peer_comparison") or {}
    val = d.get("valuation") or {}
    rows = pc.get("rows") or []
    return {
        "ticker":        ticker,
        "elapsed_s":     elapsed,
        # Valuation fix verification
        "pe_ttm":        val.get("pe_ttm"),
        "pe_5y_avg":     val.get("pe_5yr_avg"),
        "roe_ttm_pct":   val.get("roe_ttm_pct"),
        "roic_ttm_pct":  val.get("roic_ttm_pct"),
        "pfcf_ttm":      val.get("pfcf_ttm"),
        "ev_ebitda":     val.get("ev_ebitda"),
        "div_yield_pct": val.get("div_yield_pct"),
        # Peer table verification
        "subject_pe":    next((r.get("pe") for r in rows if r.get("is_subject")), None),
        "subject_roe":   next((r.get("roe_pct") for r in rows if r.get("is_subject")), None),
        "peer_pes":      [{"sym": r.get("symbol"), "pe": r.get("pe"), "roe": r.get("roe_pct")}
                            for r in rows if not r.get("is_subject")],
        "median_pe":     (pc.get("summary") or {}).get("median_pe"),
        "median_roe":    (pc.get("summary") or {}).get("median_roe_pct"),
        "premium_pct_pe": (pc.get("relative") or {}).get("premium_pct_pe"),
        # Claude assessment now references real numbers
        "ai_peer_assessment": (d.get("peer_comparison_assessment") or "")[:500],
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "tickers": {}}
    for t in ["AAPL", "MSFT", "JPM"]:
        try:
            out["tickers"][t] = smoke(t)
        except Exception as e:
            out["tickers"][t] = {"error": str(e)[:200]}
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1140] DONE")


if __name__ == "__main__":
    main()
