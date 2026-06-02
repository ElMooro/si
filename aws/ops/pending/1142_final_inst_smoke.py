"""1142 — final verification across all 5 institutional features.

Smoke a FRESH ticker (no cache) to exercise the entire updated pipeline:
  1. Earnings beat/miss track record
  2. Capital allocation timeline
  3. Institutional activity (13D/13G)
  4. Earnings call sentiment
  5. Short interest data-gap card
"""
import json, pathlib, time
from datetime import datetime, timezone
import urllib.request

REPORT = "aws/ops/reports/1142_final_inst_smoke.json"
LAMBDA_URL = "https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"


def smoke(ticker, refresh=True, timeout=180):
    url = f"{LAMBDA_URL}?ticker={ticker}" + ("&refresh=1" if refresh else "")
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1142/1.0"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        elapsed = round(time.time() - t0, 1)
    d = json.loads(body)

    # Feature 1: Earnings track record
    etr = d.get("earnings_track_record") or {}
    # Feature 2: Capital allocation
    ca = d.get("capital_allocation") or {}
    # Feature 3: Institutional activity
    ia = d.get("institutional_activity") or {}
    # Feature 4: Earnings call sentiment
    ec = d.get("earnings_call") or {}
    sent = d.get("earnings_call_sentiment") or {}
    # Feature 5: Short interest (placeholder)
    si = d.get("short_interest") or {}

    return {
        "ticker":         ticker,
        "elapsed_s":      elapsed,
        "size_kb":        round(len(body)/1024, 1),
        "rating":         (d.get("verdict") or {}).get("rating"),
        "conviction":     (d.get("verdict") or {}).get("conviction_grade"),

        # Feature 1
        "f1_earnings_track_record": {
            "n_quarters":     etr.get("n_quarters"),
            "eps_beat_rate":  etr.get("eps_beat_rate_pct"),
            "eps_beats":      etr.get("eps_beats"),
            "eps_misses":     etr.get("eps_misses"),
            "eps_streak":     etr.get("eps_current_streak"),
            "magnitude_trend": etr.get("eps_magnitude_trend"),
            "rev_beat_rate":  etr.get("revenue_beat_rate_pct"),
            "ai_assessment":  (d.get("earnings_track_record_assessment") or "")[:240],
        },

        # Feature 2
        "f2_capital_allocation": {
            "n_years":            len(ca.get("timeline") or []),
            "total_returned_10y": ca.get("total_returned_10y"),
            "shareholder_yield":  ca.get("shareholder_yield_pct"),
            "buyback_share":      ca.get("buyback_share_of_return_pct"),
            "payout_ratio":       ca.get("latest_payout_ratio_pct"),
            "capex_intensity":    ca.get("capex_intensity_trend"),
            "ai_assessment":      (d.get("capital_allocation_assessment") or "")[:240],
        },

        # Feature 3
        "f3_institutional_activity": {
            "n_total":         ia.get("n_filings_total"),
            "n_recent_24m":    ia.get("n_filings_recent_24m"),
            "n_unique_filers": ia.get("n_unique_filers_24m"),
            "top_filers":      [f.get("filer", "?")[:40]
                                  for f in (ia.get("filings_display") or [])[:3]],
            "ai_assessment":   (d.get("institutional_activity_assessment") or "")[:240],
        },

        # Feature 4
        "f4_earnings_call": {
            "call_date":      ec.get("date") if ec else None,
            "call_quarter":   ec.get("quarter") if ec else None,
            "full_chars":     ec.get("full_chars") if ec else None,
            "overall_tone":   sent.get("overall_tone"),
            "guidance":       sent.get("guidance_change"),
            "n_topics":       len(sent.get("key_topics") or []),
            "n_quotes":       len(sent.get("notable_quotes") or []),
            "tone_summary":   (sent.get("tone_summary") or "")[:240],
        },

        # Feature 5
        "f5_short_interest": {
            "available":      si.get("available"),
            "reason_present": bool(si.get("reason")),
        },

        "claude_elapsed":   (d.get("metadata") or {}).get("claude_elapsed_sec"),
        "fmp_loaded":       f"{(d.get('metadata') or {}).get('data_sources_loaded')}/{(d.get('metadata') or {}).get('data_sources_total')}",
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "tickers": {}}
    # AAPL — well-covered, expect 13D/13G filings, transcript, all features
    # NVDA — high-growth, expect different capital allocation profile
    # KO — classic dividend payer, expect different cap allocation story
    for t in ["AAPL", "KO"]:
        try:
            out["tickers"][t] = smoke(t)
        except Exception as e:
            out["tickers"][t] = {"error": str(e)[:400]}
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1142] DONE")


if __name__ == "__main__":
    main()
