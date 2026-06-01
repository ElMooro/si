"""1108 — audit what data exists for pump-detection signals."""
import json, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1108_pump_audit.json"
s3 = boto3.client("s3", region_name="us-east-1")

# Data files that could feed an early pump detector
PUMP_SIGNALS = {
    "retail-sentiment":          "data/retail-sentiment.json",
    "buzz-velocity":             "data/buzz-velocity.json",
    "momentum-breakout":         "data/momentum-breakout.json",
    "options-flow":              "data/options-flow.json",
    "news-velocity":             "data/news-velocity.json",
    "gdelt-sentiment":           "data/gdelt-sentiment.json",
    "earnings-tracker":          "data/earnings-tracker.json",
    "earnings-whisper":          "data/earnings-whisper.json",
    "eps-revision-velocity":     "data/eps-revision-velocity.json",
    "earnings-sentiment":        "data/earnings-sentiment.json",
    "earnings-tone-velocity":    "data/earnings-tone-velocity.json",
    "earnings-pead":             "data/earnings-pead.json",
    "earnings-quality":          "data/earnings-quality.json",
    "sector-earnings-diffusion": "data/sector-earnings-diffusion.json",
    "post-earnings-mean-rev":    "data/post-earnings-mean-rev.json",
    "valuations":                "data/valuations.json",
    "fundamentals":              "data/fundamentals.json",
    "etf-flows":                 "data/etf-flows.json",
    "capital-return":            "data/capital-return.json",
    "stablecoin-flow":           "data/stablecoin-flow.json",
    "tic-flows":                 "data/tic-flows.json",
    "sympathetic-momentum":      "data/sympathetic-momentum.json",
    "ticker-trends":             "data/ticker-trends.json",
    "synthetic-monitor":         "data/synthetic-monitor.json",
    "political-trades":          "data/political-trades.json",
    "sec-filings":               "data/sec-filings-intel.json",
    "insider-aggregate":         "data/insider-aggregate.json",
    "options-flow-scanner":      "data/options-flow-scanner.json",
    "exchange-flows":            "data/exchange-flows.json",
    "liquidity-flow":            "data/liquidity-flow.json",
    "aaii-sentiment":            "data/aaii-sentiment.json",
    "news-sentiment":            "data/news-sentiment.json",
    "event-flow-monitor":        "data/event-flow-monitor.json",
    "earnings-cascade":          "data/earnings-cascade.json",
    "earnings-iv-crush":         "data/earnings-iv-crush.json",
    "dividend-growth":           "data/dividend-growth.json",
    "fmp-fundamentals":          "data/fmp-fundamentals.json",
    "lobbying-intel":            "data/lobbying-intel.json",
    "ark-holdings":              "data/ark-holdings.json",
    "patent-velocity":           "data/patent-velocity.json",
    "political-stocks":          "data/political-stocks.json",
    "hiring-velocity":           "data/hiring-velocity.json",
    "buzz-velocity-summary":     "data/buzz-velocity-summary.json",
}

ALERT_TICKERS = ["SAP", "CXAI", "NBIS", "ARM", "RDDT"]

def main():
    out = {"started": datetime.now(timezone.utc).isoformat(),
            "alert_tickers": ALERT_TICKERS,
            "engines": {}}
    
    for name, key in PUMP_SIGNALS.items():
        info = {"key": key}
        try:
            obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            info["exists"] = True
            info["size_kb"] = round(obj["ContentLength"]/1024, 1)
            info["last_modified"] = obj["LastModified"].isoformat()
            info["age_h"] = round((datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600, 1)
        except Exception:
            info["exists"] = False
        out["engines"][name] = info
    
    # Look for the alert tickers across these data files
    out["ticker_hits"] = {t: [] for t in ALERT_TICKERS}
    
    for name, info in out["engines"].items():
        if not info.get("exists"):
            continue
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=info["key"])
            d = json.loads(obj["Body"].read())
            text = json.dumps(d, default=str).upper()
            for t in ALERT_TICKERS:
                # Word-boundary match
                if f'"{t}"' in text or f'"{t.lower()}"' in text.lower() or f' {t} ' in text or f' {t},' in text:
                    out["ticker_hits"][t].append(name)
        except Exception:
            pass
    
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1108] DONE")

if __name__ == "__main__":
    main()
