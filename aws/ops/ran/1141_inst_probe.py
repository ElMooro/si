"""1141 — probe FMP endpoints for the 5 institutional features.

Tests endpoint availability + field shapes so we don't have to redeploy
the Lambda 5 times to debug field names. Returns the first row from each
endpoint so we know exactly what FMP gives us.

Endpoints under test:
  1. Earnings beat/miss → /stable/earnings (already fetched)
                       → /stable/earnings-surprises (more specific)
  2. Capital allocation → already in cash flow + dividends
                       → /stable/share-buybacks (if exists)
  3. Insider transactions → /stable/insider-trading
                         → /stable/insider-trading-statistics
  4. Short interest → /stable/short-interest (unsure if exists on Premium)
                  → /stable/historical-short-interest
  5. Earnings transcripts → /stable/earning-call-transcript
                         → /stable/earning-call-transcript-dates
"""
import json, pathlib, time
from datetime import datetime, timezone
import urllib.request, urllib.parse, urllib.error

REPORT = "aws/ops/reports/1141_inst_probe.json"
FMP = "https://financialmodelingprep.com/stable"
KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
TICKER = "AAPL"


def fetch(path, **params):
    q = dict(params); q["apikey"] = KEY
    url = f"{FMP}/{path}?{urllib.parse.urlencode(q)}"
    req = urllib.request.Request(url, headers={"User-Agent": "ops-1141/1.0"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read()
        elapsed = round((time.time()-t0)*1000, 0)
        data = json.loads(body) if body else None
        sample = None
        if isinstance(data, list) and data:
            sample = data[0] if data else None
            n = len(data)
        elif isinstance(data, dict):
            sample = data
            n = 1
        else:
            n = 0
        return {
            "status":     "ok",
            "elapsed_ms": elapsed,
            "size":       len(body),
            "n_items":    n,
            "all_keys":   sorted(sample.keys()) if isinstance(sample, dict) else [],
            "sample":     sample,
        }
    except urllib.error.HTTPError as e:
        return {"status": "http_error", "code": e.code, "msg": e.reason}
    except Exception as e:
        return {"status": "error", "msg": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "ticker": TICKER, "probes": {}}

    # ── 1. Earnings beat/miss
    out["probes"]["earnings"]            = fetch("earnings", symbol=TICKER, limit=8)
    out["probes"]["earnings_surprises"]  = fetch("earnings-surprises", symbol=TICKER, limit=8)

    # ── 2. Capital allocation (we have cashflow already; check buyback endpoint)
    out["probes"]["share_buybacks"]      = fetch("share-buybacks", symbol=TICKER)
    out["probes"]["share_repurchases"]   = fetch("share-repurchases", symbol=TICKER)

    # ── 3. Insider transactions
    out["probes"]["insider_trading"]     = fetch("insider-trading", symbol=TICKER, limit=20)
    out["probes"]["insider_trading_stats"] = fetch("insider-trading-statistics", symbol=TICKER)
    # Also a different shape
    out["probes"]["acquisition_ownership"] = fetch("acquisition-of-beneficial-ownership", symbol=TICKER)

    # ── 4. Short interest
    out["probes"]["short_interest"]      = fetch("short-interest", symbol=TICKER)
    out["probes"]["historical_short"]    = fetch("historical-short-interest", symbol=TICKER, limit=12)

    # ── 5. Earnings transcripts
    out["probes"]["transcript_dates"]    = fetch("earning-call-transcript-dates", symbol=TICKER)
    out["probes"]["transcript_latest"]   = fetch("earning-call-transcript-latest", symbol=TICKER)
    out["probes"]["transcript"]          = fetch("earning-call-transcript", symbol=TICKER, year=2026, quarter=1)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1141] DONE")


if __name__ == "__main__":
    main()
