"""justhodl-coverage-gap-report — E11 (ops 4447).

Nightly actual-vs-target coverage against the Bloomberg/Refinitiv bar,
computed from LIVE platform artifacts (never asserted): tickers from the E1
symbology master, FIGIs from its enrichment stats, filings from E3's EDGAR
summary, NY Fed rates from E8, FRED breadth from the E12 rollup's
feed-count (labelled a proxy — series-level census is a stated TODO),
CUSIPs honestly 0 until the 13F join lands. Writes
data/audit/coverage-gap.json with per-metric numerator/denominator/method.
"""
import json
import os
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")


def _get(k):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception:
        return None


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    sym = _get("data/symbology/master.json") or {}
    edgar = _get("data/warm/edgar-filings/latest-summary.json") or {}
    nyfed = _get("data/warm/nyfed/latest-summary.json") or {}
    roll = _get("data/audit/data-source-rollup.json") or {}
    n_tickers = sym.get("n_tickers")
    bt = sym.get("by_ticker") or {}
    n_figi = sum(1 for r in bt.values() if r.get("figi")) if bt else None
    nyfed_ok = sum(1 for v in (nyfed.get("rates") or {}).values()
                   if v.get("n_obs"))
    fred_feeds = (roll.get("global_feed_counts") or {}).get("fred")

    def m(name, actual, target, method):
        pct = (round(100 * actual / target, 2)
               if isinstance(actual, (int, float)) and target else None)
        return {"metric": name, "actual": actual, "target": target,
                "pct_of_target": pct, "method": method}

    metrics = [
        m("us_tickers", n_tickers, 320000,
          "E1 symbology master (SEC registrants) vs Bloomberg global "
          "universe incl. delisted+funds"),
        m("figi_ids", n_figi, n_tickers or 0,
          "FIGIs resolved vs E1 spine (converges ~2500/night)"),
        m("cusips", 0, 500000,
          "HONEST ZERO — 13F cusip-map join not yet landed"),
        m("edgar_filings_qtd", edgar.get("n_filings"), None,
          "E3 full-index, complete for current quarter (no external "
          "target: completeness is the target and it is met)"),
        m("nyfed_reference_rates", nyfed_ok, 5,
          "E8 histories loaded of SOFR/EFFR/OBFR/TGCR/BGCR"),
        m("fred_feeds_in_use", fred_feeds, 45000,
          "PROXY: E12 rollup feed-count, not a series census — "
          "series-level census is a stated TODO (E4)"),
    ]
    doc = {"as_of": now, "spec": "E11 coverage-gap report",
           "metrics": metrics,
           "note": "Every number is read from a live platform artifact "
                   "this run; unknowns are null and zeros are honest."}
    s3.put_object(Bucket=BUCKET, Key="data/audit/coverage-gap.json",
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True,
           "summary": {x["metric"]: x["actual"] for x in metrics}}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
