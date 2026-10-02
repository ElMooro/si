"""
justhodl-retail-attention -- Wikipedia Retail Attention Tracker
===============================================================

Retail attention proxy: daily Wikipedia pageviews per company's article over
the last 90 days. Attention score = recent 7-day average vs 90-day average
(spike ratio). Sudden spikes often precede / accompany retail-driven moves.

DATA SOURCES
------------
1. Wikimedia Pageviews API (FREE, no key):
   https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/
   en.wikipedia/all-access/all-agents/{article}/daily/{start}/{end}
2. Google Trends: UNAVAILABLE from automation (trends.google.com explore
   endpoint requires session tokens and returns 429 to bots). Marked
   explicitly in output; fail-soft wikipedia-only.

METRICS
-------
views_7d_avg / views_90d_avg / spike_ratio = 7d_avg / 90d_avg (1.0 = normal)
attention_score = 100 * spike_ratio / (spike_ratio + 1)  -> 0..100, 50 = normal,
   75 = 3x normal attention, 33 = half normal attention.

OUTPUT
------
data/retail-attention.json
{
  generated_at, status, sources: {wikipedia: ok, google_trends: unavailable},
  by_ticker: {TICKER: {article, views_7d_avg, views_90d_avg, spike_ratio,
                       attention_score, days_covered}},
  top_spikes: [{ticker, article, spike_ratio, attention_score, views_7d_avg}, ...],
  coverage: {...}
}
"""
import datetime as dt
import json
import os
import time
import traceback
import urllib.parse
import urllib.request

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/retail-attention.json"
UA = ("JustHodlAI-RetailAttention/1.0 (contact: justhodl.ai; "
      "pipeline: justhodl-retail-attention)")
WIKI_BASE = ("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
             "en.wikipedia/all-access/all-agents")
PACING_S = 0.25
HISTORY_DAYS = 90
RECENT_DAYS = 7

# ticker -> Wikipedia article title (underscores, no URL-encoding needed here;
# encoding is applied at request time)
TICKER_ARTICLE = {
    "AAPL": "Apple_Inc.",
    "MSFT": "Microsoft",
    "GOOGL": "Google",
    "AMZN": "Amazon_(company)",
    "META": "Meta_Platforms",
    "TSLA": "Tesla,_Inc.",
    "NVDA": "Nvidia",
    "AVGO": "Broadcom",
    "ORCL": "Oracle_Corporation",
    "ADBE": "Adobe_Inc.",
    "CRM": "Salesforce",
    "AMD": "AMD",
    "INTC": "Intel",
    "QCOM": "Qualcomm",
    "TXN": "Texas_Instruments",
    "AMAT": "Applied_Materials",
    "MU": "Micron_Technology",
    "CSCO": "Cisco",
    "IBM": "IBM",
    "NFLX": "Netflix,_Inc.",
    "DIS": "The_Walt_Disney_Company",
    "NKE": "Nike,_Inc.",
    "MCD": "McDonald's",
    "SBUX": "Starbucks",
    "WMT": "Walmart",
    "COST": "Costco",
    "HD": "The_Home_Depot",
    "TGT": "Target_Corporation",
    "JPM": "JPMorgan_Chase",
    "BAC": "Bank_of_America",
    "V": "Visa_Inc.",
    "MA": "Mastercard",
    "PYPL": "PayPal",
    "COIN": "Coinbase",
    "PLTR": "Palantir_Technologies",
    "SNOW": "Snowflake_Inc.",
    "SHOP": "Shopify",
    "UBER": "Uber",
    "ABNB": "Airbnb",
    "PFE": "Pfizer",
    "JNJ": "Johnson_&_Johnson",
    "MRK": "Merck_&_Co.",
    "LLY": "Eli_Lilly_and_Company",
    "BMY": "Bristol_Myers_Squibb",
    "GILD": "Gilead_Sciences",
    "AMGN": "Amgen",
    "REGN": "Regeneron_Pharmaceuticals",
    "MRNA": "Moderna",
    "F": "Ford_Motor_Company",
    "GM": "General_Motors",
    "BA": "Boeing",
    "LMT": "Lockheed_Martin",
    "RTX": "RTX_Corporation",
    "GE": "General_Electric",
    "HON": "Honeywell",
}


def fetch_daily_views(article, start_yyyymmdd, end_yyyymmdd):
    """Returns list of daily view counts (oldest->newest) or None on failure."""
    enc = urllib.parse.quote(article, safe="")
    url = f"{WIKI_BASE}/{enc}/daily/{start_yyyymmdd}/{end_yyyymmdd}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode("utf-8", errors="ignore"))
        items = data.get("items") or []
        views = [int(it.get("views") or 0) for it in items
                 if isinstance(it, dict)]
        return views if views else None
    except Exception as e:
        print(f"wiki fetch failed for {article}: {e}")
        return None


def attention_score(spike_ratio):
    # 0..100, monotonic; 1.0 -> 50 (normal), 3.0 -> 75, 0.5 -> ~33
    return round(100.0 * spike_ratio / (spike_ratio + 1.0), 1)


def lambda_handler(event, context):
    started = time.time()
    s3 = boto3.client("s3")
    today = dt.datetime.now(dt.timezone.utc).date()
    # Wikimedia data lags ~1-2d; end at yesterday to avoid partial-day skew.
    end = today - dt.timedelta(days=1)
    start = end - dt.timedelta(days=HISTORY_DAYS - 1)
    s_s, s_e = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

    output = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "status": "ok",
        "sources": {
            "wikipedia_pageviews": "ok",
            "google_trends": "unavailable",
        },
        "source_notes": [
            "Google Trends is unavailable from automated pipelines: the "
            "trends.google.com explore API requires session tokens and returns "
            "HTTP 429 to non-browser clients. Attention signal is Wikipedia-only "
            "until a supported Trends path exists.",
        ],
        "window": {"start": start.isoformat(), "end": end.isoformat(),
                   "history_days": HISTORY_DAYS, "recent_days": RECENT_DAYS},
        "by_ticker": {},
        "top_spikes": [],
        "coverage": {
            "tickers_attempted": len(TICKER_ARTICLE),
            "tickers_with_data": 0,
            "articles_failed": [],
        },
    }

    try:
        for ticker, article in TICKER_ARTICLE.items():
            views = fetch_daily_views(article, s_s, s_e)
            time.sleep(PACING_S)
            if not views:
                output["coverage"]["articles_failed"].append(
                    {"ticker": ticker, "article": article})
                continue
            n = len(views)
            recent = views[-RECENT_DAYS:] if n >= RECENT_DAYS else views
            avg_7d = sum(recent) / len(recent)
            avg_90d = sum(views) / n
            spike = (avg_7d / avg_90d) if avg_90d > 0 else 0.0
            output["by_ticker"][ticker] = {
                "article": article,
                "views_7d_avg": round(avg_7d, 1),
                "views_90d_avg": round(avg_90d, 1),
                "spike_ratio": round(spike, 3),
                "attention_score": attention_score(spike),
                "days_covered": n,
            }
        output["coverage"]["tickers_with_data"] = len(output["by_ticker"])
        output["top_spikes"] = [
            {"ticker": t, "article": d["article"],
             "spike_ratio": d["spike_ratio"],
             "attention_score": d["attention_score"],
             "views_7d_avg": d["views_7d_avg"]}
            for t, d in sorted(output["by_ticker"].items(),
                               key=lambda kv: kv[1]["spike_ratio"],
                               reverse=True)[:10]
        ]
        if not output["by_ticker"]:
            output["status"] = "degraded"
            output["source_notes"].append(
                "All Wikipedia pageview fetches failed; check network/API availability.")
        elif output["coverage"]["articles_failed"]:
            output["status"] = "partial"
        output["run_duration_seconds"] = round(time.time() - started, 1)

        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                      Body=json.dumps(output, default=str).encode("utf-8"),
                      ContentType="application/json",
                      CacheControl="public, max-age=3600")
        print(f"wrote {S3_KEY}: status={output['status']} "
              f"tickers={len(output['by_ticker'])} "
              f"failed={len(output['coverage']['articles_failed'])}")
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "status": output["status"],
                                    "tickers": len(output["by_ticker"]),
                                    "duration_s": round(time.time() - started, 1)})}
    except Exception as e:
        # FAIL-SOFT: never crash the handler; always write *something*.
        try:
            output["status"] = "error"
            output["source_notes"].append(f"handler exception: {e}")
            output["run_duration_seconds"] = round(time.time() - started, 1)
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(output, default=str).encode("utf-8"),
                          ContentType="application/json",
                          CacheControl="public, max-age=3600")
        except Exception as e2:
            print(f"failed to write fallback output: {e2}")
        print(traceback.format_exc()[:1500])
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": str(e)})}
