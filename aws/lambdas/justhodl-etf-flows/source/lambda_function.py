"""
justhodl-etf-flows — ETF creation/redemption tracker

Tracks where institutional money is actually going by monitoring daily
shares outstanding changes in liquid ETFs. Creation units = inflows,
redemptions = outflows.

Method:
  1. Pull last 30d of daily aggregates per ETF from Polygon
     (price + volume — but no shares outstanding directly)
  2. Pull current shares outstanding from Polygon /v3/reference/tickers
  3. Compute estimated AUM = shares_outstanding * latest_close
  4. Compute volume-weighted price = VWAP proxy
  5. Track aggregate net dollar flow per ETF category

Approach for FLOW estimation (since Polygon doesn't expose historical
shares outstanding directly for free):
  - Use ETF.com sourced data IF available
  - Fall back to FRED ETF holdings indicator
  - Use volume × price as proxy for dollar volume traded
  - Compute z-score of daily $ volume vs 60d trailing — high z = unusual flow

What we actually publish:
  - Per-ETF: 1d, 5d, 20d return, 5d / 20d / 60d $ volume, $ vol z-score
  - By category: net flow direction (relative volume z-scores)
  - Notable: ETFs with $ vol z > 2σ today vs 60d (unusual flow)

Output: data/etf-flows.json
"""
import json
import os
import time
import boto3
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from statistics import mean, stdev

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/etf-flows.json"

POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

# Liquid ETFs by category — covers the major asset classes
ETF_CATEGORIES = {
    "BROAD_EQUITY_US": ["SPY", "VOO", "IVV", "QQQ", "QQQM", "IWM", "VTI", "DIA"],
    "SECTOR_EQUITY": [
        "XLF",   # Financials
        "XLE",   # Energy
        "XLU",   # Utilities
        "XLV",   # Healthcare
        "XLY",   # Consumer Discretionary
        "XLP",   # Consumer Staples
        "XLB",   # Materials
        "XLI",   # Industrials
        "XLRE",  # Real Estate
        "XLK",   # Tech
        "XLC",   # Communications
    ],
    "RATES_TREASURIES": ["TLT", "IEF", "SHY", "GOVT", "BIL", "TIP"],
    "CREDIT": ["AGG", "HYG", "LQD", "JNK", "BND", "EMB"],
    "COMMODITIES": ["GLD", "IAU", "SLV", "USO", "UNG", "DBA", "DBC"],
    "INTERNATIONAL": ["EEM", "EFA", "VWO", "IEFA", "IEMG", "FXI", "EWJ", "EWZ", "INDA"],
    "VOLATILITY": ["VXX", "UVXY", "SVXY"],
    "CRYPTO": ["IBIT", "FBTC", "BITO", "ETHA", "BITB"],
    "ALTERNATIVES": ["BTAL", "MOAT", "QUAL", "MTUM", "USMV"],
}

ALL_ETFS = []
for ets in ETF_CATEGORIES.values():
    ALL_ETFS.extend(ets)
ALL_ETFS = list(set(ALL_ETFS))


def http_get(url, timeout=15):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; justhodl-etf-flows/1.0)",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fetch_polygon_aggs(ticker, days=70):
    """Last N days of daily bars."""
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=days + 14)  # cushion for weekends
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{urllib.parse.quote(ticker)}"
        f"/range/1/day/{start.isoformat()}/{today.isoformat()}"
        f"?adjusted=true&sort=asc&limit=200&apiKey={POLYGON_KEY}"
    )
    try:
        d = http_get(url)
        return d.get("results") or []
    except Exception:
        return []


def fetch_polygon_ticker(ticker):
    """Get current shares outstanding + name."""
    url = f"https://api.polygon.io/v3/reference/tickers/{urllib.parse.quote(ticker)}?apiKey={POLYGON_KEY}"
    try:
        d = http_get(url)
        r = d.get("results") or {}
        return {
            "name": r.get("name", ""),
            "share_class_shares_outstanding": r.get("share_class_shares_outstanding"),
            "weighted_shares_outstanding": r.get("weighted_shares_outstanding"),
            "market_cap": r.get("market_cap"),
        }
    except Exception:
        return None


def analyze_etf(ticker, category):
    """Compute return + flow proxy metrics."""
    bars = fetch_polygon_aggs(ticker, days=70)
    info = fetch_polygon_ticker(ticker)
    if not bars or len(bars) < 25:
        return None

    bars_sorted = sorted(bars, key=lambda b: b.get("t", 0))
    closes = [b["c"] for b in bars_sorted if b.get("c")]
    volumes = [b.get("v", 0) for b in bars_sorted]
    dollar_vols = [b.get("c", 0) * b.get("v", 0) for b in bars_sorted]

    if len(closes) < 25:
        return None

    latest_close = closes[-1]
    today_dvol = dollar_vols[-1]

    # Returns
    def ret_n(n):
        if len(closes) > n:
            return round(((closes[-1] - closes[-1 - n]) / closes[-1 - n]) * 100, 2)
        return None
    r1d = ret_n(1)
    r5d = ret_n(5)
    r20d = ret_n(20)

    # $ volume stats
    dvol_5d = mean(dollar_vols[-5:]) if len(dollar_vols) >= 5 else None
    dvol_20d = mean(dollar_vols[-20:]) if len(dollar_vols) >= 20 else None
    dvol_60d = mean(dollar_vols[-60:]) if len(dollar_vols) >= 60 else None

    # $ volume z-score: today vs 60d distribution
    dvol_z = None
    if len(dollar_vols) >= 60:
        baseline = dollar_vols[-60:-1]  # excl today
        m, s = mean(baseline), stdev(baseline) if len(baseline) > 1 else 0
        if s > 0:
            dvol_z = round((today_dvol - m) / s, 2)

    # AUM proxy
    aum_b = None
    if info and info.get("market_cap"):
        aum_b = round(info["market_cap"] / 1e9, 2)
    elif info and info.get("share_class_shares_outstanding") and latest_close:
        aum_b = round((info["share_class_shares_outstanding"] * latest_close) / 1e9, 2)

    return {
        "ticker": ticker,
        "category": category,
        "name": (info or {}).get("name", ""),
        "latest_close": latest_close,
        "aum_b": aum_b,
        "return_1d_pct": r1d,
        "return_5d_pct": r5d,
        "return_20d_pct": r20d,
        "today_dollar_vol_b": round(today_dvol / 1e9, 3),
        "avg_5d_dollar_vol_b": round(dvol_5d / 1e9, 3) if dvol_5d else None,
        "avg_20d_dollar_vol_b": round(dvol_20d / 1e9, 3) if dvol_20d else None,
        "avg_60d_dollar_vol_b": round(dvol_60d / 1e9, 3) if dvol_60d else None,
        "dvol_z_score": dvol_z,
        "dvol_5d_vs_20d_pct": (
            round(((dvol_5d - dvol_20d) / dvol_20d) * 100, 1)
            if dvol_5d and dvol_20d and dvol_20d > 0 else None
        ),
    }


def classify_flow_signal(etf):
    """
    Classify based on z-score + return direction.
    Flow signal interpretations:
      - HEAVY_INFLOW: z > 2 + price up (real buying, accumulation)
      - HEAVY_OUTFLOW: z > 2 + price down (forced selling, capitulation)
      - UNUSUAL_VOL: |z| > 2 + flat price (rotation or block trades)
      - ROTATION_IN: dvol_5d > dvol_20d by 25%+ + return positive
      - ROTATION_OUT: dvol_5d > dvol_20d by 25%+ + return negative
      - QUIET: low z, no notable flow
    """
    z = etf.get("dvol_z_score")
    r1d = etf.get("return_1d_pct") or 0
    dvol_change = etf.get("dvol_5d_vs_20d_pct") or 0
    if z and z > 2 and r1d > 0.5:
        return "HEAVY_INFLOW"
    if z and z > 2 and r1d < -0.5:
        return "HEAVY_OUTFLOW"
    if z and abs(z) > 2:
        return "UNUSUAL_VOL"
    if dvol_change > 25 and r1d > 0:
        return "ROTATION_IN"
    if dvol_change > 25 and r1d < 0:
        return "ROTATION_OUT"
    return "QUIET"


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[etf-flows] start — {len(ALL_ETFS)} ETFs across {len(ETF_CATEGORIES)} categories")

    by_etf = {}

    def task(ticker):
        for cat, ets in ETF_CATEGORIES.items():
            if ticker in ets:
                category = cat
                break
        return analyze_etf(ticker, category)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(task, t): t for t in ALL_ETFS}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                res["flow_signal"] = classify_flow_signal(res)
                by_etf[res["ticker"]] = res

    print(f"[etf-flows] analyzed {len(by_etf)}/{len(ALL_ETFS)} ETFs")

    # Aggregate by category — total $ volume + z-score weighted
    by_category = {}
    for cat, ets in ETF_CATEGORIES.items():
        cat_etfs = [by_etf[t] for t in ets if t in by_etf]
        if not cat_etfs:
            continue
        total_aum = sum((e.get("aum_b") or 0) for e in cat_etfs)
        total_today_dvol = sum((e.get("today_dollar_vol_b") or 0) for e in cat_etfs)
        avg_z = mean([e.get("dvol_z_score") for e in cat_etfs if e.get("dvol_z_score") is not None] or [0])
        avg_r1d = mean([e.get("return_1d_pct") for e in cat_etfs if e.get("return_1d_pct") is not None] or [0])
        # Aggregate signal
        if avg_z > 1 and avg_r1d > 0:
            cat_sig = "BULLISH_INFLOW"
        elif avg_z > 1 and avg_r1d < 0:
            cat_sig = "BEARISH_OUTFLOW"
        elif abs(avg_z) > 1.5:
            cat_sig = "ELEVATED_ACTIVITY"
        else:
            cat_sig = "NORMAL"
        by_category[cat] = {
            "category": cat,
            "n_etfs": len(cat_etfs),
            "total_aum_b": round(total_aum, 2),
            "total_today_dollar_vol_b": round(total_today_dvol, 3),
            "avg_dvol_z": round(avg_z, 2),
            "avg_return_1d_pct": round(avg_r1d, 2),
            "category_signal": cat_sig,
        }

    # Top notables
    heavy_inflow = [e for e in by_etf.values() if e.get("flow_signal") == "HEAVY_INFLOW"]
    heavy_outflow = [e for e in by_etf.values() if e.get("flow_signal") == "HEAVY_OUTFLOW"]
    unusual = [e for e in by_etf.values() if e.get("flow_signal") == "UNUSUAL_VOL"]
    rotation_in = [e for e in by_etf.values() if e.get("flow_signal") == "ROTATION_IN"]
    rotation_out = [e for e in by_etf.values() if e.get("flow_signal") == "ROTATION_OUT"]

    for L in (heavy_inflow, heavy_outflow, unusual):
        L.sort(key=lambda e: -(e.get("dvol_z_score") or 0))
    for L in (rotation_in, rotation_out):
        L.sort(key=lambda e: -(abs(e.get("dvol_5d_vs_20d_pct") or 0)))

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_etfs_analyzed": len(by_etf),
        "by_etf": by_etf,
        "by_category": by_category,
        "heavy_inflow": heavy_inflow[:10],
        "heavy_outflow": heavy_outflow[:10],
        "unusual_vol": unusual[:10],
        "rotation_in": rotation_in[:10],
        "rotation_out": rotation_out[:10],
        "duration_s": round(time.time() - started, 2),
        "data_sources": {
            "aggs": "Polygon /v2/aggs daily bars (price + volume)",
            "ticker_meta": "Polygon /v3/reference/tickers (shares outstanding)",
        },
        "signal_definitions": {
            "HEAVY_INFLOW": "$ volume z>2 + price up (accumulation)",
            "HEAVY_OUTFLOW": "$ volume z>2 + price down (capitulation/forced selling)",
            "UNUSUAL_VOL": "$ volume |z|>2 with flat price (rotation/block trades)",
            "ROTATION_IN": "5d vs 20d $ vol up 25%+ + price up",
            "ROTATION_OUT": "5d vs 20d $ vol up 25%+ + price down",
            "QUIET": "no notable flow signal",
        },
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET,
        Key=KEY,
        Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=900",
    )
    print(f"[etf-flows] wrote s3://{BUCKET}/{KEY} — {len(body):,}b in {out['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_etfs": len(by_etf),
            "n_heavy_inflow": len(heavy_inflow),
            "n_heavy_outflow": len(heavy_outflow),
            "n_unusual": len(unusual),
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
