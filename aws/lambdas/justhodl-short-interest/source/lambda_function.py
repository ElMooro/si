"""
justhodl-short-interest — Short positioning tracker

Tracks two complementary short metrics:
  1. SHORT VOLUME (FINRA daily files, free)
     - What % of each day's trading volume was short-flagged
     - Computed for last 14 trading days per ticker
     - Trend = recent 5d avg vs prior 9d avg (positive = increasing pressure)

  2. SHORT INTEREST (Polygon, bi-monthly snapshot)
     - Days-to-cover (short_interest / avg_daily_volume)
     - Short interest as % of float
     - Settlement-date snapshots from FINRA

Output: data/short-interest.json

Key signals:
  - SQUEEZE_RISK: high short interest + price rising + short volume falling
  - DISTRIBUTION: rising short volume + falling price (real selling)
  - CROWDED_SHORT: high % short interest + rising short volume
  - COVERING: falling short interest + falling short volume (bearish bet unwinding)
"""
import json
import os
import time
import boto3
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from io import BytesIO
import gzip

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/short-interest.json"

POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

# Watchlist — same 165 tickers as earnings tracker for consistency
WATCHLIST = [
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AVGO",
    "BRK-B", "TSM", "JPM", "WMT", "LLY", "V", "MA", "ORCL", "XOM", "UNH", "JNJ",
    "HD", "COST", "BAC", "PG", "ABBV", "NFLX", "CVX", "MRK", "KO", "AMD",
    "ADBE", "PEP", "CRM", "PM", "TMO", "LIN", "MCD", "ACN", "GE", "ABT",
    "CSCO", "WFC", "DHR", "AXP", "DIS", "VZ", "INTU", "MS", "T", "RTX",
    "AMGN", "GS", "IBM", "PFE", "QCOM", "BX", "ISRG", "TMUS", "CAT", "NOW",
    "AMAT", "BLK", "LOW", "ELV", "SCHW", "SPGI", "DE", "NKE", "C", "BKNG",
    "PLD", "SYK", "BSX", "PANW", "ETN", "MDT", "KKR", "ADP", "MMC", "REGN",
    "MU", "GILD", "VRTX", "FI", "LMT", "TJX", "INTC", "ADI", "CB", "AMT",
    "PYPL", "MO", "CI", "BA", "CME", "SHW", "ZTS", "EQIX", "HCA", "ICE",
    # High-velocity / squeeze candidates
    "PLTR", "COIN", "MARA", "RIOT", "CLSK", "SOFI", "RBLX", "U", "NET",
    "SNOW", "DDOG", "CRWD", "ZS", "OKTA", "DOCU", "SHOP", "MELI", "PDD", "NU",
    "ABNB", "DASH", "RIVN", "LCID", "F", "GM", "STLA", "TM", "HMC", "UBER",
    "LYFT", "SQ", "AFRM", "HOOD", "RDDT", "DJT", "TGT", "DLTR",
    "CVS", "WBD", "PARA", "ROKU", "SPOT", "FDX", "UPS",
    "EMR", "ITW", "SLB", "EOG", "OXY", "FANG", "MPC", "VLO", "PSX",
    # Common squeeze names
    "GME", "AMC", "BBBY", "BB", "NOK",
]

WATCHLIST_SET = set(WATCHLIST)


def http_get(url, timeout=20, raw=False):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; justhodl-short-interest/1.0)",
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        if raw:
            return body
        try:
            return json.loads(body)
        except Exception:
            return body.decode("utf-8", errors="replace")


# ────────────────────────── FINRA daily short volume ──────────────────────────
def fetch_finra_short_volume(date):
    """Fetch FINRA daily short volume file for a given date."""
    yyyymmdd = date.strftime("%Y%m%d")
    url = f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{yyyymmdd}.txt"
    try:
        body = http_get(url, timeout=20, raw=True).decode("utf-8", errors="replace")
        # Pipe-delimited: Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
        rows = {}
        n_rows_seen = 0
        n_rows_matched = 0
        for line in body.splitlines()[1:]:  # skip header
            n_rows_seen += 1
            parts = line.split("|")
            if len(parts) < 5:
                continue
            sym = parts[1].strip().upper()
            try:
                short_vol = float(parts[2])
                total_vol = float(parts[4])
                if total_vol > 0 and sym in WATCHLIST_SET:
                    rows[sym] = {
                        "short_vol": int(short_vol),
                        "total_vol": int(total_vol),
                        "short_pct": round(short_vol / total_vol * 100, 2),
                    }
                    n_rows_matched += 1
            except Exception:
                continue
        print(f"[short-interest] finra {yyyymmdd}: body={len(body):,}b parsed={n_rows_seen} matched={n_rows_matched}")
        return rows
    except Exception as e:
        print(f"[short-interest] finra {yyyymmdd} fetch fail: {type(e).__name__}: {e}")
        return {}


def collect_short_volume_history(days_back=14):
    """Collect last N trading days of FINRA short volume."""
    history = {}  # ticker -> [{"date": ..., "short_pct": ...}, ...]
    today = datetime.now(timezone.utc).date()
    # Walk back further to skip weekends + handle file lag
    days_collected = 0
    days_offset = 1  # FINRA files lag by 1 trading day
    while days_collected < days_back and days_offset < days_back + 14:
        d = today - timedelta(days=days_offset)
        days_offset += 1
        if d.weekday() >= 5:  # skip weekends
            continue
        rows = fetch_finra_short_volume(d)
        if not rows:
            continue
        for sym, vals in rows.items():
            history.setdefault(sym, []).append({
                "date": d.isoformat(),
                "short_pct": vals["short_pct"],
                "short_vol": vals["short_vol"],
                "total_vol": vals["total_vol"],
            })
        days_collected += 1
        time.sleep(0.1)
    # Sort each ticker's history newest→oldest
    for sym in history:
        history[sym].sort(key=lambda x: x["date"], reverse=True)
    return history


# ────────────────────────── Polygon short interest snapshot ──────────────────────────
def fetch_polygon_short_interest(ticker):
    """Get latest short interest snapshot from Polygon."""
    url = f"https://api.polygon.io/stocks/v1/short-interest?ticker={urllib.parse.quote(ticker)}&limit=2&apiKey={POLYGON_KEY}"
    try:
        d = http_get(url, timeout=12)
        results = d.get("results") or []
        if not results:
            return None
        latest = results[0]
        prev = results[1] if len(results) > 1 else None
        si = latest.get("short_interest")
        avg_vol = latest.get("avg_daily_volume")
        days_to_cover = None
        if si and avg_vol and avg_vol > 0:
            days_to_cover = round(si / avg_vol, 2)
        si_change = None
        if prev and prev.get("short_interest") and si:
            si_change = round(((si - prev["short_interest"]) / prev["short_interest"]) * 100, 2)
        return {
            "settlement_date": latest.get("settlement_date"),
            "short_interest": si,
            "avg_daily_volume": avg_vol,
            "days_to_cover": days_to_cover,
            "prev_settlement": prev.get("settlement_date") if prev else None,
            "prev_short_interest": prev.get("short_interest") if prev else None,
            "si_change_pct": si_change,
        }
    except Exception:
        return None


def fetch_polygon_short_interest_parallel(tickers, max_workers=10):
    """Parallel fetch short interest for tickers."""
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_polygon_short_interest, t): t for t in tickers}
        for fut in as_completed(futures):
            t = futures[fut]
            res = fut.result()
            if res:
                out[t] = res
    return out


# ────────────────────────── Aggregate signals ──────────────────────────
def compute_trend(history_for_ticker):
    """
    Compute short volume trend: recent 5d avg vs prior 9d avg.
    Returns (trend_pct_change, recent_5d_avg, prior_9d_avg).
    """
    if not history_for_ticker or len(history_for_ticker) < 10:
        return None, None, None
    recent_5 = history_for_ticker[:5]
    prior_9 = history_for_ticker[5:]
    r_avg = sum(x["short_pct"] for x in recent_5) / len(recent_5)
    p_avg = sum(x["short_pct"] for x in prior_9) / len(prior_9) if prior_9 else 0
    if p_avg <= 0:
        return None, round(r_avg, 2), round(p_avg, 2)
    pct_change = round(((r_avg - p_avg) / p_avg) * 100, 2)
    return pct_change, round(r_avg, 2), round(p_avg, 2)


def classify_signal(short_pct_now, trend_pct, days_to_cover, si_change_pct):
    """
    Classify the short positioning signal:
      - SQUEEZE_RISK: high short interest + falling short volume (covering)
      - DISTRIBUTION: rising short volume (real selling pressure)
      - CROWDED_SHORT: high short volume + rising
      - COVERING: falling short interest + falling short volume
      - NEUTRAL otherwise
    """
    high_dtc = days_to_cover is not None and days_to_cover > 5
    high_short_now = short_pct_now is not None and short_pct_now > 50
    rising = trend_pct is not None and trend_pct > 5
    falling = trend_pct is not None and trend_pct < -5
    si_falling = si_change_pct is not None and si_change_pct < -5
    si_rising = si_change_pct is not None and si_change_pct > 5

    if high_dtc and falling:
        return "SQUEEZE_RISK", 80
    if rising and high_short_now:
        return "CROWDED_SHORT_RISING", 70
    if rising:
        return "DISTRIBUTION", 65
    if si_falling and falling:
        return "COVERING", 30
    if high_dtc:
        return "HIGH_DAYS_TO_COVER", 60
    return "NEUTRAL", 50


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[short-interest] start — watchlist={len(WATCHLIST)} tickers")

    # 1. FINRA daily short volume — last 14 trading days
    history = collect_short_volume_history(days_back=14)
    print(f"[short-interest] FINRA: {len(history)} tickers w/ short volume data")

    # 2. Polygon short interest snapshot
    si_data = fetch_polygon_short_interest_parallel(WATCHLIST, max_workers=10)
    print(f"[short-interest] Polygon: {len(si_data)} tickers w/ short interest snapshot")

    # 3. Combine + classify
    by_ticker = {}
    for sym in WATCHLIST:
        h = history.get(sym, [])
        si = si_data.get(sym)
        if not h and not si:
            continue
        latest_pct = h[0]["short_pct"] if h else None
        trend_pct, r_avg, p_avg = compute_trend(h)
        days_to_cover = (si or {}).get("days_to_cover")
        si_change = (si or {}).get("si_change_pct")
        signal, score = classify_signal(latest_pct, trend_pct, days_to_cover, si_change)
        by_ticker[sym] = {
            "ticker": sym,
            "latest_short_pct": latest_pct,
            "trend_pct": trend_pct,
            "recent_5d_avg": r_avg,
            "prior_9d_avg": p_avg,
            "days_to_cover": days_to_cover,
            "si_change_pct": si_change,
            "short_interest": (si or {}).get("short_interest"),
            "settlement_date": (si or {}).get("settlement_date"),
            "signal": signal,
            "score": score,
            "n_days_volume_data": len(h),
        }

    # 4. Top signals
    crowded = [v for v in by_ticker.values() if v["signal"] in ("CROWDED_SHORT_RISING", "DISTRIBUTION")]
    crowded.sort(key=lambda x: (x.get("trend_pct") or 0), reverse=True)
    squeeze_risk = [v for v in by_ticker.values() if v["signal"] == "SQUEEZE_RISK"]
    squeeze_risk.sort(key=lambda x: -(x.get("days_to_cover") or 0))
    high_dtc = [v for v in by_ticker.values() if v["signal"] == "HIGH_DAYS_TO_COVER"]
    high_dtc.sort(key=lambda x: -(x.get("days_to_cover") or 0))
    covering = [v for v in by_ticker.values() if v["signal"] == "COVERING"]
    covering.sort(key=lambda x: (x.get("trend_pct") or 0))

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "watchlist_size": len(WATCHLIST),
        "n_tickers_with_data": len(by_ticker),
        "n_tickers_finra": len(history),
        "n_tickers_polygon": len(si_data),
        "by_ticker": by_ticker,
        "top_crowded_shorts": crowded[:15],
        "top_squeeze_risk": squeeze_risk[:10],
        "top_high_dtc": high_dtc[:10],
        "top_covering": covering[:10],
        "duration_s": round(time.time() - started, 2),
        "data_sources": {
            "short_volume": "FINRA daily RegSHO files (free)",
            "short_interest": "Polygon stocks short-interest API (bi-monthly snapshots)",
        },
        "signal_definitions": {
            "SQUEEZE_RISK": "high days-to-cover + falling short volume (shorts covering)",
            "CROWDED_SHORT_RISING": "high short volume % + rising trend",
            "DISTRIBUTION": "rising short volume (real selling pressure)",
            "COVERING": "falling short interest + falling short volume",
            "HIGH_DAYS_TO_COVER": "days-to-cover > 5",
            "NEUTRAL": "no notable positioning signal",
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
    print(f"[short-interest] wrote s3://{BUCKET}/{KEY} — {len(body):,}b in {out['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_tickers": len(by_ticker),
            "n_squeeze_risk": len(squeeze_risk),
            "n_crowded": len(crowded),
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
