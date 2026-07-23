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


# ────────────────────────── FINRA consolidated short interest ──────────────────────────
def fetch_finra_short_interest(symbols):
    """Official bi-monthly consolidated short interest from FINRA (free, no auth).

    REPLACES the dead Polygon stocks/v1/short-interest endpoint. On the current
    entitlement Polygon returns only stale 2017-18 snapshots and ignores the
    order/sort params, so the prior 180-day freshness guard correctly dropped
    ~everything — leaving days_to_cover / si_change_pct effectively None and the
    SQUEEZE_RISK / HIGH_DAYS_TO_COVER classifications (which need real DTC) inert.

    FINRA's consolidatedShortInterest gives the current settlement directly and
    is in fact richer than Polygon ever was: official daysToCoverQuantity and an
    official change-vs-prior-settlement, no client-side computation needed.

    One bulk POST per page (dateRangeFilter on settlementDate, NO sortFields —
    FINRA rejects sorting unless partition keys are EQUAL-filtered), indexed to
    the watchlist. Returns {sym: {settlement_date, short_interest,
    avg_daily_volume, days_to_cover, prev_short_interest, si_change_pct}}.
    """
    from datetime import date as _date
    url = "https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest"
    end = _date.today()
    start = end - timedelta(days=30)
    want = set(symbols)
    out = {}
    offset = 0
    for _ in range(6):  # paginate the full latest-settlement snapshot (~22k rows)
        payload = {
            "limit": 5000, "offset": offset,
            "dateRangeFilters": [{"fieldName": "settlementDate",
                                  "startDate": start.isoformat(),
                                  "endDate": end.isoformat()}],
        }
        try:
            req = urllib.request.Request(
                url, data=json.dumps(payload).encode(),
                headers={"User-Agent": "JustHodl Research raafouis@gmail.com",
                         "Content-Type": "application/json",
                         "Accept": "application/json"},
                method="POST")
            with urllib.request.urlopen(req, timeout=30) as r:
                rows = json.loads(r.read())
        except Exception as e:
            print(f"[short-interest] FINRA SI fetch fail @offset {offset}: {type(e).__name__}: {e}")
            break
        if isinstance(rows, dict):
            rows = rows.get("data") or rows.get("results") or []
        if not rows:
            break
        for r in rows:
            sym = (r.get("symbolCode") or "").upper().strip()
            if sym not in want:
                continue
            sd = r.get("settlementDate")
            prev = out.get(sym)
            if prev is None or (sd and sd >= (prev.get("settlement_date") or "")):
                out[sym] = {
                    "settlement_date": sd,
                    "short_interest": r.get("currentShortPositionQuantity"),
                    "avg_daily_volume": r.get("averageDailyVolumeQuantity"),
                    "days_to_cover": r.get("daysToCoverQuantity"),
                    "prev_short_interest": r.get("previousShortPositionQuantity"),
                    "si_change_pct": r.get("changePercent"),
                }
        if len(rows) < 5000:
            break
        offset += 5000
    print(f"[short-interest] FINRA SI: {len(out)}/{len(want)} watchlist names from official settlement")
    return out


# ─────────────── canary #19: price over the SI settlement window ───────────
def fetch_price_over_window(symbols, si_data):
    """For SI-collapse-on-FLAT-PRICE (#19): fetch each name's price change over
    the SAME window as the short-interest change — settlement date vs the prior
    settlement (~2 weeks). The canary's whole thesis is that short interest
    dropping WHILE PRICE STAYS FLAT is a different, higher-signal event than
    covering into a rising price: shorts are exiting on a catalyst they see
    (forced buy-in, index event, borrow-cost spike), not because the thesis
    broke. Without a price leg the engine cannot tell the two apart.

    One Polygon aggregates call per name over the window; returns
    {sym: {price_start, price_end, price_change_pct, window_days}}.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    out = {}
    # widest span: prior settlement is ~15d before current; pad to 40d and take
    # first vs last close in the window as the move over that period
    def one(sym):
        si = si_data.get(sym) or {}
        sd = si.get("settlement_date")
        if not sd:
            return sym, None
        try:
            end = datetime.strptime(sd[:10], "%Y-%m-%d").date()
        except Exception:
            return sym, None
        start = end - timedelta(days=40)
        url = ("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s"
               "?adjusted=true&sort=asc&limit=120&apiKey=%s"
               % (sym, start.isoformat(), end.isoformat(), POLYGON_KEY))
        try:
            j = http_get(url, timeout=15)
            res = (j or {}).get("results") or []
            closes = [r.get("c") for r in res if r.get("c")]
            if len(closes) < 5:
                return sym, None
            p0, p1 = closes[0], closes[-1]
            if not p0:
                return sym, None
            return sym, {"price_start": round(p0, 2), "price_end": round(p1, 2),
                         "price_change_pct": round(100 * (p1 / p0 - 1), 1),
                         "window_days": (end - start).days}
        except Exception:
            return sym, None

    # only names that actually have an SI change worth pricing
    live = [s for s in symbols if (si_data.get(s) or {}).get("si_change_pct") is not None]
    with ThreadPoolExecutor(max_workers=8) as ex:
        for fut in as_completed([ex.submit(one, s) for s in live]):
            sym, rec = fut.result()
            if rec:
                out[sym] = rec
    print("[short-interest] price-window join: %d/%d names priced" % (len(out), len(live)))
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


def classify_signal(short_pct_now, trend_pct, days_to_cover, si_change_pct,
                    price_change_pct=None):
    """
    Classify the short positioning signal:
      - SI_COLLAPSE_FLAT_PRICE (#19): short interest dropping HARD while price
        barely moves — the highest-signal covering variant. Shorts are exiting
        on a catalyst they see coming, not because price forced them out.
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

    # canary #19: the collapse-on-flat-price tell fires FIRST because it is the
    # most specific and most actionable. Requires a real SI drop (>=10%) AND a
    # price that stayed within a flat band (|move| <= 4% over the window).
    si_collapse = si_change_pct is not None and si_change_pct <= -10
    flat_price = (price_change_pct is not None
                  and abs(price_change_pct) <= 4.0)
    if si_collapse and flat_price:
        # sharper drop + more DTC to unwind = higher conviction
        sc = 82 + min(12, int(abs(si_change_pct) / 5))
        if high_dtc:
            sc = min(97, sc + 5)
        return "SI_COLLAPSE_FLAT_PRICE", sc

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

    # 2. Official short interest snapshot — FINRA consolidated (replaces dead Polygon)
    si_data = fetch_finra_short_interest(WATCHLIST_SET)
    print(f"[short-interest] FINRA SI: {len(si_data)} tickers w/ official short interest")

    # 2b. canary #19: price change over the SI settlement window
    price_data = fetch_price_over_window(WATCHLIST, si_data)

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
        pw = price_data.get(sym) or {}
        price_change = pw.get("price_change_pct")
        signal, score = classify_signal(latest_pct, trend_pct, days_to_cover,
                                        si_change, price_change)
        by_ticker[sym] = {
            "ticker": sym,
            "latest_short_pct": latest_pct,
            "trend_pct": trend_pct,
            "recent_5d_avg": r_avg,
            "prior_9d_avg": p_avg,
            "days_to_cover": days_to_cover,
            "si_change_pct": si_change,
            "price_change_pct": price_change,
            "price_window_days": pw.get("window_days"),
            "short_interest": (si or {}).get("short_interest"),
            "settlement_date": (si or {}).get("settlement_date"),
            "signal": signal,
            "score": score,
            "n_days_volume_data": len(h),
        }

    # ── Finviz whole-market short-float overlay ──
    # Keeps data/short-interest.json fresh fleet-wide even when FINRA/Polygon are
    # sparse or frozen. Consumers read latest_short_pct at runtime, so this single
    # producer fix refreshes every short-interest consumer with zero redeploys.
    try:
        import finviz as FV
        fvs = FV.load_short()
        n_new = n_enr = 0
        for tk, d in fvs.items():
            sf = d.get("short_float_pct")
            if sf is None:
                continue
            rec = by_ticker.get(tk)
            if rec is None:
                by_ticker[tk] = {
                    "ticker": tk, "latest_short_pct": sf, "short_float_pct": sf,
                    "days_to_cover": d.get("short_ratio"),
                    "float_shares": d.get("float_shares"),
                    "rel_volume": d.get("rel_volume"),
                    "short_src": "finviz", "signal": "NEUTRAL", "score": 0,
                }
                n_new += 1
            else:
                rec["short_float_pct"] = sf
                rec["latest_short_pct"] = sf
                if rec.get("days_to_cover") is None:
                    rec["days_to_cover"] = d.get("short_ratio")
                rec.setdefault("float_shares", d.get("float_shares"))
                rec["short_src"] = "finviz"
                n_enr += 1
        print("[short-interest] finviz overlay: +%d new, %d enriched" % (n_new, n_enr))
    except Exception as e:
        print("[short-interest] finviz overlay fail: %s" % str(e)[:80])

    # 4. Top signals
    crowded = [v for v in by_ticker.values() if v["signal"] in ("CROWDED_SHORT_RISING", "DISTRIBUTION")]
    crowded.sort(key=lambda x: (x.get("trend_pct") or 0), reverse=True)
    squeeze_risk = [v for v in by_ticker.values() if v["signal"] == "SQUEEZE_RISK"]
    squeeze_risk.sort(key=lambda x: -(x.get("days_to_cover") or 0))
    high_dtc = [v for v in by_ticker.values() if v["signal"] == "HIGH_DAYS_TO_COVER"]
    high_dtc.sort(key=lambda x: -(x.get("days_to_cover") or 0))
    covering = [v for v in by_ticker.values() if v["signal"] == "COVERING"]
    covering.sort(key=lambda x: (x.get("trend_pct") or 0))
    # canary #19 board — the highest-signal covering variant
    si_collapse = [v for v in by_ticker.values()
                   if v["signal"] == "SI_COLLAPSE_FLAT_PRICE"]
    si_collapse.sort(key=lambda x: (x.get("si_change_pct") or 0))  # most negative first

    out = {
        "version": "1.1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "watchlist_size": len(WATCHLIST),
        "n_tickers_with_data": len(by_ticker),
        "n_tickers_finra": len(history),
        "n_tickers_polygon": len(si_data),
        "n_tickers_short_interest": len(si_data),
        "n_tickers_priced": len(price_data),
        "by_ticker": by_ticker,
        "top_crowded_shorts": crowded[:15],
        "top_squeeze_risk": squeeze_risk[:10],
        "top_high_dtc": high_dtc[:10],
        "top_covering": covering[:10],
        "top_si_collapse_flat_price": si_collapse[:12],
        "duration_s": round(time.time() - started, 2),
        "data_sources": {
            "short_volume": "FINRA daily RegSHO files (free)",
            "short_interest": "FINRA Consolidated Short Interest API (official bi-monthly settlement; replaced dead Polygon feed)",
            "short_float": "Finviz Elite whole-market short float (fresh, primary for latest_short_pct)",
            "price_window": "Polygon daily aggregates over the SI settlement window (canary #19 flat-price leg)",
        },
        "signal_definitions": {
            "SI_COLLAPSE_FLAT_PRICE": "short interest down >=10% while price stayed flat (|move|<=4%) over the settlement window — shorts covering on a catalyst they see, not on price (canary #19)",
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
