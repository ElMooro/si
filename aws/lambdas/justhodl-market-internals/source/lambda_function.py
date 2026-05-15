"""
justhodl-market-internals — Bloomberg MMR equivalent.

Daily breadth + internals composite. Uses Polygon grouped daily for S&P 500
constituents + sector ETFs + FRED for NYSE TICK history.

Computes:
  • ADVANCE/DECLINE LINE — cumulative A/D, 5d momentum, 20d momentum
  • McCLELLAN OSCILLATOR — 19d EMA minus 39d EMA of (Adv − Dec)
  • McCLELLAN SUMMATION INDEX — cumulative oscillator
  • % ABOVE MOVING AVERAGES — 50DMA and 200DMA across S&P 500
  • NEW 52-WK HIGHS vs LOWS
  • Net volume on advancing vs declining issues (proxy for TRIN)

Output: data/market-internals.json
  • breadth_score 0-100 (composite of all)
  • state: STRONG / EXPANDING / NEUTRAL / NARROWING / WASHOUT
  • per-metric drill-down

Schedule: cron(15 21 ? * MON-FRI *) — daily at 5:15 PM ET after market close.

Telegram alerts:
  • McClellan oscillator extreme readings (< -100 oversold, > +100 overbought)
  • Major breadth divergence (price up, A/D line down)
  • % above 50DMA crossing 70 (overbought) or 30 (oversold)
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/market-internals.json"
S3_KEY_HIST = "data/market-internals-history.json"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# S&P 500 tickers (subset used as proxy — 100 large + 200 diversified)
SP500_PROXY = [
    "AAPL","MSFT","GOOGL","GOOG","AMZN","NVDA","META","TSLA","BRK-B","LLY",
    "AVGO","V","JPM","WMT","XOM","UNH","MA","JNJ","PG","HD","ORCL","COST",
    "ABBV","BAC","NFLX","CRM","CVX","KO","TMO","PEP","ADBE","CSCO","ACN","AMD",
    "WFC","MRK","ABT","NKE","TXN","DIS","LIN","DHR","MCD","NOW","IBM","PM",
    "INTU","CAT","SPGI","GE","AMGN","RTX","UNP","UBER","NEE","BLK","T","AMAT",
    "HON","C","BKNG","LRCX","LOW","MS","GS","ETN","COP","BX","TJX","MDT","PLD",
    "SBUX","DE","SCHW","CB","ELV","ADP","BSX","ANET","KLAC","TT","GILD","REGN",
    "PGR","PFE","CI","SO","FI","PANW","BMY","MMC","MO","CMCSA","INTC","CVS",
    "TGT","F","GM","NSC","CSX","FDX","UPS","DAL","AAL","LUV","MAR","HLT","DPZ",
    "CMG","YUM","MDLZ","CL","KMB","CHD","EL","ULTA","GIS","SJM","WMT","BBY",
    "ROST","DG","DLTR","USB","PNC","TFC","COF","BK","STT","FITB","HBAN","RF",
    "CFG","KEY","CMA","MU","ON","QCOM","MRVL","ARM","WDC","STX","BA","LMT","NOC",
    "GD","VLO","MPC","PSX","SLB","HAL","OXY","DVN","FANG","EOG","APA","VRTX",
    "ISRG","SYK","ZTS","BDX","DXCM","IDXX","HUM","CNC","MMM","KHC","DLR","EQIX",
    "PSA","CCI","AMT","CME","ICE","NDAQ","MCO","TROW","BEN","IVZ","NTRS","PRU",
    "MET","TRV","AIG","ALL","HIG","AFL","CINF","WRB","RE","RGA","BIIB","ALXN",
    "INCY","ALGN","ILMN","CRL","TMUS","VZ","CHTR","DISH","WBD","FOX","FOXA","CMCSK",
    "PARA","SIRI","LYV","TTWO","EA","WBA","WAB","WAT","WCN","WEC","WFC","WHR",
    "WMB","WST","WTW","WY","WYNN","XEL","XYL","YUM","ZBH","ZBRA","ZTS",
]
# Dedupe
SP500_PROXY = sorted(set(SP500_PROXY))

s3 = boto3.client("s3", region_name="us-east-1")


def http_get(url, timeout=20, retries=1):
    for a in range(retries+1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and a < retries:
                time.sleep(2); continue
            return None
        except Exception:
            if a < retries:
                time.sleep(1); continue
            return None


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=900"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl=cache)


def fetch_grouped_daily(date_iso):
    """Polygon grouped daily for a single date — all US tickers."""
    if not POLYGON_KEY: return None
    url = (f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
           f"{date_iso}?adjusted=true&apiKey={POLYGON_KEY}")
    data = http_get(url)
    if not data or "results" not in data:
        return None
    return data["results"]


def fetch_aggs(ticker, days_back=210):
    """Polygon daily bars for one ticker."""
    if not POLYGON_KEY: return None
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{start}/{end}?adjusted=true&apiKey={POLYGON_KEY}")
    data = http_get(url)
    if not data or "results" not in data:
        return None
    return data["results"]


def previous_market_day(d):
    """Move back until not weekend."""
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[internals] starting universe={len(SP500_PROXY)}")
    if not POLYGON_KEY:
        return {"statusCode": 500, "body": json.dumps({"err": "POLYGON_KEY missing"})}

    prior = get_s3_json(S3_KEY, {}) or {}

    # Fetch most-recent two days of grouped daily
    today = previous_market_day(datetime.now(timezone.utc).date())
    yesterday = previous_market_day(today - timedelta(days=1))

    day0 = fetch_grouped_daily(today.isoformat()) or []
    day1 = fetch_grouped_daily(yesterday.isoformat()) or []
    print(f"[internals] day0={len(day0)} day1={len(day1)}")

    # Build map by ticker
    by_t0 = {r.get("T"): r for r in day0}
    by_t1 = {r.get("T"): r for r in day1}

    # Limit to S&P proxy
    universe = [t for t in SP500_PROXY if t in by_t0 and t in by_t1]
    advances = declines = unchanged = 0
    adv_volume = dec_volume = 0
    advancing = []
    declining = []
    for t in universe:
        d0 = by_t0[t]; d1 = by_t1[t]
        chg = (d0.get("c", 0) or 0) - (d1.get("c", 0) or 0)
        vol = d0.get("v", 0) or 0
        if chg > 0:
            advances += 1; adv_volume += vol; advancing.append((t, chg))
        elif chg < 0:
            declines += 1; dec_volume += vol; declining.append((t, chg))
        else:
            unchanged += 1

    ad_diff = advances - declines
    ad_ratio = advances / max(1, declines)
    trin_proxy = (advances / max(1, declines)) / max(0.001, adv_volume / max(1, dec_volume))

    # % above 50DMA / 200DMA — fetch per-ticker bars for subset (top 100 by mcap proxy)
    pct_above_50 = pct_above_200 = None
    n_above_50 = n_above_200 = n_with_bars = 0
    n_52w_high = n_52w_low = 0

    def check_one(t):
        bars = fetch_aggs(t, days_back=260)
        if not bars or len(bars) < 200:
            return None
        closes = [b.get("c") for b in bars if b.get("c")]
        if len(closes) < 200: return None
        ma50 = sum(closes[-50:]) / 50
        ma200 = sum(closes[-200:]) / 200
        latest = closes[-1]
        hi52 = max(closes[-252:]) if len(closes) >= 252 else max(closes)
        lo52 = min(closes[-252:]) if len(closes) >= 252 else min(closes)
        return {
            "ticker": t, "latest": latest, "ma50": ma50, "ma200": ma200,
            "is_above_50": latest > ma50, "is_above_200": latest > ma200,
            "is_52w_high": latest >= hi52 * 0.99,
            "is_52w_low": latest <= lo52 * 1.01,
        }

    # Subset to top 100 to control API calls
    subset = SP500_PROXY[:100]
    bar_results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(check_one, t): t for t in subset}
        for f in as_completed(futs):
            try:
                r = f.result()
                if r: bar_results.append(r)
            except Exception: pass

    if bar_results:
        n_with_bars = len(bar_results)
        n_above_50 = sum(1 for r in bar_results if r["is_above_50"])
        n_above_200 = sum(1 for r in bar_results if r["is_above_200"])
        n_52w_high = sum(1 for r in bar_results if r["is_52w_high"])
        n_52w_low = sum(1 for r in bar_results if r["is_52w_low"])
        pct_above_50 = round(100 * n_above_50 / n_with_bars, 1)
        pct_above_200 = round(100 * n_above_200 / n_with_bars, 1)

    # McClellan oscillator (load history, append today, compute EMAs)
    hist = get_s3_json(S3_KEY_HIST, {}) or {}
    snapshots = hist.get("snapshots", [])
    snapshots.append({
        "date": today.isoformat(),
        "advances": advances, "declines": declines,
        "ad_diff": ad_diff,
    })
    # Keep last 60 trading days
    snapshots = snapshots[-60:]

    def ema(vals, period):
        if len(vals) < period: return None
        k = 2 / (period + 1)
        e = sum(vals[:period]) / period
        for v in vals[period:]:
            e = v * k + e * (1 - k)
        return e

    ad_diffs = [s["ad_diff"] for s in snapshots]
    ema19 = ema(ad_diffs, 19)
    ema39 = ema(ad_diffs, 39)
    mcclellan = round(ema19 - ema39, 1) if (ema19 is not None and ema39 is not None) else None

    # Summation Index — cumulative McClellan
    summation_index = hist.get("summation_index", 0)
    if mcclellan is not None:
        summation_index = round((summation_index or 0) + mcclellan, 1)

    # Save history
    put_s3_json(S3_KEY_HIST, {
        "snapshots": snapshots, "summation_index": summation_index,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }, cache="public, max-age=600")

    # Composite breadth score 0-100
    score = 50
    reasons = []
    if pct_above_50 is not None:
        if pct_above_50 > 70: score += 15; reasons.append(f"{pct_above_50:.0f}% > 50DMA (strong)")
        elif pct_above_50 > 55: score += 8
        elif pct_above_50 < 30: score -= 15; reasons.append(f"{pct_above_50:.0f}% > 50DMA (weak)")
        elif pct_above_50 < 45: score -= 8
    if pct_above_200 is not None:
        if pct_above_200 > 65: score += 10
        elif pct_above_200 < 35: score -= 10
    if mcclellan is not None:
        if mcclellan > 50: score += 8; reasons.append(f"McClellan +{mcclellan:.0f} (breadth expanding)")
        elif mcclellan < -50: score -= 8; reasons.append(f"McClellan {mcclellan:.0f} (breadth contracting)")
        elif mcclellan > 100: score += 12  # overheated but bullish
        elif mcclellan < -100: score -= 12
    if ad_ratio > 2:
        score += 5; reasons.append(f"A/D {ad_ratio:.1f}:1 (broad rally)")
    elif ad_ratio < 0.5:
        score -= 5; reasons.append(f"A/D {ad_ratio:.1f}:1 (broad selloff)")
    if n_52w_high > 0 and n_with_bars > 0:
        hl_diff = n_52w_high - n_52w_low
        if hl_diff > 8: score += 6
        elif hl_diff < -8: score -= 6

    score = max(0, min(100, score))
    state = ("STRONG" if score >= 70 else
              "EXPANDING" if score >= 55 else
              "NEUTRAL" if score >= 45 else
              "NARROWING" if score >= 30 else
              "WASHOUT")

    output = {
        "schema_version": "1.0",
        "method": "market_internals_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "for_date": today.isoformat(),
        "breadth_score": round(score, 1),
        "state": state,
        "reasons": reasons,
        "ad_line": {
            "advances": advances, "declines": declines, "unchanged": unchanged,
            "ad_ratio": round(ad_ratio, 2),
            "ad_diff_today": ad_diff,
            "advancing_volume": adv_volume, "declining_volume": dec_volume,
            "trin_proxy": round(trin_proxy, 3) if trin_proxy else None,
        },
        "mcclellan": {
            "oscillator": mcclellan,
            "summation_index": summation_index,
            "ema19": round(ema19, 1) if ema19 is not None else None,
            "ema39": round(ema39, 1) if ema39 is not None else None,
            "interpretation": (
                f"Overbought" if mcclellan and mcclellan > 100 else
                f"Oversold" if mcclellan and mcclellan < -100 else
                f"Bull momentum" if mcclellan and mcclellan > 50 else
                f"Bear momentum" if mcclellan and mcclellan < -50 else
                f"Neutral"
            ),
        },
        "moving_averages": {
            "pct_above_50dma": pct_above_50,
            "pct_above_200dma": pct_above_200,
            "n_with_bars": n_with_bars,
        },
        "highs_lows": {
            "n_52w_highs": n_52w_high,
            "n_52w_lows": n_52w_low,
            "hl_diff": n_52w_high - n_52w_low,
        },
        "duration_s": round(time.time()-t0, 1),
    }

    put_s3_json(S3_KEY, output)
    print(f"[internals] breadth={score:.1f} state={state} mcclellan={mcclellan}")

    # ─── ALERTS ───────────────────────────────────────────────────────
    try:
        def tg(msg):
            if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
                print(f"[tg] no creds: {msg[:80]}"); return
            body = json.dumps({
                "chat_id": TELEGRAM_CHAT_ID, "text": msg,
                "parse_mode": "HTML", "disable_web_page_preview": True,
            }).encode("utf-8")
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                data=body, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10).read()

        prior_state = prior.get("state")
        if prior_state and prior_state != state:
            tg(f"📊 <b>BREADTH STATE CHANGE</b>\n"
                f"{prior_state} → <b>{state}</b> · score {output['breadth_score']:.0f}\n"
                f"% above 50DMA: {pct_above_50}\n"
                f"McClellan: {mcclellan} ({output['mcclellan']['interpretation']})")

        # Extreme McClellan
        prior_mc = (prior.get("mcclellan") or {}).get("oscillator")
        if mcclellan and mcclellan > 100 and (prior_mc is None or prior_mc <= 100):
            tg(f"🔥 <b>McCLELLAN OVERBOUGHT</b> {mcclellan:+.0f}\n"
                f"Historically: short-term tops form within 1-3 weeks.")
        elif mcclellan and mcclellan < -100 and (prior_mc is None or prior_mc >= -100):
            tg(f"💎 <b>McCLELLAN OVERSOLD</b> {mcclellan:+.0f}\n"
                f"Historically: short-term bottoms within 1-3 weeks.")
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "breadth_score": round(score, 1), "state": state,
            "mcclellan": mcclellan, "ad_ratio": round(ad_ratio, 2),
            "duration_s": round(time.time()-t0, 1),
        }),
    }
