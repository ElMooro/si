"""
justhodl-finra-short — FINRA DAILY SHORT VOLUME & SQUEEZE-SETUP ENGINE
═════════════════════════════════════════════════════════════════════════════
Tracks short-selling activity daily for the full US equity universe using
FINRA's free Reg SHO Daily Short Sale Volume File (CNMS aggregate).

Bloomberg charges premium for short data via S3 Partners / Ortex integrations.
FINRA publishes the raw data free at T+1, 7:45 PM ET, via CDN.

═════════════════════════════════════════════════════════════════════════════
DATA SOURCE
───────────
  https://cdn.finra.org/equity/regsho/daily/CNMSshvol{YYYYMMDD}.txt
  Format: pipe-delimited
    Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
  ~11,500 tickers/day, ~510 KB.

═════════════════════════════════════════════════════════════════════════════
METRICS COMPUTED PER TICKER (S&P 500 universe + watchlist)
──────────────────────────────────────────────────────────

  1. Short Volume Ratio (SVR)
       SVR = ShortVolume / TotalVolume
       What % of today's trading was short selling.

  2. SVR Z-Score (60-day distribution)
       z = (SVR_today − mean_60d) / stdev_60d
       Abnormally heavy shorting detection.

  3. 5-day and 20-day moving averages
       Smooth out noise; trend identification.

  4. Short Momentum
       SVR_5d_avg − SVR_20d_avg
       Positive = building short pressure.

  5. Cumulative Short Volume (proxy for short interest)
       Sum of ShortVolume over rolling window (5d, 20d).
       Between FINRA's bi-monthly SI reports, this is the only daily proxy.

  6. Days-to-Cover (cumulative basis)
       Cum_short_volume_20d / avg_daily_total_volume_20d
       How many trading days would shorts need to fully cover at current vol.

  7. Squeeze Setup Score (0-100)
       +30 if SVR > 60% AND z-score > 2 (abnormal heavy shorting)
       +25 if days-to-cover > 5 (forced cover risk at any vol spike)
       +20 if price strength: last close > MA20 close (squeeze fuel)
       +15 if momentum > 2pp AND z-score > 2.5 (extreme + building)
       +10 if SVR ratio > 80% on any of last 3 days (climax shorting)
       Score ≥ 50 ⟹ squeeze candidate

═════════════════════════════════════════════════════════════════════════════
MARKET-WIDE COMPOSITES
──────────────────────
  • Volume-weighted SVR across all S&P 500 names
  • Top 30 highest SVR today
  • Top 30 squeeze setup scores
  • Sector aggregates (using SPDR mapping)
  • Distribution percentiles (p10, p50, p90)

═════════════════════════════════════════════════════════════════════════════
"""
import json
import math
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, date, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.1.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/finra-short.json"
HISTORY_KEY = "data/finra-short-history.json"

FINRA_BASE = "https://cdn.finra.org/equity/regsho/daily"
FMP_KEY = os.environ.get("FMP_KEY", "")
POLY_KEY = os.environ.get("POLY_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HTTP_TIMEOUT = 30
HISTORY_DAYS = 90  # rolling window per ticker

# Squeeze setup thresholds
SQUEEZE_SVR_HOT = 0.60
SQUEEZE_Z_HOT = 2.0
SQUEEZE_Z_EXTREME = 2.5
SQUEEZE_DTC_HOT = 5.0  # days
SQUEEZE_CLIMAX_SVR = 0.80

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# UNIVERSE & HISTORY
# ═══════════════════════════════════════════════════════════════════════════

def get_sp500_universe():
    """
    Fetch current S&P 500 constituent list.
    Source priority:
      1. GitHub datahub CSV (free, no auth, AWS-IP friendly, ~503 names)
         https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv
      2. FMP /v3/sp500_constituent (paid; may 403 on user's tier)
    Returns list of (symbol, sector, name) tuples.
    """
    # ── Source 1: GitHub datahub (preferred) ──
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "JustHodl-FINRA/1.0",
            "Accept": "text/csv",
        })
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            text = r.read().decode("utf-8")
        import csv as _csv
        from io import StringIO
        reader = _csv.DictReader(StringIO(text))
        out = []
        for row in reader:
            sym = (row.get("Symbol") or "").strip().upper()
            if not sym: continue
            # GitHub CSV uses class-letter notation (BRK.B); FINRA uses BRK.B too
            sector = (row.get("GICS Sector") or "").strip() or None
            name = (row.get("Security") or "").strip() or None
            out.append((sym, sector, name))
        if out:
            print(f"  loaded {len(out)} S&P 500 names from GitHub datahub")
            return out
    except Exception as e:
        print(f"  github sp500 fetch err: {str(e)[:120]} — trying FMP")

    # ── Source 2: FMP (fallback if user upgrades tier) ──
    if FMP_KEY:
        fmp_url = f"https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={FMP_KEY}"
        try:
            req = urllib.request.Request(fmp_url, headers={"User-Agent": "JustHodl-FINRA/1.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                data = json.loads(r.read().decode("utf-8"))
            out = [(d["symbol"], d.get("sector"), d.get("name")) for d in data if d.get("symbol")]
            print(f"  loaded {len(out)} S&P 500 names from FMP")
            return out
        except Exception as e:
            print(f"  fmp sp500 fetch err: {str(e)[:120]}")
    return []


def load_history():
    """Load rolling per-ticker history from S3."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=HISTORY_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {"tickers": {}}


def save_history(history):
    """Persist rolling history to S3."""
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=HISTORY_KEY,
            Body=json.dumps(history, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=600")
        return True
    except Exception as e:
        print(f"  history save err: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# FINRA DATA FETCH & PARSE
# ═══════════════════════════════════════════════════════════════════════════

def fetch_finra_for_date(d):
    """Fetch CNMS short volume file for given date. Returns dict[symbol] = {...}."""
    ds = d.strftime("%Y%m%d")
    url = f"{FINRA_BASE}/CNMSshvol{ds}.txt"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible) JustHodl/1.0",
            "Accept": "text/plain,*/*"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            body = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        if e.code == 403 or e.code == 404:
            return None  # not published yet for that date
        print(f"  finra fetch {ds} err: HTTP {e.code}")
        return None
    except Exception as e:
        print(f"  finra fetch {ds} err: {str(e)[:120]}")
        return None

    rows = {}
    lines = body.splitlines()
    header_found = False
    for line in lines:
        line = line.strip()
        if not line: continue
        if not header_found and line.startswith("Date|"):
            header_found = True
            continue
        parts = line.split("|")
        if len(parts) < 5: continue
        try:
            sym = parts[1].strip()
            short_vol = float(parts[2] or 0)
            short_exempt = float(parts[3] or 0)
            total_vol = float(parts[4] or 0)
            if total_vol <= 0: continue
            rows[sym] = {
                "short_volume": short_vol,
                "short_exempt": short_exempt,
                "total_volume": total_vol,
                "svr": round(short_vol / total_vol, 4),
            }
        except (ValueError, IndexError):
            continue
    return rows


def get_latest_finra_date():
    """Find the most recent date with a published file (T-1 typically)."""
    today = datetime.now(timezone.utc).date()
    # Try last 5 business days
    for n in range(0, 7):
        d = today - timedelta(days=n)
        if d.weekday() >= 5: continue  # skip weekends
        rows = fetch_finra_for_date(d)
        if rows: return d, rows
    return None, None


# ═══════════════════════════════════════════════════════════════════════════
# STATISTICS (pure Python — Lambda has no numpy)
# ═══════════════════════════════════════════════════════════════════════════

def _mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else 0.0


def _stdev(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2: return 0.0
    m = _mean(xs)
    v = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(v)


def _median(xs):
    xs = sorted([x for x in xs if x is not None])
    if not xs: return 0.0
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def _percentile(xs, p):
    """Linear-interpolated percentile, p in [0,1]."""
    xs = sorted([x for x in xs if x is not None])
    if not xs: return None
    k = (len(xs) - 1) * p
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c: return xs[f]
    return xs[f] + (xs[c] - xs[f]) * (k - f)


# ═══════════════════════════════════════════════════════════════════════════
# PER-TICKER ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

def analyze_ticker(symbol, today_row, history_series):
    """
    Compute all metrics for one ticker.

    Args:
      symbol: ticker string
      today_row: {svr, short_volume, total_volume, short_exempt} or None
      history_series: list of {ts, svr, short_vol, total_vol} sorted ascending

    Returns: dict of computed metrics or None if insufficient data.
    """
    if not today_row or today_row.get("total_volume", 0) <= 0:
        return None

    svr_history = [h.get("svr") for h in history_series if h.get("svr") is not None]
    short_vol_history = [h.get("short_vol") for h in history_series if h.get("short_vol") is not None]
    total_vol_history = [h.get("total_vol") for h in history_series if h.get("total_vol") is not None]

    if len(svr_history) < 5:  # need at least a week of history
        return {"symbol": symbol, "svr": today_row["svr"],
                  "short_volume": today_row["short_volume"],
                  "total_volume": today_row["total_volume"],
                  "insufficient_history": True}

    # Distributions for z-score (last 60 days)
    svr_60d = svr_history[-60:] if len(svr_history) >= 60 else svr_history
    mean_60 = _mean(svr_60d)
    std_60 = _stdev(svr_60d)
    z = (today_row["svr"] - mean_60) / std_60 if std_60 > 0 else None

    # Moving averages
    svr_5d_avg = _mean(svr_history[-5:])
    svr_20d_avg = _mean(svr_history[-20:]) if len(svr_history) >= 20 else _mean(svr_history)
    momentum = svr_5d_avg - svr_20d_avg

    # Cumulative short and days-to-cover
    short_vol_20d_sum = sum(short_vol_history[-20:])
    avg_total_vol_20d = _mean(total_vol_history[-20:]) if total_vol_history else 0
    days_to_cover = (short_vol_20d_sum / avg_total_vol_20d) if avg_total_vol_20d > 0 else None

    # Climax detection — top 3 days svr > 0.80
    last_3 = svr_history[-3:]
    climax = any(s > SQUEEZE_CLIMAX_SVR for s in last_3) if last_3 else False

    # Squeeze setup score
    score = 0
    flags = []
    if today_row["svr"] > SQUEEZE_SVR_HOT and z is not None and z > SQUEEZE_Z_HOT:
        score += 30; flags.append("SVR_HEAVY_ABNORMAL")
    if days_to_cover is not None and days_to_cover > SQUEEZE_DTC_HOT:
        score += 25; flags.append("HIGH_DAYS_TO_COVER")
    # Price strength flag (we'll set this externally from Polygon if available)
    if momentum > 0.02 and z is not None and z > SQUEEZE_Z_EXTREME:
        score += 15; flags.append("MOMENTUM_EXTREME")
    if climax:
        score += 10; flags.append("CLIMAX_SHORTING")

    return {
        "symbol": symbol,
        "svr": today_row["svr"],
        "svr_pct": round(today_row["svr"] * 100, 2),
        "short_volume": today_row["short_volume"],
        "total_volume": today_row["total_volume"],
        "short_exempt": today_row["short_exempt"],
        "svr_5d_avg": round(svr_5d_avg, 4),
        "svr_20d_avg": round(svr_20d_avg, 4),
        "svr_60d_mean": round(mean_60, 4),
        "svr_60d_stdev": round(std_60, 4),
        "z_score": round(z, 2) if z is not None else None,
        "momentum": round(momentum, 4),
        "momentum_pct": round(momentum * 100, 2),
        "short_volume_20d_sum": round(short_vol_20d_sum, 0),
        "avg_total_volume_20d": round(avg_total_vol_20d, 0),
        "days_to_cover": round(days_to_cover, 2) if days_to_cover else None,
        "climax_short_3d": climax,
        "squeeze_score": score,
        "squeeze_flags": flags,
        "n_history_days": len(svr_history),
    }


# ═══════════════════════════════════════════════════════════════════════════
# POLYGON PRICE STRENGTH (optional alpha overlay)
# ═══════════════════════════════════════════════════════════════════════════

def fetch_price_strength(symbol):
    """Fetch last 22 daily closes and compute price vs MA20."""
    if not POLY_KEY: return None
    today = date.today()
    start = (today - timedelta(days=45)).isoformat()
    end = today.isoformat()
    url = (f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
            f"{start}/{end}?adjusted=true&sort=asc&limit=50&apiKey={POLY_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-FINRA/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
        bars = data.get("results") or []
        if len(bars) < 21: return None
        closes = [b["c"] for b in bars]
        last = closes[-1]
        ma20 = _mean(closes[-20:])
        return {
            "last_close": round(last, 2),
            "ma20": round(ma20, 2),
            "above_ma20": last > ma20,
            "pct_vs_ma20": round((last / ma20 - 1) * 100, 2) if ma20 > 0 else None,
            "ret_5d": round((last / closes[-5] - 1) * 100, 2) if len(closes) >= 5 else None,
            "ret_20d": round((last / closes[-20] - 1) * 100, 2) if len(closes) >= 20 else None,
        }
    except Exception as e:
        return None


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception: return None


def send_telegram(text, chat_id):
    if not TELEGRAM_TOKEN or not chat_id: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text[:4000],
        "parse_mode": "Markdown", "disable_web_page_preview": "true",
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8")).get("ok", False)
    except Exception as e:
        print(f"  telegram err: {str(e)[:120]}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== FINRA-SHORT v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # ─── Step 1: Fetch latest FINRA file ───
    data_date, today_rows = get_latest_finra_date()
    if not today_rows:
        return {"statusCode": 500, "body": json.dumps({"err": "no FINRA data available"})}
    print(f"  loaded {len(today_rows):,} tickers for {data_date}")

    # ─── Step 2: Load universe + history ───
    universe = get_sp500_universe()
    universe_syms = {s for s, _, _ in universe}
    sector_map = {s: sect for s, sect, _ in universe if sect}
    name_map = {s: nm for s, _, nm in universe if nm}
    print(f"  universe: {len(universe_syms)} S&P 500 tickers")

    history = load_history()
    tickers_history = history.get("tickers", {})

    # ─── Step 3: Update history with today's data, drop ancient ───
    cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=HISTORY_DAYS)).timestamp())
    today_ts = int(time.time())
    today_iso = data_date.isoformat()

    # Update every ticker that's in today's file (full US universe for future flexibility)
    for sym, row in today_rows.items():
        series = tickers_history.get(sym, [])
        # Idempotency: replace today's entry if already present
        series = [h for h in series if h.get("date") != today_iso and h.get("ts", 0) >= cutoff_ts]
        series.append({
            "ts": today_ts, "date": today_iso,
            "svr": row["svr"],
            "short_vol": row["short_volume"],
            "total_vol": row["total_volume"],
        })
        # Cap at HISTORY_DAYS
        series = series[-HISTORY_DAYS:]
        tickers_history[sym] = series

    # Drop tickers not seen in 90 days
    stale = [s for s, series in tickers_history.items()
              if not series or series[-1].get("ts", 0) < cutoff_ts]
    for s in stale: tickers_history.pop(s, None)

    # ─── Step 4: Analyze S&P 500 universe ───
    universe_results = {}
    universe_signals = []  # for ranking and alerts
    print(f"  analyzing {len(universe_syms)} S&P 500 tickers in parallel...")

    def analyze_one(sym):
        today_row = today_rows.get(sym)
        if not today_row: return sym, None
        series = tickers_history.get(sym, [])
        # Don't include today in own history for stats (history is for past)
        past_series = series[:-1] if series else []
        result = analyze_ticker(sym, today_row, past_series)
        if not result: return sym, None
        result["sector"] = sector_map.get(sym)
        result["name"] = name_map.get(sym)
        return sym, result

    with ThreadPoolExecutor(max_workers=8) as ex:
        for sym, result in ex.map(analyze_one, universe_syms):
            if result:
                universe_results[sym] = result
                universe_signals.append(result)

    print(f"  analyzed {len(universe_results)} tickers with data")

    # ─── Step 5: Add price strength to top candidates (Polygon overlay) ───
    # Top 50 by squeeze_score get price overlay (limits API calls)
    universe_signals.sort(key=lambda r: -(r.get("squeeze_score") or 0))
    top_for_price = [r["symbol"] for r in universe_signals[:50]
                       if (r.get("squeeze_score") or 0) >= 30]

    if top_for_price and POLY_KEY:
        print(f"  fetching price strength for top {len(top_for_price)} squeeze candidates...")
        with ThreadPoolExecutor(max_workers=6) as ex:
            future_to_sym = {ex.submit(fetch_price_strength, s): s for s in top_for_price}
            for fut in as_completed(future_to_sym):
                sym = future_to_sym[fut]
                try:
                    ps = fut.result()
                    if ps:
                        universe_results[sym]["price_strength"] = ps
                        # Bonus +20 if price above MA20 (squeeze fuel)
                        if ps.get("above_ma20"):
                            universe_results[sym]["squeeze_score"] = (universe_results[sym].get("squeeze_score") or 0) + 20
                            universe_results[sym]["squeeze_flags"].append("PRICE_STRENGTH")
                except Exception: pass

    # Rebuild signals list with updated scores
    universe_signals = list(universe_results.values())
    universe_signals.sort(key=lambda r: -(r.get("squeeze_score") or 0))
    squeeze_candidates = [r for r in universe_signals if (r.get("squeeze_score") or 0) >= 50]

    # ─── Step 6: Compute market composites ───
    all_svrs = [r["svr"] for r in universe_signals if r.get("svr") is not None]
    all_volumes = [r["total_volume"] for r in universe_signals if r.get("total_volume", 0) > 0]
    vw_svr = (sum(r["svr"] * r["total_volume"] for r in universe_signals
                    if r.get("svr") is not None and r.get("total_volume", 0) > 0)
              / sum(all_volumes)) if all_volumes else None

    p10 = _percentile(all_svrs, 0.10)
    p50 = _percentile(all_svrs, 0.50)
    p90 = _percentile(all_svrs, 0.90)
    p99 = _percentile(all_svrs, 0.99)

    # Top 30 by SVR (most aggressive shorting today)
    top_svr = sorted(universe_signals, key=lambda r: -r.get("svr", 0))[:30]

    # Top 30 by z-score (most ABNORMAL shorting today)
    top_zscore = sorted([r for r in universe_signals if r.get("z_score") is not None],
                          key=lambda r: -r.get("z_score", 0))[:30]

    # Sector aggregates
    sector_agg = defaultdict(lambda: {"svrs": [], "vol": 0, "n": 0})
    for r in universe_signals:
        sec = r.get("sector")
        if not sec: continue
        sector_agg[sec]["svrs"].append(r["svr"])
        sector_agg[sec]["vol"] += r.get("total_volume", 0)
        sector_agg[sec]["n"] += 1
    sector_results = {}
    for sec, d in sector_agg.items():
        sector_results[sec] = {
            "n_tickers": d["n"],
            "median_svr": round(_median(d["svrs"]) * 100, 2),
            "mean_svr": round(_mean(d["svrs"]) * 100, 2),
            "max_svr": round(max(d["svrs"]) * 100, 2) if d["svrs"] else 0,
            "total_volume": round(d["vol"], 0),
        }
    sector_results = dict(sorted(sector_results.items(),
                                   key=lambda kv: -kv[1]["median_svr"]))

    # ─── Step 7: Market-wide regime classification ───
    if vw_svr is None:
        regime = "UNKNOWN"
    elif vw_svr > 0.55:
        regime = "HEAVY_SHORTING"
    elif vw_svr > 0.50:
        regime = "ELEVATED_SHORTING"
    elif vw_svr > 0.45:
        regime = "NORMAL"
    else:
        regime = "LIGHT_SHORTING"

    # ─── Step 8: Build & write payload ───
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": today_ts,
        "version": VERSION,
        "data_date": today_iso,
        "elapsed_seconds": round(time.time() - started, 2),
        "config": {
            "universe": "S&P 500",
            "history_days": HISTORY_DAYS,
            "squeeze_thresholds": {
                "svr_hot": SQUEEZE_SVR_HOT, "z_hot": SQUEEZE_Z_HOT,
                "z_extreme": SQUEEZE_Z_EXTREME, "dtc_hot": SQUEEZE_DTC_HOT,
                "climax_svr": SQUEEZE_CLIMAX_SVR,
            },
        },
        "market_composite": {
            "regime": regime,
            "volume_weighted_svr": round(vw_svr, 4) if vw_svr else None,
            "volume_weighted_svr_pct": round(vw_svr * 100, 2) if vw_svr else None,
            "median_svr_pct": round(p50 * 100, 2) if p50 else None,
            "p10_svr_pct": round(p10 * 100, 2) if p10 else None,
            "p90_svr_pct": round(p90 * 100, 2) if p90 else None,
            "p99_svr_pct": round(p99 * 100, 2) if p99 else None,
            "n_analyzed": len(universe_signals),
            "n_universe_in_file": len([s for s in universe_syms if s in today_rows]),
            "n_high_svr": sum(1 for r in universe_signals if r.get("svr", 0) > 0.60),
            "n_extreme_z": sum(1 for r in universe_signals
                                 if r.get("z_score") is not None and r["z_score"] > 2),
        },
        "squeeze_candidates": [
            {k: r.get(k) for k in [
                "symbol", "name", "sector", "svr_pct", "z_score", "momentum_pct",
                "days_to_cover", "squeeze_score", "squeeze_flags", "price_strength",
                "short_volume", "total_volume",
            ]} for r in squeeze_candidates[:30]
        ],
        "top_svr": [
            {k: r.get(k) for k in [
                "symbol", "name", "sector", "svr_pct", "z_score",
                "short_volume", "total_volume",
            ]} for r in top_svr
        ],
        "top_zscore": [
            {k: r.get(k) for k in [
                "symbol", "name", "sector", "svr_pct", "z_score", "momentum_pct",
                "short_volume",
            ]} for r in top_zscore
        ],
        "sectors": sector_results,
        "tickers": universe_results,  # full per-ticker data for the page
    }

    try:
        body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
            ContentType="application/json", CacheControl="public, max-age=600")
        size_kb = len(body) / 1024
        print(f"  ✓ finra-short.json written ({size_kb:.1f} KB)")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # ─── Step 9: Update history file ───
    history["tickers"] = tickers_history
    history["last_updated"] = today_ts
    history["last_data_date"] = today_iso
    save_history(history)

    # ─── Step 10: Telegram alert on top squeeze candidates ───
    alert_sent = False
    if squeeze_candidates:
        chat_id = get_chat_id()
        if chat_id:
            top5 = squeeze_candidates[:5]
            lines = [f"🩸 *FINRA Short Volume — Squeeze Setups ({today_iso})*",
                      f"_Regime: {regime} · VW-SVR: {round(vw_svr*100,1) if vw_svr else '?'}%_\n"]
            for sc in top5:
                ps = sc.get("price_strength") or {}
                lines.append(f"*{sc['symbol']}* · score {sc['squeeze_score']}/100")
                lines.append(f"  SVR {sc.get('svr_pct')}% · z={sc.get('z_score')} · DTC {sc.get('days_to_cover')}d")
                if ps.get("above_ma20"):
                    lines.append(f"  Price strength: {ps.get('pct_vs_ma20'):+.1f}% vs MA20")
                lines.append(f"  Flags: {', '.join(sc.get('squeeze_flags', []))}\n")
            lines.append(f"[Full Dashboard](https://justhodl.ai/short/)")
            try: alert_sent = send_telegram("\n".join(lines), chat_id)
            except Exception as e: print(f"  alert err: {e}")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "data_date": today_iso,
        "n_universe_analyzed": len(universe_signals),
        "n_squeeze_candidates": len(squeeze_candidates),
        "vw_svr_pct": round(vw_svr * 100, 2) if vw_svr else None,
        "regime": regime,
        "top_squeeze": [sc["symbol"] for sc in squeeze_candidates[:5]],
        "alert_sent": alert_sent,
        "elapsed_seconds": round(time.time() - started, 2),
    })}
