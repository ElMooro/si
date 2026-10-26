"""
justhodl-pead-detector — Earnings Surprise Streak + PEAD scanner

Post-Earnings Announcement Drift (PEAD) is one of the most documented
anomalies in equity markets. Stocks that beat earnings expectations
significantly continue to drift in the direction of the surprise for
60-90 days. The effect is strongest when:
  • Beat magnitude > 5% of consensus (top decile of surprises)
  • Multiple consecutive quarters of beats (streak of 3+ → much stronger)
  • Surprise magnitudes themselves accelerating (each Q's beat bigger)
  • Stock didn't fully gap-up on the announcement (institutions still building)

WHAT THIS DETECTS — for every stock in universe (all caps):

  1. EPS Surprise Streak — count consecutive quarters where
     reported_eps > estimated_eps with magnitude > 0%
  2. Average beat magnitude across last 4 quarters (in % of estimate)
  3. Acceleration of beats — are recent beats larger than older beats?
  4. Revenue beat streak — same logic but for revenue
  5. Combined EPS+Rev beat (both metrics beat) — gold standard
  6. Days since last earnings announcement (PEAD effect strongest 1-30 days post)
  7. Post-earnings price drift — stock up since last earnings announcement?
     If yes, drift continuing; if no, mean-reversion opportunity
  8. Pre-earnings setup — for stocks 2-10 days FROM next earnings

SCORE 0-100 combines:
  • Beat streak length (3+ Q in a row): max 25
  • Average beat magnitude: max 25
  • Acceleration of magnitudes: max 15
  • Recent EPS+Rev double-beat: max 15
  • Post-earnings drift positive: max 10
  • Recent earnings within 30 days: max 10

Tiers:
  TIER_S_DRIFTING: 4Q+ streak, avg beat >10%, recent earnings <30d
  TIER_A_BEATING:  3Q streak, avg beat >5%
  TIER_B_BUILDING: 2Q streak, avg beat >3%

OUTPUT: data/pead-signals.json

v1.1.0 (2026-10-26, bank fusion wave 2): the earnings history comes from the warehouse first - the FMP walker's
per-symbol `earnings` file (EPS + revenue, every quarter), then the factor book's surprises + calendar blocks - and the
live /stable/earnings call is only the fallback (one call per name instead of two).  The post-earnings drift reads the
book's tape returns (ret_5d/21d/63d, `drift_basis`) before the live price history.  The universe is data/universe.json
plus the book's whole-market beat-streak candidates (beat rate >= 75% over 4 quarters, avg surprise > 3%,
`universe_basis`).  Every row carries `bank_context` (Altman Z, Piotroski, short % float, analyst upside, consensus,
growth / street composites) and `earnings_basis`.  Floors, scoring and tiers unchanged.
"""

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
import io, json, os, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials
try:  # v1.1.0 bank fusion: the factor book (aws/shared/bank_enrich) + the FMP walker bank (aws/shared/fmp_bank)
    import bank_enrich as _book
except Exception:  # noqa: BLE001
    _book = None
try:
    import fmp_bank as _bank
except Exception:  # noqa: BLE001
    _bank = None

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/pead-signals.json")
FMP_KEY = managed_secret(('FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
N_WORKERS = int(os.environ.get("N_WORKERS", "12"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "1500"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "550"))
VERSION = "1.1.0"
BOOK_CANDIDATES = int(os.environ.get("PEAD_BOOK_CANDIDATES", "600"))       # whole-market beat-streak names added to the universe
BOOK_MIN_MCAP = float(os.environ.get("PEAD_BOOK_MIN_MCAP", "50000000"))   # $50M: the engine scans every cap bucket
BANK_CONTEXT_FIELDS = ("mcap", "sector", "industry", "altman_z", "piotroski", "short_pct_float", "pt_upside_pct", "pt_n_1m", "consensus_score",
                       "beat_rate_4q", "avg_surprise_pct_4q", "composite_growth", "composite_street", "composite_quality")
BASIS = {"fmp-walker earnings": 0, "factor-book surprises+calendar": 0, "fmp live": 0, "none": 0}
import threading as _threading
_BASIS_LOCK = _threading.Lock()

S3 = boto3.client("s3", region_name=REGION)


def book_ok():
    return _book is not None and _book.available()


def cap_bucket_from_mcap(m):
    """The universe file's size buckets, from a market cap (book candidates only)."""
    try:
        m = float(m or 0)
    except (TypeError, ValueError):
        return "?"
    if m <= 0:
        return "?"
    if m < 50e6:
        return "nano"
    if m < 300e6:
        return "micro"
    if m < 2e9:
        return "small"
    if m < 10e9:
        return "mid"
    if m < 200e9:
        return "large"
    return "mega"


def book_candidates(exclude):
    """Whole-market beat-streak candidates from the factor book: beat rate >= 75% over the last 4 quarters, average
    surprise > 3%, >= BOOK_MIN_MCAP, US common stocks (ETF/OTC/fund/preferred excluded by the SDK), largest first."""
    if not book_ok():
        return []
    try:
        rows = _book.prescreen_rows(lambda r: (r.get("beat_rate_4q") or 0) >= 0.75 and (r.get("avg_surprise_pct_4q") or 0) > 3
                               and (r.get("mcap") or 0) >= BOOK_MIN_MCAP and not r.get("is_adr"),
                               order_by="mcap", descending=True, limit=BOOK_CANDIDATES + len(exclude),
                               fields=["beat_rate_4q", "avg_surprise_pct_4q", "mcap", "sector"])
    except Exception as e:  # noqa: BLE001
        print("[pead] factor book candidates failed: " + str(e)[:80])
        return []
    out = []
    for r in rows:
        sym = r.get("symbol")
        if not sym or sym in exclude:
            continue
        out.append({"symbol": sym, "sector": r.get("sector") or "?", "market_cap": r.get("mcap") or 0,
                    "cap_bucket": cap_bucket_from_mcap(r.get("mcap")), "from_book": True})
        if len(out) >= BOOK_CANDIDATES:
            break
    return out


def _num(x):
    try:
        return float(x) if x is not None and x != "" else None
    except (TypeError, ValueError):
        return None


def bank_earnings_rows(symbol):
    """The symbol's earnings rows in the /stable/earnings shape ({date, epsActual, epsEstimated, revenueActual,
    revenueEstimated}) from the warehouse: the walker's per-symbol `earnings` file first (complete, with revenue), then
    the factor book document (surprises = EPS actual vs estimate per report, calendar = the next / latest reports with
    revenue).  (rows, basis) or (None, None) when neither bank holds the name."""
    if _bank is not None:
        try:
            rows = _bank.walker("earnings", symbol)
        except Exception:  # noqa: BLE001
            rows = None
        if isinstance(rows, list) and rows:
            return [r for r in rows if isinstance(r, dict) and r.get("date")], "fmp-walker earnings"
    if not book_ok():
        return None, None
    try:
        d = _book.factor(symbol)
    except Exception:  # noqa: BLE001
        d = None
    if not d:
        return None, None
    by_date = {}
    for r in (d.get("surprises") or []):
        if not isinstance(r, dict) or not r.get("date"):
            continue
        by_date[str(r["date"])[:10]] = {"date": str(r["date"])[:10], "symbol": symbol,
                                        "epsActual": _num(r.get("actualEarningResult", r.get("epsActual"))),
                                        "epsEstimated": _num(r.get("estimatedEarning", r.get("epsEstimated"))),
                                        "revenueActual": None, "revenueEstimated": None}
    for r in ((d.get("calendar") or {}).get("earnings") or []):
        if not isinstance(r, dict) or not r.get("date"):
            continue
        day = str(r["date"])[:10]
        row = by_date.get(day) or {"date": day, "symbol": symbol, "epsActual": None, "epsEstimated": None, "revenueActual": None, "revenueEstimated": None}
        for k in ("epsActual", "epsEstimated", "revenueActual", "revenueEstimated"):
            if r.get(k) is not None:
                row[k] = _num(r.get(k))
        by_date[day] = row
    if not by_date:
        return None, None
    return list(by_date.values()), "factor-book surprises+calendar"


def bank_context(symbol):
    """The book's facts the engine never had, as a labelled block (None when the book lacks the name)."""
    if not book_ok():
        return None
    try:
        row = _book.lookup(symbol, list(BANK_CONTEXT_FIELDS))
    except Exception:  # noqa: BLE001
        row = None
    if not row:
        return None
    out = {k: row.get(k) for k in BANK_CONTEXT_FIELDS}
    out["source"] = "factor-book"
    return out


def book_drift(symbol, days_since_earnings):
    """Post-earnings drift from the book's tape returns at the horizon nearest the days since the report
    (5 / 21 / 63 sessions); (drift_pct, basis) or (None, None)."""
    if not book_ok() or days_since_earnings is None:
        return None, None
    try:
        row = _book.lookup(symbol, ["ret_5d", "ret_21d", "ret_63d"])
    except Exception:  # noqa: BLE001
        row = None
    if not row:
        return None, None
    sessions = days_since_earnings * 252.0 / 365.0
    field = "ret_5d" if sessions <= 10 else ("ret_21d" if sessions <= 40 else "ret_63d")
    v = _num(row.get(field))
    if v is None:
        return None, None
    return v * 100.0, "factor-book " + field


def fetch_url(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-PEAD/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def get_universe():
    """All stocks across all cap buckets — PEAD is universal."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        # All buckets — PEAD works across the entire size spectrum
        all_stocks = d.get("stocks", [])
        return all_stocks[:MAX_TICKERS]
    except Exception as e:
        print("[pead] universe load failed: " + str(e))
        return []


def past_from_rows(rows, limit=12):
    """Reported quarters (epsActual present) newest first, from /stable/earnings-shaped rows."""
    past = [e for e in rows if isinstance(e, dict) and e.get("epsActual") is not None]
    past.sort(key=lambda x: x.get("date", "") or "", reverse=True)
    return past[:limit]


def window_from_rows(rows):
    """Events within +/- 90 days of today, each with `_days_diff` (the old calendar read, without the second call)."""
    out = []
    for e in rows:
        dt_str = (e.get("date") or "") if isinstance(e, dict) else ""
        if not dt_str:
            continue
        try:
            dt = datetime.strptime(dt_str[:10], "%Y-%m-%d")
        except Exception:
            continue
        days_diff = (dt - datetime.now()).days
        if -90 <= days_diff <= 90:
            out.append({**e, "_days_diff": days_diff})
    return out


def fetch_earnings_surprises(symbol, limit=12):
    """FMP /stable/earnings — returns past + future earnings with both
    eps + revenue actual/estimate. 164 quarters of history per ticker.  (Live fallback path.)"""
    url = ("https://financialmodelingprep.com/stable/earnings?"
           "symbol=" + symbol + "&apikey=" + FMP_KEY)
    try:
        d = fetch_url(url, timeout=12)
        if not isinstance(d, list):
            return None
        return past_from_rows(d, limit)
    except Exception:
        return None


def fetch_earnings_calendar(symbol):
    """Get next earnings date for this symbol via FMP."""
    today = time.strftime("%Y-%m-%d")
    future = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 90 * 86400))
    url = ("https://financialmodelingprep.com/stable/earnings?"
           "symbol=" + symbol + "&apikey=" + FMP_KEY)
    try:
        d = fetch_url(url, timeout=10)
        if not isinstance(d, list):
            return None
        # Filter to upcoming (future date) and past (last 90 days)
        recent_or_upcoming = []
        for e in d:
            dt_str = e.get("date") or ""
            if not dt_str:
                continue
            # Only include events within +/- 90d for context
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d")
                days_diff = (dt - datetime.now()).days
                if -90 <= days_diff <= 90:
                    recent_or_upcoming.append({**e, "_days_diff": days_diff})
            except Exception:
                continue
        return recent_or_upcoming
    except Exception:
        return None


def fetch_quote(symbol):
    try:
        d = fetch_url("https://financialmodelingprep.com/stable/quote?symbol=" + symbol + "&apikey=" + FMP_KEY, timeout=10)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def fetch_history(symbol, days=120):
    try:
        d = fetch_url("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=" + symbol + "&apikey=" + FMP_KEY, timeout=15)
        if not isinstance(d, list):
            return None
        out = []
        for x in d[:days]:
            if x.get("close") and x.get("date"):
                out.append({"date": x["date"], "close": float(x["close"])})
        out.sort(key=lambda r: r["date"])
        return out
    except Exception:
        return None


def evaluate_ticker(stock):
    sym = (stock.get("symbol") or "").upper()
    cap_bucket = stock.get("cap_bucket", "?")
    sector = stock.get("sector", "?")
    market_cap = stock.get("market_cap", 0)

    # Earnings rows: the warehouse first (walker file / factor book), the live /stable/earnings call as the fallback
    rows, earnings_basis = bank_earnings_rows(sym)
    if rows is None:
        try:
            rows = fetch_url("https://financialmodelingprep.com/stable/earnings?symbol=" + sym + "&apikey=" + FMP_KEY, timeout=12)
        except Exception:
            rows = None
        earnings_basis = "fmp live" if isinstance(rows, list) and rows else "none"
    with _BASIS_LOCK:
        BASIS[earnings_basis] = BASIS.get(earnings_basis, 0) + 1
    if not isinstance(rows, list) or not rows:
        return None
    surprises = past_from_rows(rows, limit=8)
    if not surprises or len(surprises) < 3:
        return None

    # surprises is already filtered to past + sorted newest first
    surprises_sorted = surprises

    # Compute EPS + revenue beats
    beats = []
    for q in surprises_sorted:
        eps_actual = q.get("epsActual")
        eps_est = q.get("epsEstimated")
        rev_actual = q.get("revenueActual")
        rev_est = q.get("revenueEstimated")
        
        if eps_actual is None or eps_est is None:
            continue
        try:
            eps_actual = float(eps_actual)
            eps_est = float(eps_est)
        except (TypeError, ValueError):
            continue
        if abs(eps_est) < 0.001:
            continue
        eps_beat_pct = (eps_actual - eps_est) / abs(eps_est) * 100
        
        # Revenue beat (optional)
        rev_beat_pct = None
        if rev_actual is not None and rev_est is not None:
            try:
                rev_actual = float(rev_actual)
                rev_est = float(rev_est)
                if rev_est > 0:
                    rev_beat_pct = (rev_actual - rev_est) / rev_est * 100
            except (TypeError, ValueError):
                pass
        
        beats.append({
            "quarter_end": q.get("date"),
            "actual": eps_actual,
            "estimate": eps_est,
            "beat_pct": eps_beat_pct,
            "beat": eps_actual > eps_est,
            "rev_actual": rev_actual,
            "rev_estimate": rev_est,
            "rev_beat_pct": rev_beat_pct,
            "double_beat": eps_actual > eps_est and (rev_beat_pct is not None and rev_beat_pct > 0),
        })

    if len(beats) < 3:
        return None

    # Streak: consecutive beats from most recent backward
    streak = 0
    for b in beats:
        if b["beat"] and b["beat_pct"] > 0:
            streak += 1
        else:
            break

    # Average beat magnitude (last 4 quarters)
    last_4 = beats[:4]
    avg_beat_pct = sum(b["beat_pct"] for b in last_4) / len(last_4) if last_4 else 0
    
    # Acceleration: are most recent beats larger than older ones?
    if len(beats) >= 4:
        recent_avg = sum(b["beat_pct"] for b in beats[:2]) / 2
        older_avg = sum(b["beat_pct"] for b in beats[2:4]) / 2
        beat_acceleration = recent_avg - older_avg
    else:
        beat_acceleration = 0

    # Latest beat
    latest_beat = beats[0] if beats else None
    latest_beat_pct = latest_beat["beat_pct"] if latest_beat else 0
    latest_qtr_end = latest_beat["quarter_end"] if latest_beat else None

    # Days since latest earnings announcement (proxy: quarter end + ~30 days)
    days_since_qtr_end = None
    days_since_earnings = None
    if latest_qtr_end:
        try:
            qe_dt = datetime.strptime(latest_qtr_end, "%Y-%m-%d")
            days_since_qtr_end = (datetime.now() - qe_dt).days
            # Earnings announcement is typically 25-50 days after quarter end
            days_since_earnings = max(0, days_since_qtr_end - 35)
        except Exception:
            pass

    # Post-earnings price drift (if recent earnings within 60 days): the book's tape returns first, live history after
    drift_pct = None
    drift_basis = None
    if days_since_earnings is not None and days_since_earnings < 60:
        drift_pct, drift_basis = book_drift(sym, days_since_earnings)
    if drift_pct is None and days_since_earnings is not None and days_since_earnings < 60:
        history = fetch_history(sym, days=90)
        drift_basis = "fmp live history" if history else None
        if history:
            # Compare today vs price at announcement time (approximate)
            today_close = history[-1]["close"]
            # Find price ~days_since_earnings ago
            target_idx = max(0, len(history) - days_since_earnings - 1)
            past_close = history[target_idx]["close"] if target_idx < len(history) else None
            if past_close and past_close > 0:
                drift_pct = (today_close - past_close) / past_close * 100

    # Next earnings date: from the same rows (no second call)
    upcoming_earnings = None
    days_to_next = None
    cal = window_from_rows(rows)
    if cal:
        future_events = [e for e in cal if e.get("_days_diff", -100) > 0]
        if future_events:
            # Sort by date
            future_events.sort(key=lambda x: x.get("_days_diff", 999))
            upcoming_earnings = future_events[0].get("date")
            days_to_next = future_events[0].get("_days_diff")

    # ─── SCORING ───
    score = 0.0
    flags = []

    # 1. Streak length
    if streak >= 5:
        score += 25; flags.append("STREAK_5Q+")
    elif streak >= 4:
        score += 20; flags.append("STREAK_4Q")
    elif streak >= 3:
        score += 15; flags.append("STREAK_3Q")
    elif streak >= 2:
        score += 8; flags.append("STREAK_2Q")

    # 2. Average beat magnitude
    if avg_beat_pct > 25:
        score += 25; flags.append("BIG_AVG_BEAT_25%+")
    elif avg_beat_pct > 15:
        score += 18; flags.append("STRONG_AVG_BEAT_15%+")
    elif avg_beat_pct > 8:
        score += 12; flags.append("AVG_BEAT_8%+")
    elif avg_beat_pct > 3:
        score += 6; flags.append("AVG_BEAT_3%+")

    # 3. Beat acceleration (magnitudes growing)
    if beat_acceleration > 10:
        score += 15; flags.append("BEATS_ACCELERATING_10PP+")
    elif beat_acceleration > 5:
        score += 10; flags.append("BEATS_ACCELERATING_5PP+")
    elif beat_acceleration > 0:
        score += 5

    # Count of last 4 with double-beat (EPS + revenue both beat)
    last_4_db = sum(1 for b in beats[:4] if b.get("double_beat"))
    if last_4_db >= 4:
        score += 15; flags.append("DOUBLE_BEAT_4Q")
    elif last_4_db >= 3:
        score += 10; flags.append("DOUBLE_BEAT_3Q")
    elif last_4_db >= 2:
        score += 5

    # 4. Latest beat — was it big?
    if latest_beat_pct > 30:
        score += 15; flags.append("LATEST_BEAT_30%+")
    elif latest_beat_pct > 15:
        score += 10; flags.append("LATEST_BEAT_15%+")
    elif latest_beat_pct > 5:
        score += 5; flags.append("LATEST_BEAT_5%+")

    # 5. Recent earnings (PEAD window — drift strongest 1-30 days)
    if days_since_earnings is not None:
        if 1 <= days_since_earnings <= 15:
            score += 10; flags.append("PEAD_PEAK_WINDOW")
        elif 15 < days_since_earnings <= 45:
            score += 7; flags.append("PEAD_ACTIVE")
        elif 45 < days_since_earnings <= 90:
            score += 3; flags.append("PEAD_FADING")

    # 6. Drift confirmation
    if drift_pct is not None:
        if drift_pct > 10:
            score += 10; flags.append("DRIFT_UP_10%+")
        elif drift_pct > 3:
            score += 6; flags.append("DRIFT_UP")
        elif drift_pct < -5:
            # Mean-reversion opportunity (good company, oversold)
            flags.append("MEAN_REVERSION_SETUP")
            if streak >= 3:
                score += 8  # paradox: beating but selling = opportunity
                flags.append("FUNDAMENTALS_VS_PRICE_DIVERGE")

    # 7. Pre-earnings setup
    if days_to_next is not None and 2 <= days_to_next <= 14:
        if streak >= 3:
            score += 8; flags.append("PRE_EARNINGS_STREAK")
        else:
            flags.append("PRE_EARNINGS_NO_EDGE")

    score = min(score, 100)

    # Tier
    if streak >= 4 and avg_beat_pct > 10 and (days_since_earnings is None or days_since_earnings < 45):
        tier = "TIER_S_DRIFTING"
    elif streak >= 3 and avg_beat_pct > 5:
        tier = "TIER_A_BEATING"
    elif streak >= 2 and avg_beat_pct > 3:
        tier = "TIER_B_BUILDING"
    else:
        tier = "QUIET"

    ctx = bank_context(sym)
    if ctx and stock.get("from_book"):
        sector = ctx.get("sector") or sector
        market_cap = ctx.get("mcap") or market_cap
    return {
        "symbol": sym,
        "score": round(score, 1),
        "tier": tier,
        "flags": flags,
        "earnings_basis": earnings_basis,
        "drift_basis": drift_basis,
        "bank_context": ctx,
        "metrics": {
            "streak": streak,
            "avg_beat_pct": round(avg_beat_pct, 2),
            "beat_acceleration": round(beat_acceleration, 2),
            "latest_beat_pct": round(latest_beat_pct, 2),
            "days_since_earnings": days_since_earnings,
            "days_to_next_earnings": days_to_next,
            "next_earnings_date": upcoming_earnings,
            "post_earnings_drift_pct": round(drift_pct, 2) if drift_pct is not None else None,
            "cap_bucket": cap_bucket,
            "market_cap": market_cap,
            "sector": sector,
        },
        "history": [
            {
                "qtr_end": b["quarter_end"],
                "actual": round(b["actual"], 4),
                "estimate": round(b["estimate"], 4),
                "beat_pct": round(b["beat_pct"], 2),
            }
            for b in beats[:6]
        ],
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline = started + TIMEOUT_BUDGET_S
    print("[pead] starting v" + VERSION)
    for k in BASIS:
        BASIS[k] = 0

    universe = get_universe()
    n_file = len(universe)
    book = book_candidates({(s.get("symbol") or "").upper() for s in universe})
    universe = universe + book
    book_as_of = _book.as_of() if book_ok() else None
    universe_basis = ("data/universe.json (%d) + factor book beat-streak candidates (%d: beat rate >= 75%% / 4q, avg surprise > 3%%, >= $%dM)"
                      % (n_file, len(book), int(BOOK_MIN_MCAP / 1e6))) if book else ("data/universe.json (%d); factor book unavailable" % n_file)
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"n": 0})}
    print("[pead] universe: " + str(len(universe)) + " stocks (" + universe_basis + ")")

    results = []
    n_no_data = 0

    def evaluate(stock):
        if time.time() > deadline:
            return None
        try:
            return evaluate_ticker(stock)
        except Exception as e:
            print("[pead] " + (stock.get("symbol") or "?") + " err: " + str(e)[:80])
            return None

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate, s): s for s in universe}
        for f in as_completed(futures):
            try:
                r = f.result()
                if r:
                    results.append(r)
                else:
                    n_no_data += 1
            except Exception:
                n_no_data += 1

    print("[pead] OK: " + str(len(results)) + ", no_data: " + str(n_no_data) + ", earnings basis: " + json.dumps(BASIS))
    results.sort(key=lambda x: -x["score"])

    by_tier = {
        "tier_s": [r for r in results if r["tier"] == "TIER_S_DRIFTING"],
        "tier_a": [r for r in results if r["tier"] == "TIER_A_BEATING"],
        "tier_b": [r for r in results if r["tier"] == "TIER_B_BUILDING"],
    }

    # Cap-bucket breakdowns
    by_cap = {"nano": [], "micro": [], "small": [], "mid": [], "large": [], "mega": []}
    for r in results:
        b = r["metrics"]["cap_bucket"]
        if b in by_cap and r["tier"] != "QUIET":
            by_cap[b].append(r)

    # Pre-earnings opportunities (next 2-14 days)
    pre_earnings_setups = [r for r in results
                            if r["metrics"]["days_to_next_earnings"] is not None
                            and 2 <= r["metrics"]["days_to_next_earnings"] <= 14
                            and r["metrics"]["streak"] >= 3]

    out = {
        "schema_version": 1,
        "method": "pead_detector_v1.1",
        "version": VERSION,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "universe_basis": universe_basis,
        "book_as_of": book_as_of,
        "stats": {
            "n_universe": len(universe),
            "n_universe_file": n_file,
            "n_book_candidates": len(book),
            "earnings_basis": dict(BASIS),
            "n_evaluated": len(results),
            "n_no_data": n_no_data,
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "n_tier_b": len(by_tier["tier_b"]),
            "n_pre_earnings_setups": len(pre_earnings_setups),
            "by_cap_bucket": {k: len(v) for k, v in by_cap.items()},
        },
        "summary": {
            "top_30_overall": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "tier": r["tier"],
                    "streak": r["metrics"]["streak"],
                    "avg_beat_pct": r["metrics"]["avg_beat_pct"],
                    "beat_accel": r["metrics"]["beat_acceleration"],
                    "latest_beat_pct": r["metrics"]["latest_beat_pct"],
                    "days_since_earnings": r["metrics"]["days_since_earnings"],
                    "days_to_next": r["metrics"]["days_to_next_earnings"],
                    "drift_pct": r["metrics"]["post_earnings_drift_pct"],
                    "cap_bucket": r["metrics"]["cap_bucket"],
                    "flags": r["flags"],
                }
                for r in results[:30]
            ],
            "tier_s": [r["symbol"] for r in by_tier["tier_s"]][:30],
            "best_microcap": [
                {"symbol": r["symbol"], "score": r["score"], "streak": r["metrics"]["streak"],
                 "avg_beat": r["metrics"]["avg_beat_pct"]}
                for r in results
                if r["metrics"]["cap_bucket"] in ("nano", "micro") and r["score"] >= 40
            ][:15],
            "best_smallcap": [
                {"symbol": r["symbol"], "score": r["score"], "streak": r["metrics"]["streak"],
                 "avg_beat": r["metrics"]["avg_beat_pct"]}
                for r in results
                if r["metrics"]["cap_bucket"] == "small" and r["score"] >= 50
            ][:15],
            "pre_earnings_setups": [
                {"symbol": r["symbol"], "score": r["score"], "streak": r["metrics"]["streak"],
                 "next_earnings": r["metrics"]["next_earnings_date"],
                 "days_to_next": r["metrics"]["days_to_next_earnings"]}
                for r in pre_earnings_setups[:20]
            ],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[pead] wrote " + str(len(body)) + "b")
    print("[pead] tier_s=" + str(len(by_tier["tier_s"])) + " tier_a=" + str(len(by_tier["tier_a"])))
    if results[:5]:
        print("[pead] top: " + str([(r["symbol"], r["score"], r["metrics"]["streak"]) for r in results[:5]]))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_evaluated": len(results),
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "n_pre_earnings": len(pre_earnings_setups),
            "duration_s": out["duration_s"],
        }),
    }
