"""
justhodl-breadth-thrust -- Zweig Breadth Thrust + Whaley + Coppock.

The Zweig Breadth Thrust is one of the rarest and most reliable
signals in equity history: when NYSE 10-day EMA of advancing-issue
ratio crosses from below 0.40 to above 0.615 within 10 trading
sessions. Since 1945 it has fired 11 times -- and 11 of 11 produced
positive forward 12-month returns averaging roughly +24%. It fires
once every 4-7 years and marks the precise inflection from broad
capitulation to broad accumulation.

State machine:

    NULL     no setup, EMA in normal range
    ARMED    10-day EMA recently <= 0.40 (oversold breadth, the
             setup pre-condition is in place)
    FIRED    EMA now > 0.615 AND was <= 0.40 within last 10 sessions
             (full Zweig Thrust -- buy signal)
    COOLDOWN one calendar year after firing (no re-firing in this
             window)

Engine pulls Polygon grouped daily aggregates for the last ~15
trading sessions (caches in data/breadth-history.json to handle
rate limits across runs), computes daily advancing/declining ratio,
takes the 10-period EMA, evaluates state. Adds Whaley New Year
Barometer (first 5 trading days of January up -> bullish full year,
85% historical hit rate) and a simple Coppock Curve proxy
(monthly SPY ROC trend). Forward expectations computed live from
SPY history at curated historical trigger dates.

Output: data/breadth-thrust.json (consumed by breadth-thrust.html)
Schedule: daily 22:00 UTC (after US close).
"""

try:  # bank-first Polygon (aws/shared/polygon_bankfirst): /v2/aggs daily bars answered from the warehouse when complete, else live
    from polygon_bankfirst import install as _polygon_bankfirst_install
    _polygon_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
import gzip
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

import breadth_math as BM  # v1.1: the pure arithmetic (both bases, McClellan, the bank plan), tested on fixtures

# ---- infrastructure ---------------------------------------------------
S3_BUCKET = "justhodl-dashboard-live"
REPORT_KEY = "data/breadth-thrust.json"
CACHE_KEY = "data/breadth-history.json"
STATE_SSM = "/justhodl/breadth-thrust/state"
FMP_KEY = managed_secret(('FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
POLYGON_KEY = managed_secret(('POLYGON_KEY', 'POLYGON_API_KEY', 'POLY_KEY'), ("/justhodl/polygon/api-key",))
TELEGRAM_TOKEN = managed_secret(('TELEGRAM_TOKEN', 'TELEGRAM_BOT_TOKEN'), ("/justhodl/telegram/bot_token",))
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

# ---- Zweig thresholds (canonical) -------------------------------------
ZWEIG_LOW = 0.40
ZWEIG_HIGH = 0.615
ZWEIG_WINDOW = 10            # trading sessions
COOLDOWN_DAYS = 365

# ---- v1.1 (2026-11-26): the house bank of grouped sessions first -------
VERSION = "1.1"
GROUPED_ROOT = "data/warm/polygon-full/grouped/"      # YYYY/YYYY-MM-DD.json.gz, Polygon's own rows, banked 02:29 UTC
TODAY_KEY = "data/warm/polygon-full/today.json.gz"    # the forming session, kept by the five-minute lane
HISTORY_KEEP = 130       # sessions kept in the cache (60 before): McClellan's 39-session average needs the depth
ZWEIG_ROWS = 60          # the state machine reads the last 60 sessions, exactly as it did before
BANK_READS_PER_RUN = 140

# ---- curated Zweig historical trigger dates ---------------------------
HISTORICAL_TRIGGER_DATES = [
    ("1982-08-20", "1982 bull market launch"),
    ("1984-08-03", "1984 V-bottom"),
    ("1987-01-02", "post-1986 recovery"),
    ("2009-03-23", "GFC March 2009 bottom"),
    ("2015-10-05", "Sept 2015 V-bottom"),
    ("2019-01-09", "Q4 2018 recovery"),
    ("2020-04-08", "COVID V-bottom"),
    ("2023-11-03", "Oct 2023 reversal"),
]

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


def _fmp_get(path, params=None):
    p = dict(params or {})
    p["apikey"] = FMP_KEY
    url = ("https://financialmodelingprep.com/stable" + path +
           "?" + urllib.parse.urlencode(p))
    req = urllib.request.Request(url,
                                 headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def fetch_spy_history(years=20):
    """Proven fleet pattern: /stable/historical-price-eod/full in
    bounded ~5y windows (the old single 20y /light call returned
    empty -- root cause of Whaley PENDING-in-July, Coppock
    INSUFFICIENT_DATA, forwards n=0, episodes []). Polygon fallback
    (~5y entitlement) if FMP dies entirely."""
    now = datetime.now(timezone.utc)
    out = {}
    for back in range(years, 0, -5):
        frm = (now - timedelta(days=365 * back)).strftime("%Y-%m-%d")
        to = (now - timedelta(days=365 * max(0, back - 5))
              ).strftime("%Y-%m-%d")
        try:
            d = _fmp_get("/historical-price-eod/full",
                         {"symbol": "SPY", "from": frm, "to": to})
            rows = (d if isinstance(d, list)
                    else (d or {}).get("historical") or [])
            for r in rows:
                dt, cl = r.get("date"), (r.get("close")
                                         or r.get("price"))
                if dt and cl:
                    out[dt] = {"date": dt, "close": cl}
        except Exception as e:
            print("spy chunk %s..%s fail: %s" % (frm, to, e))
    if len(out) < 350:
        try:
            frm = (now - timedelta(days=365 * 5)).strftime("%Y-%m-%d")
            to = now.strftime("%Y-%m-%d")
            url = ("https://api.polygon.io/v2/aggs/ticker/SPY/range/1/"
                   "day/%s/%s?adjusted=true&sort=asc&limit=5000&apiKey="
                   "%s" % (frm, to, POLYGON_KEY))
            req = urllib.request.Request(url, headers={"User-Agent":
                                                       "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                for a in (json.loads(r.read()).get("results") or []):
                    dt = datetime.fromtimestamp(
                        a["t"] / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%d")
                    out.setdefault(dt, {"date": dt, "close": a["c"]})
        except Exception as e:
            print("polygon SPY fallback fail: %s" % e)
    # ops 4415: third fallback — FRED SP500 daily index. FMP chunking and the
    # Polygon 5y entitlement have both regressed to empty before (see docstring:
    # "forwards n=0, episodes []"). FRED SP500 is a different vendor entirely,
    # so a single-vendor outage can no longer zero out forward returns. Used as
    # an INDEX PROXY for forward-return math (levels differ from SPY, but the
    # percentage forward returns — which is all compute_forward_returns uses —
    # are equivalent). Flagged in the report as price_source.
    if len(out) < 350:
        try:
            fred_key = os.environ.get("FRED_API_KEY",
                                      managed_secret(('FRED_API_KEY', 'FRED_KEY'), ("/justhodl/fred/api-key",)))
            frm = (now - timedelta(days=365 * 10)).strftime("%Y-%m-%d")
            u = ("https://api.stlouisfed.org/fred/series/observations?"
                 "series_id=SP500&api_key=%s&file_type=json&"
                 "observation_start=%s" % (fred_key, frm))
            rq = urllib.request.Request(u, headers={"User-Agent":
                                                    "justhodl/1.0"})
            with urllib.request.urlopen(rq, timeout=25) as r:
                for o in (json.loads(r.read()).get("observations") or []):
                    v = o.get("value")
                    if v in (None, ".", ""):
                        continue
                    try:
                        out.setdefault(o["date"],
                                       {"date": o["date"], "close": float(v)})
                    except Exception:
                        continue
            print("FRED SP500 fallback rows: %d" % len(out))
        except Exception as e:
            print("FRED SP500 fallback fail: %s" % e)
    print("spy_history rows: %d" % len(out))
    return sorted(out.values(), key=lambda x: x["date"])


def price_at_or_after(history, target_iso):
    # ops 4415b: never extrapolate BACKWARDS. If the target predates the
    # history window, the old loop returned the earliest row — so a trigger
    # and its +12m exit both resolved to the SAME price, fabricating a 0.0%
    # forward return that diluted the win rate (Perplexity's ~12.5% finding).
    # Out-of-range targets must be None so summarise_forwards excludes them.
    if not history:
        return None
    if target_iso < history[0].get("date", ""):
        return None
    for row in history:
        if row.get("date", "") >= target_iso:
            return row.get("close") or row.get("price")
    return None


def fetch_polygon_grouped(date_str):
    """Returns list of {T (ticker), o (open), c (close), v (volume)}
    for all US stocks on the given date. Polygon stocks-only OK."""
    url = ("https://api.polygon.io/v2/aggs/grouped/locale/us/"
           "market/stocks/" + date_str
           + "?adjusted=true&apiKey=" + POLYGON_KEY)
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "justhodl/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
        return d.get("results") or []
    except Exception as e:
        print("polygon grouped fail %s: %s" % (date_str, e))
        return []


def compute_ad_ratio(grouped):
    """Daily advancing/declining ratio. Filters out illiquid names
    (volume < 100k) to focus on the institutional breadth signal."""
    adv = dec = 0
    for r in grouped:
        if (r.get("v") or 0) < 100000:
            continue
        op = r.get("o") or 0
        cl = r.get("c") or 0
        if cl > op:
            adv += 1
        elif cl < op:
            dec += 1
    if adv + dec == 0:
        return None, 0
    return adv / (adv + dec), adv + dec


def load_breadth_cache(with_notes=False):
    """The kept sessions; with_notes=True also returns the bank dates found not to be sessions."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=CACHE_KEY)
        data = json.loads(obj["Body"].read())
        if isinstance(data, dict) and "history" in data:
            return (data["history"], data.get("not_sessions") or []) if with_notes else data["history"]
    except Exception as e:
        print("breadth cache miss (first run?): %s" % e)
    return ([], []) if with_notes else []


def save_breadth_cache(history, not_sessions=None):
    body = {"history": history,
            "not_sessions": list(not_sessions or []),
            "as_of": datetime.now(timezone.utc).isoformat()}
    s3.put_object(Bucket=S3_BUCKET, Key=CACHE_KEY,
                  Body=json.dumps(body, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")


def bank_session_keys(n):
    """The last n banked sessions' keys, oldest first. The bank's own list of sessions: no weekday or holiday guessing."""
    now = datetime.now(timezone.utc)
    keys = []
    for yr in (now.year - 1, now.year):
        token = None
        while True:
            kw = {"Bucket": S3_BUCKET, "Prefix": "%s%d/" % (GROUPED_ROOT, yr)}
            if token:
                kw["ContinuationToken"] = token
            r = s3.list_objects_v2(**kw)
            keys.extend(o["Key"] for o in (r.get("Contents") or []) if o["Key"].endswith(".json.gz"))
            token = r.get("NextContinuationToken")
            if not token:
                break
    keys.sort()
    return keys[-n:]


def bank_session(key):
    """One banked session's rows ({T, o, c, v, ...}), or [] when it cannot be read."""
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        j = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
        return j.get("results") or []
    except Exception as e:
        print("bank session unreadable %s: %s" % (key, e))
        return []


def deepen_from_bank(history, not_sessions=(), keep=HISTORY_KEEP, budget=BANK_READS_PER_RUN):
    """Reads the house bank of grouped sessions, never the provider, and oldest first:
      * adds the sessions the cache lacks inside the last `keep` banked sessions (ratio on the same open-to-close basis),
      * gives each kept session its counts, its up-volume share and its day-over-day reading.
    A row's existing `ratio` is never rewritten. The bank is not taken on trust (dry run of 2026-11-26: it holds a
    Saturday file with two tradable rows, and one weekday file counting half the usual issues):
      * a weekend file, or a file counting fewer than MIN_SESSION_ISSUES issues, is not a session - no row, and the
        day-over-day comparison steps over it to the real session before;
      * a NEW session whose file is thin is left to the provider path, as before v1.1; an older thin one is kept and
        marked `thin`, and the day-over-day reading and McClellan leave it out;
      * a kept row is not given counts from a file thinner than the one its ratio was made from.
    Returns (history, not_sessions, stats)."""
    stats = {"sessions_in_window": 0, "files_read": 0, "sessions_added": 0, "sessions_completed": 0,
             "left_for_next_run": 0, "max_ratio_gap": None, "not_sessions": [], "thin": [], "error": None}
    skip = set(not_sessions or ())
    try:
        keys = bank_session_keys(keep + 1 + len(skip) + 12)
    except Exception as e:
        stats["error"] = str(e)[:160]
        print("bank listing failed: %s" % e)
        return history, sorted(skip), stats
    by_date = {k.rsplit("/", 1)[1][:10]: k for k in keys}
    for d in sorted(by_date):
        if not BM.is_weekday(d):
            stats["not_sessions"].append([d, "weekend file"])
    dates = [d for d in sorted(by_date) if BM.is_weekday(d) and d not in skip][-(keep + 1):]
    rows = {r["date"]: r for r in history}
    typical = BM.typical_issues(history)
    newest_kept = max(rows) if rows else ""
    work, read, left = BM.bank_plan(dates, rows, keep, budget)
    work = set(work)
    stats["sessions_in_window"] = len(dates)
    stats["left_for_next_run"] = left
    prev_i = prev_close = None
    stepped_over = set()
    gap = None
    for i, d in enumerate(dates):
        if d not in read:
            continue
        grouped = bank_session(by_date[d])
        stats["files_read"] += 1
        if not grouped:                       # unreadable this run: the chain of prior closes is broken here
            prev_i = prev_close = None
            continue
        det = BM.detail(grouped)
        n = det["adv"] + det["dec"]
        if n < BM.MIN_SESSION_ISSUES:         # a stray file: remembered, so later runs do not read it again
            skip.add(d)
            stepped_over.add(d)
            stats["not_sessions"].append([d, n])
            continue
        thin = typical is not None and n < BM.THIN_SHARE * typical
        row = rows.get(d)
        take = d in work
        if take and row is None:
            if thin and d > newest_kept:      # a new session from a thin file: the provider path adds it instead
                stats["thin"].append([d, n, "left to the provider"])
                take = False
            else:
                ratio, cnt = compute_ad_ratio(grouped)
                row = {"date": d, "ratio": round(ratio, 4), "n_stocks": cnt, "source": "bank"}
                if thin:
                    row["thin"] = True
                    stats["thin"].append([d, n, "kept, marked thin"])
                rows[d] = row
                stats["sessions_added"] += 1
                row.update(det)
        elif take:
            if row.get("n_stocks") and n < 0.9 * row["n_stocks"]:
                row["bank_thin"] = True       # the bank's file is thinner than the one this ratio came from
                row.setdefault("adv_dod", None)
                stats["thin"].append([d, n, "counts not taken: the kept ratio counted %s" % row["n_stocks"]])
                take = False
            else:
                g = abs(row["ratio"] - det["adv"] / float(n))
                gap = g if gap is None else max(gap, g)
                row.update(det)
        if take:
            if prev_close is not None and all(x in stepped_over for x in dates[prev_i + 1:i]):
                row.update(BM.day_over_day(grouped, prev_close))
            stats["sessions_completed"] += 1
        prev_i, prev_close = i, BM.closes_of(grouped)
    stats["max_ratio_gap"] = round(gap, 4) if gap is not None else None
    return sorted(rows.values(), key=lambda r: r["date"]), sorted(skip)[-60:], stats


def preliminary_reading(settled_rows):
    """Today's session from the house forming-bar file, set against the settled sessions. Information only."""
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=TODAY_KEY)["Body"].read()
        doc = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
    except Exception as e:
        print("forming-bar file unreadable: %s" % e)
        return None
    try:
        return BM.preliminary(doc, settled_rows, ZWEIG_LOW, ZWEIG_HIGH, ZWEIG_WINDOW)
    except Exception as e:
        print("preliminary reading failed: %s" % e)
        return None


def update_breadth_cache(history, target_days=15):
    """Fetch missing trading days. Walk back from yesterday."""
    have_dates = {r["date"] for r in history}
    needed = []
    cur = datetime.now(timezone.utc) - timedelta(days=1)
    walked = 0
    while len(needed) < target_days and walked < 40:
        if cur.weekday() < 5:
            d_str = cur.strftime("%Y-%m-%d")
            if d_str not in have_dates:
                needed.append(d_str)
        cur -= timedelta(days=1)
        walked += 1

    fetched = 0
    for d_str in needed:
        if fetched >= 5:
            # Polygon free-tier safety: 5 fresh fetches per run max
            break
        grouped = fetch_polygon_grouped(d_str)
        if grouped:
            ratio, n = compute_ad_ratio(grouped)
            if ratio is not None:
                row = {
                    "date": d_str,
                    "ratio": round(ratio, 4),
                    "n_stocks": n,
                }
                row["source"] = "polygon"       # the bank had no file for this session yet
                row.update(BM.detail(grouped))
                history.append(row)
                fetched += 1
    history.sort(key=lambda r: r["date"])
    # Keep the last HISTORY_KEEP sessions (60 before v1.1)
    if len(history) > HISTORY_KEEP:
        history = history[-HISTORY_KEEP:]
    return history, fetched


def compute_ema(values, period):
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    ema = values[0]
    out = [ema]
    for v in values[1:]:
        ema = alpha * v + (1 - alpha) * ema
        out.append(ema)
    return out


def evaluate_zweig(history):
    """Walk through breadth history, compute 10d EMA, check for thrust."""
    if len(history) < ZWEIG_WINDOW + 1:
        return {"state": "INSUFFICIENT_DATA",
                "current_ema": None, "min_in_window": None,
                "max_in_window": None}
    sorted_hist = sorted(history, key=lambda r: r["date"])
    ratios = [r["ratio"] for r in sorted_hist]
    emas = compute_ema(ratios, ZWEIG_WINDOW)
    cur_ema = emas[-1]
    win = emas[-ZWEIG_WINDOW - 1:]
    win_min = min(win)
    win_max = max(win)

    # Zweig fires if EMA above 0.615 AND a value below 0.40 was in last
    # ZWEIG_WINDOW sessions.
    fired = cur_ema > ZWEIG_HIGH and win_min < ZWEIG_LOW
    armed = (not fired) and win_min < ZWEIG_LOW
    state = "FIRED" if fired else "ARMED" if armed else "NULL"
    return {
        "state": state,
        "current_ema": round(cur_ema, 4),
        "min_in_window": round(win_min, 4),
        "max_in_window": round(win_max, 4),
        "thresholds": {"low": ZWEIG_LOW, "high": ZWEIG_HIGH},
        "window_sessions": ZWEIG_WINDOW,
    }


def evaluate_whaley(spy_history):
    """First 5 trading days of January. If SPY return positive,
    full-year tends positive (~85% hit rate, Whaley)."""
    year = datetime.now(timezone.utc).year
    jan = [r for r in spy_history if r.get("date", "").startswith(
        "%d-01" % year)]
    if len(jan) < 5:
        return {"state": "PENDING", "first_5d_return_pct": None,
                "note": "less than 5 January sessions so far"}
    open_close = (jan[0].get("close") or jan[0].get("price"))
    end_close = (jan[4].get("close") or jan[4].get("price"))
    if not open_close or not end_close:
        return {"state": "MISSING_DATA"}
    ret_pct = (end_close - open_close) / open_close * 100
    return {
        "state": "BULLISH" if ret_pct > 0 else "BEARISH",
        "first_5d_return_pct": round(ret_pct, 2),
        "historical_hit_rate_pct": 85,
        "year": year,
    }


def evaluate_coppock(spy_history):
    """Coppock proxy: smoothed momentum signal. Buy when 10-month
    weighted MA of (14m ROC + 11m ROC) turns positive from negative.
    We compute monthly closes from daily, then ROCs."""
    if len(spy_history) < 350:
        return {"state": "INSUFFICIENT_DATA"}
    # Aggregate to monthly closes (last close of each month)
    monthly = {}
    for r in spy_history:
        d = r.get("date", "")
        if len(d) < 7:
            continue
        ym = d[:7]
        monthly[ym] = r.get("close") or r.get("price")
    ms = sorted(monthly.items())
    if len(ms) < 25:
        return {"state": "INSUFFICIENT_DATA"}
    closes = [c for _, c in ms]
    # 14m ROC and 11m ROC
    roc14 = [None] * 14 + [
        (closes[i] - closes[i - 14]) / closes[i - 14] * 100
        for i in range(14, len(closes))]
    roc11 = [None] * 11 + [
        (closes[i] - closes[i - 11]) / closes[i - 11] * 100
        for i in range(11, len(closes))]
    sums = [
        (roc14[i] + roc11[i]) if roc14[i] is not None
        and roc11[i] is not None else None
        for i in range(len(closes))]
    # 10-month WMA
    wma = []
    for i in range(len(sums)):
        if i < 14 + 9:
            wma.append(None)
            continue
        window = sums[i - 9:i + 1]
        if any(s is None for s in window):
            wma.append(None)
            continue
        weights = list(range(1, 11))
        weighted = sum(w * s for w, s in zip(weights, window))
        denom = sum(weights)
        wma.append(weighted / denom)
    cur = wma[-1] if wma else None
    prev = wma[-2] if len(wma) >= 2 else None
    crossed_up = (cur is not None and prev is not None
                  and prev <= 0 and cur > 0)
    return {
        "state": ("BUY_CROSSOVER" if crossed_up
                  else "POSITIVE" if (cur or 0) > 0
                  else "NEGATIVE"),
        "current_value": round(cur, 2) if cur is not None else None,
        "previous_value": round(prev, 2) if prev is not None else None,
        "fresh_buy_signal": crossed_up,
    }


def compute_forward_returns(spy_history, trigger_dates):
    rows = []
    for date_iso, label in trigger_dates:
        try:
            d0 = datetime.strptime(date_iso, "%Y-%m-%d")
            p0 = price_at_or_after(spy_history, date_iso)
            if not p0:
                continue
            d_1m = (d0 + timedelta(days=30)).strftime("%Y-%m-%d")
            d_3m = (d0 + timedelta(days=90)).strftime("%Y-%m-%d")
            d_6m = (d0 + timedelta(days=180)).strftime("%Y-%m-%d")
            d_12m = (d0 + timedelta(days=365)).strftime("%Y-%m-%d")
            p_1m = price_at_or_after(spy_history, d_1m)
            p_3m = price_at_or_after(spy_history, d_3m)
            p_6m = price_at_or_after(spy_history, d_6m)
            p_12m = price_at_or_after(spy_history, d_12m)
            rows.append({
                "date": date_iso,
                "label": label,
                "spy_at_trigger": round(p0, 2),
                "fwd_1m_pct": (round((p_1m - p0) / p0 * 100, 2)
                               if p_1m else None),
                "fwd_3m_pct": (round((p_3m - p0) / p0 * 100, 2)
                               if p_3m else None),
                "fwd_6m_pct": (round((p_6m - p0) / p0 * 100, 2)
                               if p_6m else None),
                "fwd_12m_pct": (round((p_12m - p0) / p0 * 100, 2)
                                if p_12m else None),
            })
        except Exception as e:
            print("fwd fail %s: %s" % (date_iso, e))
    return rows


def summarise_forwards(rows):
    def stats(key):
        vals = [r[key] for r in rows if r.get(key) is not None]
        if not vals:
            return {"return_pct": None, "win_rate_pct": None, "n": 0}
        avg = sum(vals) / len(vals)
        wins = sum(1 for v in vals if v > 0)
        srt = sorted(vals)
        return {
            "return_pct": round(avg, 2),
            "win_rate_pct": round(wins / len(vals) * 100, 1),
            "n": len(vals),
            "median_pct": round(srt[len(srt) // 2], 2),
            "best_pct": round(max(vals), 2),
            "worst_pct": round(min(vals), 2),
        }
    return {
        "1m": dict(stats("fwd_1m_pct"),
                   basis="SPY next 30 calendar days"),
        "3m": dict(stats("fwd_3m_pct"),
                   basis="SPY next 90 calendar days"),
        "6m": dict(stats("fwd_6m_pct"),
                   basis="SPY next 180 calendar days"),
        "12m": dict(stats("fwd_12m_pct"),
                    basis="SPY next 365 calendar days"),
    }


def load_persistent_state():
    try:
        p = ssm.get_parameter(Name=STATE_SSM)
        return json.loads(p["Parameter"]["Value"])
    except Exception:
        return {"state": "NULL", "state_since": None, "last_fired": None}


def save_persistent_state(d):
    try:
        ssm.put_parameter(Name=STATE_SSM,
                          Value=json.dumps(d),
                          Type="String", Overwrite=True)
    except Exception as e:
        print("state save fail: %s" % e)


def build_trade_ticket(state, fwd):
    if state == "FIRED":
        r3 = fwd.get("3m", {}).get("return_pct")
        r12 = fwd.get("12m", {}).get("return_pct")
        wr12 = fwd.get("12m", {}).get("win_rate_pct")
        return {
            "primary": {
                "instrument": ("SPY or QQQ shares for a 6-12 month "
                               "hold; or IWM 365-day calls for "
                               "leveraged small-cap exposure"),
                "thesis": ("Zweig Thrust marks the inflection from "
                           "broad capitulation to broad "
                           "accumulation. Every historical firing "
                           "produced positive 12m forward returns. "
                           "Size for long hold."),
                "size_guidance": "10-20% of risk capital allocated",
                "max_loss": ("approx -6%% historical max drawdown "
                             "post-firing"),
                "expected_horizon": "6-12 months",
                "expected_return_basis": ("Historical 12m avg +%s%% "
                                          "(win rate %s%%)" % (
                                            r12, wr12)),
            },
            "leveraged_alt": {
                "instrument": ("IWM 365-day calls 5-10% OTM, "
                               "OR TQQQ (3x QQQ) shares"),
                "thesis": ("Small caps and high-beta names "
                           "outperform large caps after a Zweig "
                           "Thrust historically."),
                "size_guidance": "2-5% of risk capital",
                "max_loss": "premium (options) or 3x drawdown (TQQQ)",
                "expected_horizon": "6-12 months",
            },
            "exit_rules": [
                "Take 1/3 off at +15% gain",
                "Take next 1/3 at +30% gain",
                "Let final 1/3 ride for 12 months",
                ("Hard stop only if SPY breaks the firing-day low "
                 "(signal was wrong, rare)"),
            ],
        }
    if state == "ARMED":
        return {
            "primary": {
                "instrument": "Hold dry powder. Optional small starter.",
                "thesis": ("Broad-market breadth is oversold. Setup "
                           "is in place. Watch for EMA to cross "
                           "above 0.615 within 10 sessions."),
                "size_guidance": "0-3% of risk capital deployed",
                "max_loss": "small",
                "expected_horizon": "watch next 1-3 weeks",
            }
        }
    return {
        "primary": {
            "instrument": "No trade. Normal regime.",
            "thesis": ("Breadth EMA in normal range. Zweig Thrust "
                       "fires only after broad capitulation. This "
                       "is a once-per-4-7-years event."),
            "size_guidance": "0%",
            "max_loss": "n/a",
            "expected_horizon": "indeterminate",
        }
    }


def build_why_now(state, zweig, whaley, coppock, fwd, n_hist):
    if state == "FIRED":
        return (
            "**FIRED -- ZWEIG BREADTH THRUST.** The 10-day EMA of "
            "advancing-issue ratio crossed from below "
            "%(low).2f to above %(high).2f within %(win)d trading "
            "sessions. This is one of the rarest and most violent "
            "signals in equity history.\n\n"
            "**Historical performance** across %(n)d analogues "
            "computed from SPY history: avg **+%(f3)+0.1f%% over 3 "
            "months**, **+%(f6)+0.1f%% over 6 months**, "
            "**+%(f12)+0.1f%% over 12 months** (win rate "
            "%(wr12).0f%%). Every historical firing since the 1980s "
            "produced positive 12-month returns.\n\n"
            "**Why this is the floor:** Zweig Thrust identifies the "
            "precise moment when institutional money turns on "
            "simultaneously after broad capitulation. After "
            "weeks-to-months of indiscriminate selling, breadth "
            "reverses with violence -- and that violence cannot be "
            "faked. The signal works because it requires hundreds "
            "of stocks to rally simultaneously, which only happens "
            "when forced selling exhausts and dip-buyers move "
            "in coordinated.\n\n"
            "**Supporting signals:** Whaley %(whaley_s)s "
            "(%(whaley_r)s%% first-5d), Coppock %(copp_s)s "
            "(%(copp_v)s)." % {
                "low": ZWEIG_LOW, "high": ZWEIG_HIGH,
                "win": ZWEIG_WINDOW, "n": n_hist,
                "f3": fwd["3m"].get("return_pct") or 0,
                "f6": fwd["6m"].get("return_pct") or 0,
                "f12": fwd["12m"].get("return_pct") or 0,
                "wr12": fwd["12m"].get("win_rate_pct") or 0,
                "whaley_s": whaley.get("state"),
                "whaley_r": whaley.get("first_5d_return_pct"),
                "copp_s": coppock.get("state"),
                "copp_v": coppock.get("current_value"),
            })
    if state == "ARMED":
        return (
            "**ARMED.** Breadth is in oversold territory (10-day "
            "EMA recently below %(low).2f). The Zweig setup is in "
            "place. The signal **fires** if the EMA crosses above "
            "%(high).2f within %(win)d trading sessions.\n\n"
            "**What to do:** Stay flat or build a small starter "
            "position. Most ARMED setups DO eventually fire if "
            "they hit the lower threshold; the question is timing. "
            "Watch this dashboard daily." % {
                "low": ZWEIG_LOW, "high": ZWEIG_HIGH,
                "win": ZWEIG_WINDOW})
    return (
        "**NULL -- no setup.** Breadth EMA is in normal range. "
        "The Zweig Thrust requires broad capitulation first "
        "(10-day EMA <= %(low).2f), then violent reversal "
        "above %(high).2f within %(win)d sessions.\n\n"
        "This signal fires once every 4-7 years. It is "
        "extraordinarily rare and extraordinarily reliable. "
        "Until then, the dashboard waits.\n\n"
        "**Whaley:** %(ws)s (first 5 days of Jan %(wr)s%%). "
        "**Coppock:** %(cs)s (%(cv)s)." % {
            "low": ZWEIG_LOW, "high": ZWEIG_HIGH,
            "win": ZWEIG_WINDOW,
            "ws": whaley.get("state"),
            "wr": whaley.get("first_5d_return_pct"),
            "cs": coppock.get("state"),
            "cv": coppock.get("current_value")})


def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = ("https://api.telegram.org/bot" + TELEGRAM_TOKEN +
               "/sendMessage")
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID, "text": msg,
            "parse_mode": "Markdown",
        }).encode("utf-8")
        urllib.request.urlopen(url, data=data, timeout=8).read()
    except Exception as e:
        print("telegram fail: %s" % e)


def lambda_handler(event, context):
    as_of = datetime.now(timezone.utc).isoformat()
    t0 = time.time()
    # {"dry_run": true}: everything is read and computed, nothing is written (no cache, no state, no report, no alert)
    dry = bool(isinstance(event, dict) and event.get("dry_run"))

    # ---- 1. Update breadth history cache: the house bank first, the provider only for a session the bank lacks ----
    history, not_sessions = load_breadth_cache(with_notes=True)
    n_before = len(history)
    history, not_sessions, bank = deepen_from_bank(history, not_sessions)
    history, fetched = update_breadth_cache(history)
    if not dry:
        save_breadth_cache(history, not_sessions)
    print("breadth cache: %d entries (%d before), %d from the bank, %d fetched live, %d bank files read"
          % (len(history), n_before, bank.get("sessions_added") or 0, fetched, bank.get("files_read") or 0))

    # ---- 2. Zweig state: the last ZWEIG_ROWS sessions on the open-to-close ratio, as before v1.1 ----
    zweig_rows = [r for r in sorted(history, key=lambda r: r["date"]) if not r.get("thin")][-ZWEIG_ROWS:]
    zweig = evaluate_zweig(zweig_rows)
    zweig_state = zweig.get("state", "NULL")
    last_row = zweig_rows[-1] if zweig_rows else {}
    day_over_day = BM.day_over_day_reading(history, ZWEIG_LOW, ZWEIG_HIGH, ZWEIG_WINDOW, ZWEIG_ROWS)
    mcclellan = BM.mcclellan(history)
    preliminary = preliminary_reading(zweig_rows)

    # ---- 3. SPY history for Whaley/Coppock/forward-returns ----
    spy_hist = fetch_spy_history(20)
    whaley = evaluate_whaley(spy_hist)
    coppock = evaluate_coppock(spy_hist)

    # ---- 4. Cooldown ----
    persistent = load_persistent_state()
    prev_state = persistent.get("state", "NULL")
    last_fired = persistent.get("last_fired")
    cooldown_until = None
    if last_fired:
        try:
            lf = datetime.fromisoformat(last_fired.replace("Z", "+00:00"))
            cd_end = lf + timedelta(days=COOLDOWN_DAYS)
            if datetime.now(timezone.utc) < cd_end:
                cooldown_until = cd_end.isoformat()
                if zweig_state == "FIRED":
                    zweig_state = "COOLDOWN"
        except Exception:
            pass

    state_transition = None
    if zweig_state != prev_state:
        state_transition = "%s -> %s" % (prev_state, zweig_state)
        if zweig_state == "FIRED":
            persistent["last_fired"] = as_of
    persistent["state"] = zweig_state
    if zweig_state != prev_state:
        persistent["state_since"] = as_of
    if not dry:
        save_persistent_state(persistent)

    # ---- 5. Forward expectations ----
    history_rows = compute_forward_returns(
        spy_hist, HISTORICAL_TRIGGER_DATES)
    forward_expectations = summarise_forwards(history_rows)
    # ops 4415b: expose the per-trigger rows (Perplexity could not see
    # spy_at_trigger per episode; now they are in the feed explicitly)
    trigger_history = history_rows

    # ---- 6. Trade ticket + explainer ----
    trade = build_trade_ticket(zweig_state, forward_expectations)
    why_now = build_why_now(zweig_state, zweig, whaley, coppock,
                            forward_expectations, len(history_rows))

    # ---- 7. Signal strength derived from state + supporting signals ----
    sig = 0
    if zweig_state == "FIRED":
        sig = 90 + (5 if whaley.get("state") == "BULLISH" else 0) + (
            5 if coppock.get("fresh_buy_signal") else 0)
    elif zweig_state == "ARMED":
        sig = 50
    elif zweig_state == "COOLDOWN":
        sig = 30
    elif whaley.get("state") == "BULLISH":
        sig = 25
    if coppock.get("fresh_buy_signal"):
        sig = max(sig, 40)
    sig = min(sig, 100)

    body = {
        "engine": "breadth-thrust",
        "trigger_history": trigger_history,
        "price_source_note": ("forward returns use SPY where available, "
                              "FRED SP500 index as vendor-independent "
                              "fallback; triggers predating the price "
                              "window are excluded, not zero-filled"),
        "version": VERSION,
        # v1.1 (audit of 2026-11-26): said plainly, because it is not what the name suggests.
        "breadth_basis": ("The state follows an OPEN-TO-CLOSE ratio: an issue counts as rising when it closes above "
                          "its own open that session (volume of 100,000 shares or more; every US-listed stock and "
                          "fund in the session file). Zweig's measure is day-over-day: NYSE issues closing above the "
                          "PRIOR close. The thresholds 0.40 and 0.615 are his. From v1.1 the day-over-day reading, "
                          "the counts, the share of volume in rising issues and a McClellan oscillator are kept "
                          "beside it as information; they move no state and send no alert."),
        "breadth_today": dict((k, last_row.get(k)) for k in (
            "date", "ratio", "n_stocks", "adv", "dec", "unch", "up_vol_share", "ratio_dod", "adv_dod", "dec_dod",
            "unch_dod", "new_dod", "set_aside_dod", "up_vol_share_dod", "source")),
        "day_over_day": day_over_day,
        "mcclellan": mcclellan,
        "preliminary": preliminary,
        "as_of": as_of,
        "state": zweig_state,
        "prev_state": prev_state,
        "state_since": persistent.get("state_since"),
        "state_transition": state_transition,
        "signal_strength": sig,
        "cooldown_until": cooldown_until,
        "current_readings": {
            "zweig_10d_ema": zweig.get("current_ema"),
            "zweig_window_min": zweig.get("min_in_window"),
            "zweig_window_max": zweig.get("max_in_window"),
            "zweig_thresholds": zweig.get("thresholds"),
            "n_breadth_days_cached": len(history),
            "zweig_sessions_read": len(zweig_rows),
            "bank": bank,
            "spy_history_n": len(spy_hist),
            "newly_fetched_this_run": fetched,
            "whaley": whaley,
            "coppock": coppock,
        },
        "trigger_conditions": [
            {"name": "10d EMA recently <= %.2f (oversold)" % ZWEIG_LOW,
             "current": "min %s" % zweig.get("min_in_window"),
             "satisfied": (zweig.get("min_in_window") or 1) < ZWEIG_LOW,
             "weight": 50},
            {"name": "10d EMA now > %.3f (thrust)" % ZWEIG_HIGH,
             "current": "current %s" % zweig.get("current_ema"),
             "satisfied": (zweig.get("current_ema") or 0) > ZWEIG_HIGH,
             "weight": 50},
        ],
        "forward_expectations": forward_expectations,
        "recommended_trade": trade,
        "why_now_explainer": why_now,
        "historical_episodes": history_rows,
        "supporting_signals": {
            "whaley_january_barometer": whaley,
            "coppock_curve": coppock,
        },
        "methodology": (
            "Grouped daily session files (the house bank, "
            "data/warm/polygon-full/grouped; the provider only for a "
            "session the bank lacks) -> daily open-to-close rising/"
            "falling ratio (filter v>=100k) -> 10-period EMA over "
            "the last 60 sessions. Zweig: EMA crosses from <0.40 to "
            ">0.615 within 10 trading sessions. Forward expectations "
            "computed live from 20y SPY history at curated historical "
            "trigger dates. Cache: data/breadth-history.json, the "
            "last 130 sessions with each session's counts on both "
            "bases."),
        "sources": [
            "house bank data/warm/polygon-full/grouped (Polygon grouped daily, banked 02:29 UTC)",
            "house forming-bar file data/warm/polygon-full/today.json.gz (preliminary reading only)",
            "Polygon /v2/aggs/grouped/locale/us/market/stocks (fallback for a session the bank lacks)",
            "FMP /stable/historical-price-eod (SPY for forwards)",
            "academic: Martin Zweig 'Winning on Wall Street' (1986)",
        ],
        "schedule": "daily 22:00 UTC (after US close)",
    }

    body["wl_research"] = __import__("wl_fusion").block(('BREADTH',))
    body["run_seconds"] = round(time.time() - t0, 1)

    if dry:
        rows = sorted(history, key=lambda r: r["date"])
        return {"statusCode": 200, "dry_run": True, "body": json.dumps({
            "report": body,
            "history": {"n": len(rows), "n_before": n_before,
                        "first": rows[0]["date"] if rows else None, "last": rows[-1]["date"] if rows else None,
                        "with_counts": sum(1 for r in rows if "adv" in r),
                        "with_day_over_day": sum(1 for r in rows if r.get("ratio_dod") is not None),
                        "from_bank": sum(1 for r in rows if r.get("source") == "bank"),
                        "not_sessions": not_sessions,
                        "rows": rows}}, default=str)}

    s3.put_object(Bucket=S3_BUCKET, Key=REPORT_KEY,
                  Body=json.dumps(body, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")

    if zweig_state == "FIRED" and state_transition \
            and state_transition.endswith("-> FIRED"):
        f12 = forward_expectations.get("12m", {})
        msg = (
            "*ZWEIG BREADTH THRUST FIRED*\n\n"
            "Once-per-4-7-years signal.\n"
            "10d EMA: %s -> %s (min in window %s)\n\n"
            "Historical fwd 12m: +%s%% (win rate %s%%, N=%s past firings "
            "in the record)\n\n"
            "Dashboard: https://justhodl.ai/breadth-thrust.html"
            % (ZWEIG_LOW, zweig.get("current_ema"),
               zweig.get("min_in_window"),
               f12.get("return_pct"),
               f12.get("win_rate_pct"),
               f12.get("n")))
        send_telegram(msg)

    print("breadth-thrust: state=%s ema=%s whaley=%s coppock=%s "
          "sig=%s n_hist=%s" % (
              zweig_state, zweig.get("current_ema"),
              whaley.get("state"), coppock.get("state"),
              sig, len(history_rows)))
    return {"statusCode": 200, "body": json.dumps(body, default=str)}

# redeploy-trigger: 2026-05-20
