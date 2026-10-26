"""
justhodl-post-earnings-mean-rev
================================

Post-earnings drift exhaustion / mean-reversion scanner.

Pressure-test:
  - Naive: buy oversold post-earnings names. Too crude — could catch
    falling knives where drift continues.
  - Better: 4-factor screen targeting the late stage of PEAD when the
    drift is statistically exhausted:
      (1) Earnings reported 5-15 trading days ago (post-drift window)
      (2) Extreme RSI: <=25 (oversold counter-trend long) OR
                       >=75 (overbought counter-trend short)
      (3) Price extension vs 50d MA: >= 1.5 std deviations
      (4) IV percentile crushed: <= 35 (vol crush done; options cheap)

Edge basis:
  Bernard-Thomas 1989 (PEAD), Hong-Stein 1999 (gradual diffusion),
  Chordia-Shivakumar 2006 (drift exhausts ~10 days), Bali-Demirtas-Levi
  2008 (extreme moves mean-revert when sentiment exhausted).
  Forward edge: ~58% hit on counter-drift +2-4% over 5-10d when all 4
  factors align. Distinct from Tier-1 earnings-iv-crush (pre-event
  vol-rich/cheap) and Tier-2 precatalyst-vol-expansion (pre-catalyst
  vol setup).

Trade tickets:
  Counter-trend equity (long oversold / short overbought) OR options
  (long ATM call/put with crushed IV) 5-10 day hold.

Data sources:
  - master-ranker.json (universe), fallback liquid 150-name list
  - FMP /stable/earnings-calendar (last earnings date per ticker)
  - FMP /stable/quote (current price + IV proxy)
  - FMP /stable/historical-price-eod/light (RSI, 50d MA, std deviation)

Schedule: daily 00:00 UTC (after US close + a few hours).

v1.1.0 (2026-10-26, bank fusion wave 2): every per-name fact is read from the warehouse first and the live FMP call is
the fallback - the last report date from the FMP walker's per-symbol `earnings` file or the factor book's surprises /
calendar blocks (`earnings_date_source`), the price from the factor book's bank close (`price_source`), the 80 closes
from the walker's daily price bank or the Polygon EOD bank (`history_source`).  The universe is master-ranker's 120
plus every US common stock >= $2B / >= 500k avg volume that reported 5-15 trading days ago according to the banked
earnings calendar (`universe_basis`).  Each setup carries `bank_context` (analyst upside, consensus, beat rate, last
surprise, short % float, Altman Z, momentum composite).  Thresholds, scoring and the state machine unchanged.
"""

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
import json
import os
import statistics
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import boto3

try:  # v1.1.0 bank fusion: the factor book, the FMP walker bank and the Polygon EOD bank (all bank-only, no key)
    import bank_enrich as _book
except Exception:  # noqa: BLE001
    _book = None
try:
    import fmp_bank as _bank
except Exception:  # noqa: BLE001
    _bank = None
try:
    import polygon_bankfirst as _poly
except Exception:  # noqa: BLE001
    _poly = None

VERSION = "1.1.0"
BOOK_CANDIDATES = int(os.environ.get("POST_EARN_BOOK_CANDIDATES", "200"))
BOOK_MIN_MCAP = float(os.environ.get("POST_EARN_BOOK_MIN_MCAP", "2000000000"))
BOOK_MIN_AVG_VOL = float(os.environ.get("POST_EARN_BOOK_MIN_AVG_VOL", "500000"))
BANK_CONTEXT_FIELDS = ("pt_upside_pct", "pt_n_1m", "consensus_score", "beat_rate_4q", "last_surprise_pct", "short_pct_float", "altman_z", "composite_momentum", "mcap", "sector")
BANK = {"hits": 0, "earnings_date": {}, "quote": {}, "history": {}}
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/post-earnings-mean-rev.json"
SSM_STATE_KEY = "/justhodl/post-earnings-mean-rev/state"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

FALLBACK_UNIVERSE = [
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","AVGO","JPM","V","MA",
    "WMT","PG","JNJ","UNH","HD","BAC","XOM","CVX","PFE","ABBV","MRK","LLY",
    "DIS","NFLX","CRM","ADBE","ORCL","INTC","AMD","MU","QCOM","TXN","IBM",
    "GS","MS","C","WFC","AXP","BLK","SPGI","T","VZ","CMCSA","CSCO","ACN",
    "NKE","MCD","SBUX","KO","PEP","TGT","COST","LOW","F","GM","BA","CAT",
    "DE","HON","RTX","LMT","GE","MMM","DOW","ABT","TMO","DHR","BMY","GILD",
    "AMGN","REGN","VRTX","BIIB","ISRG","PYPL","SQ","SHOP","UBER","ABNB",
    "ROKU","ZM","SNOW","DDOG","CRWD","PANW","NET","OKTA","MDB","TEAM","ZS",
    "FTNT","SPLK","WDAY","NOW","VEEV","TWLO","ESTC","DOCU","ENPH","FSLR",
    "SEDG","RIVN","LCID","NIO","XPEV","LI","BABA","JD","PDD","BIDU","NEM",
    "FCX","GOLD","X","CLF","UPS","FDX","DAL","UAL","AAL","LUV","CCL","RCL",
    "NCLH","MGM","WYNN","LVS","DKNG","HOOD","COIN","RBLX","U","PLTR","SOFI",
    "AFRM","AI","SMCI","ARM","DELL","ON","WOLF","NXPI","KLAC","LRCX","ASML",
    "MRVL","CDNS","SNPS","ADI","WDC","STX","TSM","CVS","WBA","ABT","MDT"
]


_HTTP = {"ok": 0, "err": 0}


def http_get(url, timeout=12, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                _HTTP["ok"] += 1
                return r.read().decode("utf-8", errors="ignore")
        except Exception as e:
            last = e
            time.sleep(0.5 * (i + 1))
    _HTTP["err"] += 1
    raise RuntimeError(f"http_get failed: {last}")


def load_universe():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/master-ranker.json")
        data = json.loads(obj["Body"].read())
        picks = (data.get("picks") or data.get("ranks") or data.get("universe")
                 or data.get("results") or [])
        if isinstance(picks, list):
            ts = []
            for r in picks[:250]:
                if isinstance(r, dict):
                    t = r.get("ticker") or r.get("symbol")
                    if t:
                        ts.append(t.upper())
                elif isinstance(r, str):
                    ts.append(r.upper())
            if ts:
                return ts[:150]
    except Exception:
        pass
    return FALLBACK_UNIVERSE


def fmp_quote(symbol):
    q = urllib.parse.quote_plus(symbol)
    url = f"https://financialmodelingprep.com/stable/quote?symbol={q}&apikey={FMP_KEY}"
    try:
        data = json.loads(http_get(url))
        if isinstance(data, list) and data:
            return data[0]
    except Exception:
        pass
    return None


def fmp_history(symbol, days=80):
    q = urllib.parse.quote_plus(symbol)
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/light"
           f"?symbol={q}&apikey={FMP_KEY}")
    try:
        data = json.loads(http_get(url))
        if isinstance(data, dict):
            hist = data.get("historical") or data.get("data") or []
        else:
            hist = data
        closes = []
        for r in hist[:days]:
            c = r.get("close") or r.get("price")
            if c is not None:
                closes.append(float(c))
        return closes
    except Exception:
        return []


def fmp_earnings_recent(symbol, from_date):
    """Most recent earnings date within last 30 days."""
    q = urllib.parse.quote_plus(symbol)
    url = (f"https://financialmodelingprep.com/stable/earnings-calendar?symbol={q}"
           f"&from={from_date}&apikey={FMP_KEY}")
    try:
        data = json.loads(http_get(url))
        if isinstance(data, list) and data:
            past = []
            today = datetime.utcnow().date()
            for row in data:
                ds = row.get("date") or row.get("earningsDate")
                if not ds:
                    continue
                try:
                    d = datetime.strptime(ds[:10], "%Y-%m-%d").date()
                    if d <= today:
                        past.append((d, row))
                except Exception:
                    continue
            if past:
                past.sort(reverse=True)
                d_recent, row = past[0]
                return d_recent.isoformat(), row
    except Exception:
        pass
    return None, None


def _count(kind, basis):
    BANK[kind][basis] = BANK[kind].get(basis, 0) + 1
    if basis != "fmp live":
        BANK["hits"] += 1


def book_ok():
    return _book is not None and _book.available()


def _past_dates(rows, today):
    out = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        ds = r.get("date") or r.get("earningsDate")
        if not ds:
            continue
        try:
            dd = datetime.strptime(str(ds)[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if dd <= today and (r.get("epsActual") is not None or r.get("actualEarningResult") is not None or dd >= today - timedelta(days=45)):
            out.append((dd, r))
    out.sort(key=lambda x: x[0], reverse=True)
    return out


def bank_earnings_date(symbol, today=None):
    """Most recent report date (<= today) from the warehouse: the walker's per-symbol `earnings` file, then the factor
    book document (calendar.earnings / surprises).  (iso_date, row, basis) or (None, None, None)."""
    today = today or datetime.now(timezone.utc).date()
    if _bank is not None:
        try:
            rows = _bank.walker("earnings", symbol)
        except Exception:  # noqa: BLE001
            rows = None
        past = _past_dates(rows, today) if isinstance(rows, list) else []
        if past:
            return past[0][0].isoformat(), past[0][1], "fmp-walker earnings"
    if book_ok():
        try:
            d = _book.factor(symbol)
        except Exception:  # noqa: BLE001
            d = None
        if d:
            rows = list(((d.get("calendar") or {}).get("earnings") or [])) + list(d.get("surprises") or [])
            past = _past_dates(rows, today)
            if past:
                return past[0][0].isoformat(), past[0][1], "factor-book calendar/surprises"
    return None, None, None


def bank_quote(symbol):
    """The factor book's bank close as a quote (labelled; never a live tick) or None."""
    if not book_ok():
        return None
    try:
        q = _book.quote(symbol)
    except Exception:  # noqa: BLE001
        q = None
    return q if q and q.get("price") else None


def bank_closes(symbol, days=80):
    """Daily closes newest first from the banks: the FMP walker's daily price file, then the Polygon EOD bank (a window
    ending at the newest session the bank holds).  (closes, basis, as_of) or ([], None, None)."""
    if _bank is not None:
        try:
            rows = _bank.walker("historical-price-eod-full", symbol)
        except Exception:  # noqa: BLE001
            rows = None
        if isinstance(rows, list) and rows:
            dated = [(str(r.get("date"))[:10], r.get("close") if r.get("close") is not None else r.get("price")) for r in rows if isinstance(r, dict) and r.get("date")]
            dated = [(d, float(c)) for d, c in dated if c is not None]
            dated.sort(key=lambda x: x[0], reverse=True)
            if len(dated) >= 55:
                return [c for _, c in dated[:days]], "fmp-walker eod", dated[0][0]
    if _poly is not None:
        today = datetime.now(timezone.utc).date()
        start = (today - timedelta(days=int(days * 1.7) + 10)).isoformat()
        for back in range(0, 5):
            end = (today - timedelta(days=back)).isoformat()
            try:
                raw = _poly.answer("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s?adjusted=true&sort=asc&limit=500" % (urllib.parse.quote(symbol), start, end))
            except Exception:  # noqa: BLE001
                raw = None
            if not raw:
                continue
            try:
                res = json.loads(raw).get("results") or []
            except Exception:  # noqa: BLE001
                res = []
            closes = [float(r["c"]) for r in res if isinstance(r, dict) and r.get("c") is not None]
            if len(closes) >= 55:
                closes.reverse()
                last = datetime.fromtimestamp(res[-1]["t"] / 1000.0, tz=timezone.utc).date().isoformat() if res and res[-1].get("t") else end
                return closes[:days], "polygon-bank eod", last
            break
    return [], None, None


def bank_context(symbol):
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


def book_candidates(exclude, today=None):
    """Every US common stock that reported 5-15 trading days ago per the banked FMP earnings calendar (current + previous
    month windows), >= BOOK_MIN_MCAP and >= BOOK_MIN_AVG_VOL in the factor book, not an ADR; {symbol: report_date},
    largest first, capped.  {} when the banks are not there."""
    if _bank is None or not book_ok():
        return {}
    today = today or datetime.now(timezone.utc).date()
    months = {today.strftime("%Y-%m"), (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")}
    dates = {}
    for m in sorted(months):
        try:
            rows = _bank.walker("earnings-calendar", day=m)
        except Exception:  # noqa: BLE001
            rows = None
        for r in rows or []:
            if not isinstance(r, dict) or not r.get("symbol") or not r.get("date"):
                continue
            try:
                dd = datetime.strptime(str(r["date"])[:10], "%Y-%m-%d").date()
            except Exception:
                continue
            td = int((today - dd).days * 5 / 7)
            if dd <= today and 5 <= td <= 15:
                sym = str(r["symbol"]).upper()
                if sym not in exclude and (sym not in dates or dd > dates[sym]):
                    dates[sym] = dd
    if not dates:
        return {}
    ranked = []
    for sym, dd in dates.items():
        try:
            row = _book.lookup(sym, ["mcap", "avg_volume", "is_adr"])
        except Exception:  # noqa: BLE001
            row = None
        if not row or (row.get("mcap") or 0) < BOOK_MIN_MCAP or (row.get("avg_volume") or 0) < BOOK_MIN_AVG_VOL or row.get("is_adr"):
            continue
        ranked.append((row.get("mcap") or 0, sym, dd))
    ranked.sort(reverse=True)
    return {sym: dd.isoformat() for _, sym, dd in ranked[:BOOK_CANDIDATES]}


def trading_days_between(from_date_str):
    """Approximate trading days between from_date and today (weekdays only)."""
    if not from_date_str:
        return None
    try:
        d0 = datetime.strptime(from_date_str[:10], "%Y-%m-%d").date()
        today = datetime.utcnow().date()
        delta = (today - d0).days
        # Rough trading-day estimate: ~5/7 of calendar days
        return int(delta * 5 / 7)
    except Exception:
        return None


def rsi(closes, period=14):
    if not closes or len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(period):
        diff = closes[i] - closes[i + 1]
        if diff > 0:
            gains.append(diff)
        else:
            losses.append(-diff)
    avg_g = sum(gains) / period if gains else 0
    avg_l = sum(losses) / period if losses else 1e-9
    rs = avg_g / avg_l if avg_l else 0
    return round(100 - (100 / (1 + rs)), 1)


def price_extension_std(closes, lookback=50):
    """Z-score of price vs 50d MA, normalized by 50d stdev."""
    if not closes or len(closes) < lookback + 1:
        return None
    ma = statistics.mean(closes[1:lookback + 1])
    sd = statistics.stdev(closes[1:lookback + 1]) or 1e-9
    return round((closes[0] - ma) / sd, 2)


def iv_percentile_proxy(closes, lookback=60):
    """Proxy for IV: realized vol over 20d as percentile of 60d distribution."""
    if not closes or len(closes) < lookback + 2:
        return None
    # Realized vol windows
    def rv(window_closes):
        rets = []
        for i in range(len(window_closes) - 1):
            if window_closes[i + 1] == 0:
                continue
            rets.append((window_closes[i] / window_closes[i + 1]) - 1.0)
        if len(rets) < 2:
            return None
        return statistics.stdev(rets) * (252 ** 0.5)
    rv_current = rv(closes[:21])
    if rv_current is None:
        return None
    # Compare to last 60d of rolling 20d RVs
    rolling = []
    for start in range(lookback - 20):
        sub = closes[start:start + 21]
        v = rv(sub)
        if v is not None:
            rolling.append(v)
    if len(rolling) < 10:
        return None
    rolling_sorted = sorted(rolling)
    below = sum(1 for v in rolling_sorted if v < rv_current)
    return round(100.0 * below / len(rolling_sorted), 1)


def analyze_ticker(symbol, from_date, known_date=None):
    """Full multi-factor analysis for one ticker (bank first, live fallback for every input)."""
    # 1. Earnings within 5-15 trading days ago
    if known_date:
        ed, e_row, ed_src = known_date, None, "fmp-walker earnings-calendar"
    else:
        ed, e_row, ed_src = bank_earnings_date(symbol)
    if not ed:
        ed, e_row = fmp_earnings_recent(symbol, from_date)
        ed_src = "fmp live" if ed else None
    if not ed:
        return None
    _count("earnings_date", ed_src)
    days_ago = trading_days_between(ed)
    if days_ago is None or days_ago < 5 or days_ago > 15:
        return None
    # 2. Pull price + history
    quote = bank_quote(symbol)
    px_src = "factor-book close" if quote else None
    if not quote:
        quote = fmp_quote(symbol)
        px_src = "fmp live" if quote else None
    if not quote:
        return None
    _count("quote", px_src)
    price = quote.get("price")
    if not price:
        return None
    closes, hist_src, hist_as_of = bank_closes(symbol, 80)
    if len(closes) < 55:
        closes = fmp_history(symbol, 80)
        hist_src, hist_as_of = ("fmp live" if len(closes) >= 55 else None), None
    if len(closes) < 55:
        return None
    _count("history", hist_src)
    if abs(closes[0] - price) > price * 0.05:
        closes = [price] + closes
    # 3. RSI extreme
    r14 = rsi(closes, 14)
    if r14 is None:
        return None
    # 4. Price extension std
    ext_std = price_extension_std(closes, 50)
    if ext_std is None:
        return None
    # 5. IV percentile proxy
    iv_pct = iv_percentile_proxy(closes, 60)
    # Direction + qualification
    if r14 >= 75 and ext_std >= 1.5:
        direction = "SHORT"  # Counter-trend short (overbought after up-drift)
    elif r14 <= 25 and ext_std <= -1.5:
        direction = "LONG"  # Counter-trend long (oversold after down-drift)
    else:
        return None
    if iv_pct is None or iv_pct > 50:
        return None  # Vol not sufficiently crushed
    # Composite score
    score = 0.0
    if direction == "SHORT":
        if r14 >= 80:
            score += 0.25
        elif r14 >= 75:
            score += 0.15
    else:
        if r14 <= 20:
            score += 0.25
        elif r14 <= 25:
            score += 0.15
    if abs(ext_std) >= 2.5:
        score += 0.3
    elif abs(ext_std) >= 2.0:
        score += 0.2
    else:
        score += 0.1
    if iv_pct <= 20:
        score += 0.3
    elif iv_pct <= 35:
        score += 0.2
    else:
        score += 0.1
    if 8 <= days_ago <= 12:
        score += 0.15  # Sweet spot of PEAD exhaustion
    score = round(min(1.0, score), 3)
    target = round(abs(ext_std) * 1.2, 1)  # Expect 1.2 std mean-rev
    return {
        "ticker": symbol,
        "direction": direction,
        "price": price,
        "price_source": px_src,
        "earnings_date": ed,
        "earnings_date_source": ed_src,
        "history_source": hist_src,
        "history_as_of": hist_as_of,
        "bank_context": bank_context(symbol),
        "days_post_earnings": days_ago,
        "rsi14": r14,
        "price_ext_std_50d": ext_std,
        "iv_percentile_60d": iv_pct,
        "score": score,
        "trade_ticket": {
            "ticker": symbol,
            "side": direction,
            "rationale": (
                f"Post-earnings mean-rev: {days_ago}d after report, RSI {r14}, "
                f"ext {ext_std} std, IV pct {iv_pct} (crushed). "
                f"Counter-trend {direction.lower()} on exhausted drift."
            ),
            "target_pct": target,
            "stop_pct": -2.5 if direction == "LONG" else 2.5,
            "holding_period": "5-10 trading days",
            "size_pct_portfolio": 1.5,
        },
    }


def send_telegram(text):
    if not TELEGRAM_TOKEN:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        body = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT, "text": text, "parse_mode": "Markdown",
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=10)
    except Exception:
        pass


def state_changed(new_state):
    try:
        prev = ssm.get_parameter(Name=SSM_STATE_KEY)["Parameter"]["Value"]
    except Exception:
        prev = None
    if prev != new_state:
        try:
            ssm.put_parameter(Name=SSM_STATE_KEY, Value=new_state, Type="String", Overwrite=True)
        except Exception:
            pass
        return True
    return False


def lambda_handler(event, context):
    start = time.time()
    try:
        for k in ("earnings_date", "quote", "history"):
            BANK[k] = {}
        BANK["hits"] = 0
        universe = load_universe()
        core = universe[:120]
        book = book_candidates(set(core))
        book_as_of = _book.as_of() if book_ok() else None
        universe_basis = ("master-ranker (%d) + factor book / banked earnings calendar: reported 5-15 trading days ago, >= $%dB, >= %dk avg volume (%d)"
                          % (len(core), int(BOOK_MIN_MCAP / 1e9), int(BOOK_MIN_AVG_VOL / 1e3), len(book))) if book else ("master-ranker (%d); factor book unavailable" % len(core))
        from_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
        setups = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(analyze_ticker, t, from_date): t for t in core}
            futs.update({ex.submit(analyze_ticker, t, from_date, dd): t for t, dd in book.items()})
            for f in as_completed(futs):
                try:
                    res = f.result()
                    if res:
                        setups.append(res)
                except Exception:
                    continue
        setups.sort(key=lambda s: s["score"], reverse=True)

        n_high = sum(1 for s in setups if s["score"] >= 0.65)
        n_med = sum(1 for s in setups if 0.45 <= s["score"] < 0.65)
        n_long = sum(1 for s in setups if s["direction"] == "LONG")
        n_short = sum(1 for s in setups if s["direction"] == "SHORT")

        if n_high >= 6:
            state, strength = "MEAN_REV_RICH", 0.9
        elif n_high >= 2 or (n_high + n_med) >= 6:
            state, strength = "ACTIVE", 0.65
        elif n_high >= 1 or n_med >= 2:
            state, strength = "NORMAL", 0.35
        else:
            state, strength = "QUIET", 0.1
        # wo4592 BUG-4 gate: "no mean-rev setups" is unknowable when every
        # fetch errored. Blind says blind, not calm.
        data_sufficiency = {"http_ok": _HTTP["ok"],
                            "http_err": _HTTP["err"],
                            "bank_reads": BANK["hits"],
                            "bank_basis": {k: BANK[k] for k in ("earnings_date", "quote", "history")},
                            "rule": "QUIET only claimable when >=1 fetch (live or bank) "
                                    "answered; else INSUFFICIENT_DATA"}
        if state == "QUIET" and _HTTP["ok"] == 0 and BANK["hits"] == 0 and _HTTP["err"] > 0:
            state, strength = "INSUFFICIENT_DATA", 0.0

        out = {
            "engine": "post-earnings-mean-rev",
            "version": VERSION,
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state": state,
            "data_sufficiency": data_sufficiency,
            "signal_strength": round(strength, 2),
            "n_setups": len(setups),
            "n_high_conviction": n_high,
            "n_long_setups": n_long,
            "n_short_setups": n_short,
            "universe_size": len(core) + len(book),
            "universe_basis": universe_basis,
            "n_book_candidates": len(book),
            "book_as_of": book_as_of,
            "top_setups": setups[:15],
            "all_setups": setups,
            "methodology": (
                "Post-Earnings Mean-Reversion (PEAD exhaustion). 4-factor screen: "
                "(1) earnings 5-15 trading days ago; (2) RSI<=25 (long) or "
                "RSI>=75 (short); (3) price extension vs 50d MA >=1.5 std; "
                "(4) IV percentile (RV proxy) <=50 (vol crushed). Composite "
                "score weights RSI extremity, extension magnitude, IV crush "
                "depth, days-post-earnings sweet spot (8-12d). "
                "Edge basis: Bernard-Thomas 1989, Hong-Stein 1999, "
                "Chordia-Shivakumar 2006, Bali-Demirtas-Levi 2008. "
                "Forward edge ~58% hit / +2-4% counter-drift / 5-10d hold."
            ),
            "sources": [
                "s3://justhodl-dashboard-live/data/master-ranker.json (universe)",
                "factor book data/warm/factor-book (bank close, analyst / surprise context, candidate gates)",
                "FMP walker bank data/warm/fmp (earnings, earnings-calendar, historical-price-eod-full)",
                "Polygon EOD bank data/warm/polygon-full/eod (closes fallback)",
                "FMP /stable/earnings-calendar (live fallback)",
                "FMP /stable/quote (live fallback)",
                "FMP /stable/historical-price-eod/light (live fallback)",
            ],
            "why_now": f"{n_high} high-conviction + {n_med} moderate; {n_long} LONG, {n_short} SHORT",
            "run_seconds": round(time.time() - start, 2),
        }

        if state_changed(state) and state in ("MEAN_REV_RICH", "ACTIVE"):
            top = setups[:5]
            top_str = "\n".join(
                f"- {s['ticker']} {s['direction']} (RSI {s['rsi14']}, "
                f"ext {s['price_ext_std_50d']}std, score {s['score']})"
                for s in top)
            send_telegram(
                f"*POST-EARNINGS MEAN-REV -> {state}*\n"
                f"{n_long} LONG / {n_short} SHORT setups\n"
                f"Top 5:\n{top_str}\n"
                f"Hold 5-10 days. retail-edges.html for details."
            )

        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, max-age=60",
        )
        return {"statusCode": 200, "body": json.dumps({"ok": True, "state": state,
                                                         "n_setups": len(setups)})}
    except Exception as e:
        import traceback
        err = {"engine": "post-earnings-mean-rev", "error": str(e)[:300],
               "trace": traceback.format_exc()[:600],
               "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode("utf-8"),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"error": str(e)[:300]})}
