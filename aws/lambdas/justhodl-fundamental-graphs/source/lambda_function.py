"""justhodl-fundamental-graphs v1.5.0 (ops 3462/3464/3465)
MARKER: FUNDGRAPH_V1_OPS3462

TradingView-class "Fundamental Graphs" API for fundamental-graphs.html.
One call per symbol+period returns 200+ aligned series over 10+ years:

  - Raw statement lines (income / balance / cash-flow) from FMP /stable.
  - Every ratio/margin/valuation COMPUTED IN-HOUSE, TTM-proper, with
    mcap_t = close_t x diluted shares_t. No vendor-ratio roulette.
  - Institutional/HF set: ROIC, GP/Assets (Novy-Marx), Rule of 40,
    Greenblatt earnings-yield + ROC, EV/GP, FCF conversion, net-buyback
    yield (net of issuance, SBC_WASH doctrine), total shareholder yield,
    capex/D&A, full credit block (debt/EBITDA, EBITDA/interest, FCF/debt).
  - Growth family: YoY for 12 lines + 3y/5y CAGRs (TTM-based).
  - Distress/quality scores per period: Altman Z + Z'', Piotroski F,
    Beneish M, Sloan, Springate, Zmijewski, Fulmer H, KZ index, Tobin's Q.
  - Analyst estimates (history + future) and per-employee series.
  - Weekly closes for the "Show price charts" overlay.

Real data only. S3-cached 20h (data/fundgraph/cache/, _v11 keys). Public
Function URL; CORS emitted ONLY by the URL config (single authority —
dual emission produced duplicate ACAO and browser "Failed to fetch",
closed in ops 3464). gzip via Accept-Encoding or ?gz=1.

Invoke shapes:
  Function URL GET  ?symbol=AAPL&period=quarter|annual[&refresh=1][&gz=1]
  Direct/Event      {"symbol":"AAPL","period":"quarter"}
  Warm (Event)      {"warm":["AAPL","CHTR"],"periods":["quarter","annual"]}
"""

import base64
import gzip
import json
import math
import re
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_KEY = os.environ.get("FMP_KEY", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
CACHE_PREFIX = "data/fundgraph/cache/"
CACHE_VER = "v21"  # v21: FULL history (statements to inception, price from 1962, deep NBER)  # v12: + earnings layer (report dates, beat/miss)
CACHE_TTL_SEC = int(os.environ.get("CACHE_TTL_SEC", 20 * 3600))
MAX_Q = 220   # full history — matches FETCH_Q (ops 3518)
MAX_A = 65
FETCH_Q = 200   # ~50y of quarters — FMP serves to inception
FETCH_A = 60    # ~60y annual
UA = {"User-Agent": "Mozilla/5.0 (justhodl-fundamental-graphs/1.1)"}
SLEEP = 0.22

_s3 = boto3.client("s3")


# ── tiny utils ───────────────────────────────────────────────────────────────
def num(x):
    try:
        if x is None or isinstance(x, bool):
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def g(row, *names):
    for n in names:
        v = num(row.get(n))
        if v is not None:
            return v
    return None


def _http(url, timeout=25):
    last = None
    for att in range(3):
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers=UA), timeout=timeout
            ) as r:
                return json.loads(r.read().decode("utf-8", "replace"))
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(0.6 * (att + 1))
    raise RuntimeError(f"fetch failed: {str(last)[:160]}")


def _fmp(path_qs):
    sep = "&" if "?" in path_qs else "?"
    data = _http(f"{FMP_BASE}/{path_qs}{sep}apikey={FMP_KEY}")
    time.sleep(SLEEP)
    return data


def rnd(v, p=6):
    if v is None:
        return None
    try:
        return round(float(v), p)
    except (TypeError, ValueError):
        return None


def clamp(v, lo, hi):
    if v is None:
        return None
    return max(lo, min(hi, v))


def log10p(v):
    return None if (v is None or v <= 0) else math.log10(v)


# ── FMP fetchers ─────────────────────────────────────────────────────────────
def fetch_statements(sym, period):
    lim = FETCH_Q if period == "quarter" else FETCH_A
    pq = f"symbol={urllib.parse.quote(sym)}&period={period}&limit={lim}"
    inc = _fmp(f"income-statement?{pq}")
    bal = _fmp(f"balance-sheet-statement?{pq}")
    cf = _fmp(f"cash-flow-statement?{pq}")
    for name, x in (("income", inc), ("balance", bal), ("cashflow", cf)):
        if not isinstance(x, list):
            raise RuntimeError(f"{name} statement not a list for {sym}")
    return inc, bal, cf


def fetch_estimates(sym, period):
    pq = f"symbol={urllib.parse.quote(sym)}&period={period}&limit=60"
    try:
        est = _fmp(f"analyst-estimates?{pq}")
        if not (isinstance(est, list) and est):
            est = _fmp(f"analyst-estimates?symbol={urllib.parse.quote(sym)}&limit=40")
    except Exception:  # noqa: BLE001
        est = []
    return est if isinstance(est, list) else []


def fetch_profile(sym):
    try:
        p = _fmp(f"profile?symbol={urllib.parse.quote(sym)}")
        row = p[0] if isinstance(p, list) and p else (p if isinstance(p, dict) else {})
        return {
            "name": row.get("companyName") or row.get("name") or sym,
            "sector": row.get("sector") or "",
            "industry": row.get("industry") or "",
            "currency": row.get("currency") or "USD",
            "mktCap": g(row, "mktCap", "marketCap"),
            "price": g(row, "price"),
            "exchange": row.get("exchangeShortName") or row.get("exchange") or "",
        }
    except Exception:  # noqa: BLE001
        return {"name": sym, "sector": "", "industry": "", "currency": "USD",
                "mktCap": None, "price": None, "exchange": ""}


# ── TA_ENGINE_OPS3500: pure daily-bar technicals ─────────────────────
def _sma(cs, n):
    out, acc = [None] * len(cs), 0.0
    for i, c in enumerate(cs):
        acc += c
        if i >= n:
            acc -= cs[i - n]
        if i >= n - 1:
            out[i] = acc / n
    return out


def _rsi14(cs):
    n = 14
    out = [None] * len(cs)
    if len(cs) <= n:
        return out
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = cs[i] - cs[i - 1]
        gains += max(d, 0)
        losses += max(-d, 0)
    ag, al = gains / n, losses / n
    out[n] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    for i in range(n + 1, len(cs)):
        d = cs[i] - cs[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n
        al = (al * (n - 1) + max(-d, 0)) / n
        out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    return out


def _bb20(cs):
    n = 20
    up, dn, mid = [None] * len(cs), [None] * len(cs), _sma(cs, n)
    for i in range(n - 1, len(cs)):
        w = cs[i - n + 1:i + 1]
        mu = sum(w) / n
        sd = (sum((x - mu) ** 2 for x in w) / n) ** 0.5
        up[i], dn[i] = mu + 2 * sd, mu - 2 * sd
    return up, dn, mid


def _pivots(cs, k=10):
    hi, lo = [], []
    for i in range(k, len(cs) - k):
        w = cs[i - k:i + k + 1]
        if cs[i] == max(w) and w.count(cs[i]) == 1:
            hi.append(i)
        if cs[i] == min(w) and w.count(cs[i]) == 1:
            lo.append(i)
    return hi, lo


def _doubles(ds, cs, look=600):
    """Last double top / double bottom in the window (classic 3% peaks,
    20-250 bar gap, >=5% valley/peak between; confirmed on break)."""
    st = max(0, len(cs) - look)
    hi, lo = _pivots(cs[st:], 10)
    hi = [i + st for i in hi]
    lo = [i + st for i in lo]
    out = []

    def scan(piv, top):
        best = None
        for a in range(len(piv) - 1):
            for b in range(a + 1, len(piv)):
                i, j = piv[a], piv[b]
                if not 20 <= j - i <= 250:
                    continue
                h1, h2 = cs[i], cs[j]
                if abs(h2 - h1) / h1 > 0.03:
                    continue
                between = cs[i + 1:j]
                if not between:
                    continue
                if top:
                    v = min(between)
                    if v > 0.95 * min(h1, h2):
                        continue
                    conf = any(c < v for c in cs[j + 1:])
                else:
                    v = max(between)
                    if v < 1.05 * max(h1, h2):
                        continue
                    conf = any(c > v for c in cs[j + 1:])
                best = {"type": "DBL_TOP" if top else "DBL_BOTTOM",
                        "d": ds[j], "p1": ds[i],
                        "level": rnd((h1 + h2) / 2, 2),
                        "neck": rnd(v, 2),
                        "confirmed": bool(conf)}
        return best

    t = scan(hi, True)
    b2 = scan(lo, False)
    if t:
        out.append(t)
    if b2:
        out.append(b2)
    return out


def compute_ta(daily):
    """daily = sorted [(iso, close)]; returns None if too short."""
    if not daily or len(daily) < 60:
        return None
    ds = [d for d, _ in daily]
    cs = [float(c) for _, c in daily]
    mas = {n: _sma(cs, n) for n in (20, 50, 100, 200)}
    bb_up, bb_dn, _ = _bb20(cs)
    rsi = _rsi14(cs)
    events = []
    cutoff = ds[-1][:4]
    two_y = max(0, len(ds) - 520)
    for n in (20, 50, 100, 200):
        ma = mas[n]
        prev = None
        for i in range(len(cs)):
            if ma[i] is None:
                continue
            sgn = 1 if cs[i] > ma[i] else (-1 if cs[i] < ma[i] else 0)
            if prev is not None and sgn != 0 and sgn != prev:
                if i >= two_y:
                    events.append([ds[i],
                                   "X_UP_%d" % n if sgn > 0 else "X_DN_%d" % n,
                                   "price crossed %s the %d-DMA"
                                   % ("above" if sgn > 0 else "below", n)])
            if sgn != 0:
                prev = sgn
    prev = None
    for i in range(len(cs)):
        a, b3 = mas[50][i], mas[200][i]
        if a is None or b3 is None:
            continue
        sgn = 1 if a > b3 else (-1 if a < b3 else 0)
        if prev is not None and sgn != 0 and sgn != prev:
            events.append([ds[i],
                           "GC_50_200" if sgn > 0 else "DC_50_200",
                           "golden cross 50/200" if sgn > 0
                           else "death cross 50/200"])
        if sgn != 0:
            prev = sgn
    pats = _doubles(ds, cs)
    for p2 in pats:
        events.append([p2["d"], p2["type"],
                       "%s %s (neck %.2f)"
                       % (p2["type"].replace("_", " ").lower(),
                          "confirmed" if p2["confirmed"] else "forming",
                          p2["neck"])])
    events.sort()
    last = cs[-1]
    m200 = mas[200][-1]
    status = {"last_close": rnd(last, 4), "last_date": ds[-1],
              "ma20": rnd(mas[20][-1], 4), "ma50": rnd(mas[50][-1], 4),
              "ma100": rnd(mas[100][-1], 4), "ma200": rnd(m200, 4),
              "above_200": bool(m200 and last > m200),
              "pct_vs_200": rnd((last / m200 - 1) * 100, 2) if m200 else None,
              "above_50": bool(mas[50][-1] and last > mas[50][-1]),
              "bull_stack": bool(mas[20][-1] and mas[50][-1]
                                 and mas[100][-1] and m200
                                 and mas[20][-1] > mas[50][-1]
                                 > mas[100][-1] > m200),
              "rsi14": rnd(rsi[-1], 1) if rsi[-1] is not None else None,
              "bb_pos": rnd((last - (bb_up[-1] + bb_dn[-1]) / 2)
                            / ((bb_up[-1] - bb_dn[-1]) / 2), 2)
              if bb_up[-1] is not None and bb_up[-1] != bb_dn[-1] else None,
              "last_cross": ([{"d": e[0], "type": e[1]}
                              for e in events
                              if e[1].startswith(("X_", "GC", "DC"))]
                             or [None])[-1],
              "patterns": pats}

    def wk_sample(arr, wdates, idx):
        return [[wd, rnd(arr[idx[wd]], 4)] for wd in wdates
                if wd in idx and arr[idx[wd]] is not None]

    return {"mas": mas, "bb_up": bb_up, "bb_dn": bb_dn, "rsi": rsi,
            "ds": ds, "events": events[-40:], "status": status,
            "wk_sample": wk_sample, "cutoff_year": cutoff}


PRICE_WINDOWS = [("1962-01-01", "1980-12-31"),
                 ("1981-01-01", "1995-12-31"),
                 ("1996-01-01", "2008-12-31"),
                 ("2009-01-01", None)]


def fetch_price(sym):
    """FULL price history via windowed stitch — the /light endpoint caps
    ~5k rows per request (single from=1962 stops ~2006; ops 3516 probe).
    Four windows each under the cap, concat + dedupe by date."""
    hist = []
    for w_from, w_to in PRICE_WINDOWS:
        qs = (f"historical-price-eod/light?symbol="
              f"{urllib.parse.quote(sym)}&from={w_from}")
        if w_to:
            qs += f"&to={w_to}"
        try:
            data = _fmp(qs)
        except Exception:  # noqa: BLE001
            data = []
        part = (data.get("historical") or data.get("data") or []
                if isinstance(data, dict) else data)
        if isinstance(part, list):
            hist += part
    seen = set()
    hist = [r for r in hist
            if isinstance(r, dict) and r.get("date")
            and not (str(r["date"])[:10] in seen
                     or seen.add(str(r["date"])[:10]))]
    rows, vrows = [], []
    for r in hist if isinstance(hist, list) else []:
        d = r.get("date")
        c = g(r, "close", "price", "adjClose")
        if d and c is not None and c > 0:
            rows.append((str(d)[:10], c))
            v = r.get("volume")
            vrows.append((str(d)[:10],
                          float(v) if isinstance(v, (int, float)) else 0.0))
    rows.sort(key=lambda t: t[0])
    vrows.sort(key=lambda t: t[0])
    weekly, last_wk = [], None
    for d, c in rows:
        try:
            wk = datetime.strptime(d, "%Y-%m-%d").isocalendar()[:2]
        except ValueError:
            continue
        if wk != last_wk:
            weekly.append([d, rnd(c, 4)])
            last_wk = wk
        else:
            weekly[-1] = [d, rnd(c, 4)]
    return rows, weekly, vrows


def volume_layer(vrows, weekly_px):
    """Continuity-gated volume: weekly sums, 20d avg, RVOL, spikes.
    Returns (P_add dict, events list, status dict) or (None, [], status)
    with named insufficiency."""
    if not vrows or len(vrows) < 250:
        return None, [], {"state": "insufficient",
                          "why": "fewer than 250 daily volume rows"}
    tail = vrows[-756:]
    nz = sum(1 for _, v in tail if v > 0)
    cov = 100.0 * nz / len(tail)
    if cov < 95.0:
        return None, [], {"state": "insufficient",
                          "why": "coverage %.1f%% of last %d days (<95%%)"
                          % (cov, len(tail))}
    ds = [d for d, _ in vrows]
    vs = [v for _, v in vrows]
    va20 = _sma(vs, 20)
    wk_bucket, wk_of = {}, {}
    for d, v in vrows:
        try:
            wk = datetime.strptime(d, "%Y-%m-%d").isocalendar()[:2]
        except ValueError:
            continue
        wk_bucket[wk] = wk_bucket.get(wk, 0.0) + v
    for d, _ in weekly_px:
        try:
            wk_of[d] = datetime.strptime(d, "%Y-%m-%d").isocalendar()[:2]
        except ValueError:
            pass
    vol_w = [[d, rnd(wk_bucket.get(wk_of.get(d), 0.0), 0)]
             for d, _ in weekly_px if wk_of.get(d) in wk_bucket]
    idx = {d: i for i, d in enumerate(ds)}
    ma20_w = [[d, rnd(va20[idx[d]], 0)] for d, _ in weekly_px
              if d in idx and va20[idx[d]] is not None]
    events = []
    start = max(20, len(vs) - 520)
    for i in range(start, len(vs)):
        base = va20[i - 1] if i >= 1 else None
        if base and base > 0 and vs[i] >= 2.5 * base:
            events.append([ds[i], "VOL_SPIKE",
                           "volume spike %.1fx 20d avg" % (vs[i] / base)])
    events = events[-15:]
    avg_prior = (sum(vs[-21:-1]) / 20.0) if len(vs) >= 21 else None
    status = {"state": "ok", "coverage_pct": rnd(cov, 1),
              "last": rnd(vs[-1], 0),
              "avg20": rnd(avg_prior, 0) if avg_prior else None,
              "rvol": rnd(vs[-1] / avg_prior, 2)
              if avg_prior and avg_prior > 0 else None}
    return {"volume_w": vol_w, "vol_ma20": ma20_w}, events, status


def fetch_earnings(sym):
    """Report dates + est-vs-actual EPS (beat/miss layer). /stable 'earnings'."""
    try:
        rows = _fmp(f"earnings?symbol={urllib.parse.quote(sym)}&limit=70")
    except Exception:  # noqa: BLE001
        rows = []
    out = []
    for r in rows if isinstance(rows, list) else []:
        d = str(r.get("date") or "")[:10]
        act = g(r, "epsActual", "actualEPS", "eps")
        est = g(r, "epsEstimated", "estimatedEPS")
        if len(d) == 10 and (act is not None or est is not None):
            out.append([d, rnd(act, 4), rnd(est, 4)])
    out.sort(key=lambda t: t[0])
    return out[-48:]


def fetch_employees(sym):
    """Historical head-count (annual-ish). Graceful when absent."""
    try:
        rows = _fmp(f"employee-count?symbol={urllib.parse.quote(sym)}&limit=200")
    except Exception:  # noqa: BLE001
        rows = []
    out = []
    for r in rows if isinstance(rows, list) else []:
        d = str(r.get("periodOfReport") or r.get("date") or r.get("filingDate") or "")[:10]
        c = g(r, "employeeCount", "numberOfEmployees", "employeesCount")
        if len(d) == 10 and c and c > 0:
            out.append((d, c))
    out.sort(key=lambda t: t[0])
    return out


SYMDIR_KEY = "data/fundgraph/symdir2.json"  # v2: rows carry marketCap
SYMDIR_TTL = 8 * 86400
_SYMDIR = {"rows": None, "ts": 0, "diag": []}
US_EXCH = {"NASDAQ", "NYSE", "AMEX", "NYSE ARCA", "BATS", "CBOE", "NYSE MKT"}


EXCH_TOK = ("NASDAQ", "NYSE", "AMEX", "BATS", "CBOE")


def build_symdir():
    """Multi-endpoint, tolerant US-universe builder with diagnostics."""
    diag = []
    acc = {}
    for ep in ("company-screener?marketCapMoreThan=2000000000&limit=10000",
               "company-screener?marketCapMoreThan=5000000&marketCapLowerThan=2000000000&limit=10000",
               "stock-list"):
        try:
            data = _fmp(ep)
            n_raw = len(data) if isinstance(data, list) else -1
        except Exception as e:  # noqa: BLE001
            diag.append([ep, "ERR " + str(e)[:60]])
            continue
        if not (isinstance(data, list) and n_raw > 800):
            diag.append([ep, f"raw={n_raw}"])
            continue
        rows = []
        for r in data:
            sy = str(r.get("symbol") or "").upper()
            nm = r.get("name") or r.get("companyName") or ""
            exU = str(r.get("exchangeShortName") or r.get("exchange")
                      or "").upper()
            ty = str(r.get("type") or r.get("assetType") or "").lower()
            tok = next((t for t in EXCH_TOK if t in exU), None)
            if (sy and nm and tok
                    and "USD" not in sy and len(sy) <= 6
                    and "crypto" not in ty and "forex" not in ty
                    and "index" not in ty
                    and r.get("isActivelyTrading") in (None, True, "true")
                    and str(r.get("isFund")).lower() != "true"):
                rows.append([sy, str(nm)[:60], tok,
                             num(r.get("marketCap")) or 0])
        if len(rows) == 0 and n_raw > 0:
            try:
                diag.append([ep, "raw=%d kept=0 keys=%s"
                             % (n_raw, sorted(list(data[0].keys()))[:10])])
            except Exception:  # noqa: BLE001
                diag.append([ep, f"raw={n_raw} kept=0"])
        else:
            diag.append([ep, f"raw={n_raw} kept={len(rows)}"])
        for row in rows:
            acc.setdefault(row[0], row)
        if len(acc) >= 12000:
            break
    _SYMDIR["diag"] = diag
    return list(acc.values())


def load_symdir(force=False):
    now = time.time()
    if not force and _SYMDIR["rows"] and now - _SYMDIR["ts"] < 3600:
        return _SYMDIR["rows"]
    rows = None
    if not force:
        try:
            obj = _s3.get_object(Bucket=S3_BUCKET, Key=SYMDIR_KEY)
            doc = json.loads(obj["Body"].read())
            if now - doc.get("ts", 0) < SYMDIR_TTL and len(doc.get("rows", [])) > 3000:
                rows = doc["rows"]
        except Exception:  # noqa: BLE001
            rows = None
    if rows is None:
        rows = build_symdir()
        if len(rows) > 3000:
            try:
                _s3.put_object(Bucket=S3_BUCKET, Key=SYMDIR_KEY,
                               Body=json.dumps({"ts": now, "n": len(rows),
                                                "rows": rows},
                                               separators=(",", ":")).encode(),
                               ContentType="application/json")
            except Exception:  # noqa: BLE001
                pass
    _SYMDIR.update(rows=rows or [], ts=now)
    return _SYMDIR["rows"]


def symdir_search(q, cap=8):
    rows = load_symdir()
    if not rows:
        return None
    qU, qL = q.upper(), q.lower()
    scored = []
    for row in rows:
        sy, nm, ex = row[0], row[1], row[2]
        mc = row[3] if len(row) > 3 else 0
        nl = nm.lower()
        if sy == qU:
            tier = 0
        elif sy.startswith(qU):
            tier = 1
        elif nl.startswith(qL):
            tier = 2
        elif (" " + qL) in (" " + nl):
            tier = 3
        elif qL in nl:
            tier = 4
        else:
            continue
        # within a tier: biggest company first (Microsoft > MicroBot)
        scored.append((tier, -(mc or 0),
                       0 if ex in ("NASDAQ", "NYSE") else 1, len(sy),
                       sy, nm, ex))
    scored.sort()
    return [{"symbol": t[4], "name": t[5], "exchange": t[6]}
            for t in scored[:cap]]


def at_or_before(series, date, max_gap_days=460):
    """Nearest (d, v) on/before date within gap (binary search)."""
    if not series or date < series[0][0]:
        return None
    lo, hi = 0, len(series) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if series[mid][0] <= date:
            lo = mid
        else:
            hi = mid - 1
    d, v = series[lo]
    try:
        gap = (datetime.strptime(date, "%Y-%m-%d")
               - datetime.strptime(d, "%Y-%m-%d")).days
    except ValueError:
        return None
    return v if gap <= max_gap_days else None


def price_at(daily, date):
    return at_or_before(daily, date, max_gap_days=30)


# ── series assembly ──────────────────────────────────────────────────────────
def by_date(rows):
    out = {}
    for r in rows:
        d = str(r.get("date") or "")[:10]
        if len(d) == 10:
            out[d] = r
    return out


def nearest(dmap, date, tol_days=48):
    if date in dmap:
        return dmap[date]
    try:
        t0 = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return {}
    best, bestdiff = {}, tol_days + 1
    for d, r in dmap.items():
        try:
            diff = abs((datetime.strptime(d, "%Y-%m-%d") - t0).days)
        except ValueError:
            continue
        if diff < bestdiff:
            best, bestdiff = r, diff
    return best if bestdiff <= tol_days else {}


def build_doc(sym, period):
    inc, bal, cf = fetch_statements(sym, period)
    if not inc:
        raise RuntimeError(f"no income data for {sym}")
    est_rows = fetch_estimates(sym, period)
    profile = fetch_profile(sym)
    daily_px, weekly_px, vol_px = fetch_price(sym)
    tech_doc = None
    try:
        _fdna = factor_dna(sym)
    except Exception as _fe:  # noqa: BLE001
        _fdna = {"state": "insufficient", "why": str(_fe)[:80]}
    emp_series = fetch_employees(sym)
    earnings = fetch_earnings(sym)
    whales = whale_lookup(sym)
    events = fleet_events(sym)

    bmap, cmap = by_date(bal), by_date(cf)
    inc_sorted = sorted(
        [r for r in inc if str(r.get("date") or "")[:10]],
        key=lambda r: str(r["date"])[:10],
    )
    frames = []
    for r in inc_sorted:
        d = str(r["date"])[:10]
        frames.append({"date": d, "inc": r, "bal": nearest(bmap, d), "cf": nearest(cmap, d)})
    n = len(frames)
    lb = 4 if period == "quarter" else 1

    R = []
    for f in frames:
        i, b, c = f["inc"], f["bal"], f["cf"]
        row = {
            "date": f["date"],
            "revenue": g(i, "revenue"),
            "costOfRevenue": g(i, "costOfRevenue"),
            "grossProfit": g(i, "grossProfit"),
            "rnd": g(i, "researchAndDevelopmentExpenses"),
            "sgna": g(i, "sellingGeneralAndAdministrativeExpenses",
                      "generalAndAdministrativeExpenses"),
            "opex": g(i, "operatingExpenses"),
            "operatingIncome": g(i, "operatingIncome"),
            "ebitda": g(i, "ebitda"),
            "da_is": g(i, "depreciationAndAmortization"),
            "interestExpense": g(i, "interestExpense"),
            "interestIncome": g(i, "interestIncome"),
            "pretaxIncome": g(i, "incomeBeforeTax"),
            "taxExpense": g(i, "incomeTaxExpense"),
            "netIncome": g(i, "netIncome"),
            "eps": g(i, "eps"),
            "epsDiluted": g(i, "epsDiluted", "epsdiluted"),
            "shs": g(i, "weightedAverageShsOut"),
            "shsDil": g(i, "weightedAverageShsOutDil"),
            "cash": g(b, "cashAndCashEquivalents"),
            "sti": g(b, "shortTermInvestments"),
            "cashSTI": g(b, "cashAndShortTermInvestments"),
            "receivables": g(b, "netReceivables"),
            "inventory": g(b, "inventory"),
            "totalCurrentAssets": g(b, "totalCurrentAssets"),
            "ppeNet": g(b, "propertyPlantEquipmentNet"),
            "goodwill": g(b, "goodwill"),
            "intangibles": g(b, "intangibleAssets"),
            "gwIntang": g(b, "goodwillAndIntangibleAssets"),
            "ltInvestments": g(b, "longTermInvestments"),
            "totalAssets": g(b, "totalAssets"),
            "accountsPayable": g(b, "accountPayables"),
            "shortTermDebt": g(b, "shortTermDebt"),
            "deferredRevenue": g(b, "deferredRevenue"),
            "totalCurrentLiabilities": g(b, "totalCurrentLiabilities"),
            "longTermDebt": g(b, "longTermDebt"),
            "totalLiabilities": g(b, "totalLiabilities"),
            "retainedEarnings": g(b, "retainedEarnings"),
            "equity": g(b, "totalStockholdersEquity", "totalEquity"),
            "totalDebt": g(b, "totalDebt"),
            "netDebt": g(b, "netDebt"),
            "minorityInterest": g(b, "minorityInterest"),
            "cfo": g(c, "netCashProvidedByOperatingActivities",
                     "operatingCashFlow"),
            "da_cf": g(c, "depreciationAndAmortization"),
            "sbc": g(c, "stockBasedCompensation"),
            "dWorkingCapital": g(c, "changeInWorkingCapital"),
            "cfi": g(c, "netCashProvidedByInvestingActivities",
                     "netCashUsedForInvestingActivites",
                     "netCashUsedForInvestingActivities"),
            "capex": g(c, "capitalExpenditure",
                       "investmentsInPropertyPlantAndEquipment"),
            "acquisitions": g(c, "acquisitionsNet"),
            "purchInvest": g(c, "purchasesOfInvestments"),
            "saleInvest": g(c, "salesMaturitiesOfInvestments"),
            "cff": g(c, "netCashProvidedByFinancingActivities",
                     "netCashUsedProvidedByFinancingActivities"),
            "debtRepayment": g(c, "debtRepayment", "netDebtIssuance"),
            "stockIssued": g(c, "commonStockIssued", "commonStockIssuance"),
            "stockRepurchased": g(c, "commonStockRepurchased",
                                  "commonStockRepurchase"),
            "dividendsPaid": g(c, "dividendsPaid", "netDividendsPaid",
                               "commonDividendsPaid"),
            "fcf": g(c, "freeCashFlow"),
        }
        if row["cashSTI"] is None:
            cs = [x for x in (row["cash"], row["sti"]) if x is not None]
            row["cashSTI"] = sum(cs) if cs else None
        if row["gwIntang"] is None:
            gi = [x for x in (row["goodwill"], row["intangibles"]) if x is not None]
            row["gwIntang"] = sum(gi) if gi else None
        if row["fcf"] is None and row["cfo"] is not None and row["capex"] is not None:
            row["fcf"] = row["cfo"] + row["capex"]
        if row["ebitda"] is None and row["operatingIncome"] is not None:
            row["ebitda"] = row["operatingIncome"] + (row["da_is"] or row["da_cf"] or 0)
        R.append(row)

    def col(key):
        return [r.get(key) for r in R]

    def ttm(vals):
        out = [None] * n
        if lb == 1:
            return list(vals)
        for i in range(n):
            if i >= lb - 1:
                w = vals[i - lb + 1: i + 1]
                if all(v is not None for v in w):
                    out[i] = sum(w)
        return out

    rev_t = ttm(col("revenue"))
    cogs_t = ttm(col("costOfRevenue"))
    gp_t = ttm(col("grossProfit"))
    ebitda_t = ttm(col("ebitda"))
    ebit_t = ttm(col("operatingIncome"))
    ni_t = ttm(col("netIncome"))
    pretax_t = ttm(col("pretaxIncome"))
    tax_t = ttm(col("taxExpense"))
    cfo_t = ttm(col("cfo"))
    cfi_t = ttm(col("cfi"))
    capex_t = ttm(col("capex"))
    fcf_t = ttm(col("fcf"))
    sbc_t = ttm(col("sbc"))
    rnd_t = ttm(col("rnd"))
    sga_t = ttm(col("sgna"))
    da_t = ttm([r.get("da_cf") if r.get("da_cf") is not None else r.get("da_is") for r in R])
    intexp_t = ttm(col("interestExpense"))
    div_t = ttm(col("dividendsPaid"))
    buyb_t = ttm(col("stockRepurchased"))
    iss_t = ttm(col("stockIssued"))
    debtrep_t = ttm(col("debtRepayment"))
    eps_t = ttm(col("epsDiluted")) if lb > 1 else col("epsDiluted")

    sh_arr = [(r.get("shsDil") or r.get("shs")) for r in R]
    mcap, ev = [None] * n, [None] * n
    for i, r in enumerate(R):
        sh = sh_arr[i]
        px = price_at(daily_px, r["date"])
        if sh and px:
            mcap[i] = sh * px
            debt = r.get("totalDebt")
            if debt is None:
                parts = [x for x in (r.get("shortTermDebt"), r.get("longTermDebt"))
                         if x is not None]
                debt = sum(parts) if parts else 0.0
            ev[i] = mcap[i] + (debt or 0) - (r.get("cashSTI") or 0)

    # helper arrays for the Growth family
    dps_arr = [(-div_t[i] / sh_arr[i]) if (div_t[i] is not None and sh_arr[i])
               else None for i in range(n)]
    bvps_arr = [(R[i].get("equity") / sh_arr[i])
                if (R[i].get("equity") is not None and sh_arr[i]) else None
                for i in range(n)]
    capexabs_t = [(-capex_t[i]) if capex_t[i] is not None else None for i in range(n)]
    netbuyb_t = [None if (buyb_t[i] is None and iss_t[i] is None)
                 else -((buyb_t[i] or 0) + (iss_t[i] or 0)) for i in range(n)]

    def yoy_arr(a, allow_neg_base=False):
        out = [None] * n
        for i in range(lb, n):
            p, v = a[i - lb], a[i]
            if v is None or p is None:
                continue
            if p > 0 or (allow_neg_base and p != 0):
                out[i] = (v / p - 1) * 100 if p > 0 else None
        return out

    def cagr_arr(a, yrs):
        per = yrs * lb
        out = [None] * n
        for i in range(per, n):
            p, v = a[i - per], a[i]
            if p and v and p > 0 and v > 0:
                out[i] = ((v / p) ** (1.0 / yrs) - 1) * 100
        return out

    g_rev = yoy_arr(rev_t)
    g_gp = yoy_arr(gp_t)
    g_ebit = yoy_arr(ebit_t)
    g_ebitda = yoy_arr(ebitda_t)
    g_ni = yoy_arr(ni_t)
    g_eps = yoy_arr(eps_t)
    g_cfo = yoy_arr(cfo_t)
    g_fcf = yoy_arr(fcf_t)
    g_dps = yoy_arr(dps_arr)
    g_bvps = yoy_arr(bvps_arr)
    g_capex = yoy_arr(capexabs_t)
    g_sbc = yoy_arr(sbc_t)
    c_rev3, c_rev5 = cagr_arr(rev_t, 3), cagr_arr(rev_t, 5)
    c_eps3, c_eps5 = cagr_arr(eps_t, 3), cagr_arr(eps_t, 5)
    c_fcf3, c_fcf5 = cagr_arr(fcf_t, 3), cagr_arr(fcf_t, 5)

    def div_(a, b, mult=1.0, pos_denom=True):
        if a is None or b in (None, 0):
            return None
        if pos_denom and b <= 0:
            return None
        return a / b * mult

    P = {}

    def put(key, i, val, prec=6):
        v = rnd(val, prec)
        if v is None:
            return
        P.setdefault(key, []).append([R[i]["date"], v])

    RAW_KEYS = [
        "revenue", "costOfRevenue", "grossProfit", "rnd", "sgna", "opex",
        "operatingIncome", "ebitda", "interestExpense", "interestIncome",
        "pretaxIncome", "taxExpense", "netIncome", "eps", "epsDiluted",
        "shs", "shsDil",
        "cash", "sti", "cashSTI", "receivables", "inventory",
        "totalCurrentAssets", "ppeNet", "goodwill", "intangibles", "gwIntang",
        "ltInvestments", "totalAssets", "accountsPayable", "shortTermDebt",
        "deferredRevenue", "totalCurrentLiabilities", "longTermDebt",
        "totalLiabilities", "retainedEarnings", "equity", "totalDebt",
        "netDebt", "minorityInterest",
        "cfo", "sbc", "dWorkingCapital", "cfi", "capex", "acquisitions",
        "purchInvest", "saleInvest", "cff", "debtRepayment", "stockIssued",
        "stockRepurchased", "dividendsPaid", "fcf",
    ]

    keep_from = max(0, n - (MAX_Q if period == "quarter" else MAX_A))
    for i in range(keep_from, n):
        r = R[i]
        for k in RAW_KEYS:
            put(k, i, r.get(k), 2)
        put("da", i, r.get("da_cf") if r.get("da_cf") is not None else r.get("da_is"), 2)

        TA, TL, EQ = r.get("totalAssets"), r.get("totalLiabilities"), r.get("equity")
        CA, CL = r.get("totalCurrentAssets"), r.get("totalCurrentLiabilities")
        sh = sh_arr[i]
        mc, evv = mcap[i], ev[i]
        wc = None if (CA is None or CL is None) else CA - CL
        nd = r.get("netDebt")
        if nd is None and r.get("totalDebt") is not None:
            nd = r["totalDebt"] - (r.get("cashSTI") or 0)
        tang_eq = (None if EQ is None else EQ - (r.get("gwIntang") or 0))
        tang_assets = (None if TA is None else TA - (r.get("gwIntang") or 0))

        put("revenue_ttm", i, rev_t[i], 2)
        put("ebitda_ttm", i, ebitda_t[i], 2)
        put("ebit_ttm", i, ebit_t[i], 2)
        put("net_income_ttm", i, ni_t[i], 2)
        put("cfo_ttm", i, cfo_t[i], 2)
        put("fcf_ttm", i, fcf_t[i], 2)
        put("gross_profit_ttm", i, gp_t[i], 2)
        put("net_buyback_ttm", i, netbuyb_t[i], 2)

        put("mcap", i, mc, 2)
        put("ev", i, evv, 2)
        put("working_capital", i, wc, 2)
        put("tangible_equity", i, tang_eq, 2)
        put("net_debt_calc", i, nd, 2)
        put("ncav", i, None if (CA is None or TL is None) else CA - TL, 2)

        # margins %
        gm_v = div_(gp_t[i], rev_t[i], 100)
        fcfm_v = div_(fcf_t[i], rev_t[i], 100)
        put("gross_margin_pct", i, gm_v, 3)
        put("operating_margin_pct", i, div_(ebit_t[i], rev_t[i], 100), 3)
        put("ebitda_margin_pct", i, div_(ebitda_t[i], rev_t[i], 100), 3)
        put("pretax_margin_pct", i, div_(pretax_t[i], rev_t[i], 100), 3)
        put("net_margin_pct", i, div_(ni_t[i], rev_t[i], 100), 3)
        put("fcf_margin_pct", i, fcfm_v, 3)

        # returns %
        eq_prev = R[i - lb]["equity"] if i - lb >= 0 else None
        eq_avg = (EQ + eq_prev) / 2 if (EQ is not None and eq_prev is not None) else EQ
        ta_prev = R[i - lb]["totalAssets"] if i - lb >= 0 else None
        ta_avg = (TA + ta_prev) / 2 if (TA is not None and ta_prev is not None) else TA
        roe_v = div_(ni_t[i], eq_avg, 100)
        put("roe_pct", i, roe_v, 3)
        put("roa_pct", i, div_(ni_t[i], ta_avg, 100), 3)
        nopat = None
        if ebit_t[i] is not None:
            tr = div_(tax_t[i], pretax_t[i]) if (tax_t[i] is not None and
                                                 pretax_t[i] and pretax_t[i] > 0) else 0.21
            tr = clamp(tr, 0.0, 0.5)
            nopat = ebit_t[i] * (1 - tr)
        icap = None
        if EQ is not None:
            icap = EQ + (r.get("totalDebt") or 0) - (r.get("cashSTI") or 0)
        put("roic_pct", i, div_(nopat, icap, 100), 3)
        put("rota_pct", i, div_(ni_t[i], tang_assets, 100), 3)
        gden = (max(wc, 0) if wc is not None else None)
        if gden is not None and r.get("ppeNet") is not None:
            gden += r["ppeNet"]
        put("roc_greenblatt_pct", i, div_(ebit_t[i], gden, 100), 3)
        put("gp_to_assets_pct", i, div_(gp_t[i], TA, 100), 3)
        put("goodwill_to_assets_pct", i, div_(r.get("goodwill"), TA, 100), 3)
        put("intangibles_to_assets_pct", i, div_(r.get("gwIntang"), TA, 100), 3)

        # valuation
        pe_v = div_(mc, ni_t[i])
        put("pe_ttm", i, pe_v, 3)
        put("ps_ttm", i, div_(mc, rev_t[i]), 3)
        put("pb", i, div_(mc, EQ), 3)
        put("ptb", i, div_(mc, tang_eq), 3)
        put("p_fcf_ttm", i, div_(mc, fcf_t[i]), 3)
        put("p_cfo_ttm", i, div_(mc, cfo_t[i]), 3)
        put("ev_ebitda_ttm", i, div_(evv, ebitda_t[i]), 3)
        put("ev_ebit_ttm", i, div_(evv, ebit_t[i]), 3)
        put("ev_sales_ttm", i, div_(evv, rev_t[i]), 3)
        put("ev_gp_ttm", i, div_(evv, gp_t[i]), 3)
        put("ev_fcf_ttm", i, div_(evv, fcf_t[i]), 3)
        put("earnings_yield_pct", i, div_(ni_t[i], mc, 100), 3)
        put("earnings_yield_ebit_pct", i, div_(ebit_t[i], evv, 100), 3)
        put("fcf_yield_pct", i, div_(fcf_t[i], mc, 100), 3)
        put("fcf_ev_yield_pct", i, div_(fcf_t[i], evv, 100), 3)
        dy = div_(-div_t[i] if div_t[i] is not None else None, mc, 100)
        by = div_(-buyb_t[i] if buyb_t[i] is not None else None, mc, 100)
        nby = div_(netbuyb_t[i], mc, 100)
        dpy = div_(-debtrep_t[i] if debtrep_t[i] is not None else None, mc, 100)
        put("dividend_yield_pct", i, dy, 3)
        put("buyback_yield_pct", i, by, 3)
        put("net_buyback_yield_pct", i, nby, 3)
        put("debt_paydown_yield_pct", i, dpy, 3)
        if dy is not None or by is not None:
            put("shareholder_yield_pct", i, (dy or 0) + (by or 0), 3)
        if dy is not None or nby is not None:
            put("net_shareholder_yield_pct", i, (dy or 0) + (nby or 0), 3)
        if any(v is not None for v in (dy, nby, dpy)):
            put("total_yield_pct", i, (dy or 0) + (nby or 0) + (dpy or 0), 3)
        if g_eps[i] is not None and g_eps[i] > 0 and pe_v is not None and pe_v > 0:
            put("peg_ttm", i, pe_v / g_eps[i], 3)
        put("tobins_q", i,
            None if (mc is None or TA in (None, 0) or TA <= 0)
            else (mc + (r.get("totalDebt") or 0)) / TA, 3)
        gn = None
        if eps_t[i] is not None and EQ is not None and sh and eps_t[i] > 0:
            bvps_ = EQ / sh
            if bvps_ > 0:
                gn = math.sqrt(22.5 * eps_t[i] * bvps_)
        put("graham_number", i, gn, 3)
        ig = implied_fcf_growth(evv, fcf_t[i])
        put("implied_fcf_growth_pct", i, ig, 3)
        if ig is not None and c_fcf3[i] is not None:
            put("implied_vs_actual_gap_pct", i, ig - c_fcf3[i], 3)

        # leverage / liquidity / credit
        put("debt_to_equity", i, div_(r.get("totalDebt"), EQ), 3)
        put("debt_to_assets", i, div_(r.get("totalDebt"), TA), 3)
        put("debt_to_revenue", i, div_(r.get("totalDebt"), rev_t[i]), 3)
        put("debt_to_capital", i,
            div_(r.get("totalDebt"),
                 (r.get("totalDebt") or 0) + EQ if EQ is not None else None), 3)
        put("equity_to_assets", i, div_(EQ, TA), 3)
        put("equity_multiplier", i, div_(TA, EQ), 3)
        put("liab_to_assets", i, div_(TL, TA), 3)
        put("cash_to_debt", i, div_(r.get("cashSTI"), r.get("totalDebt")), 3)
        put("netdebt_to_ebitda_ttm", i,
            None if (nd is None or ebitda_t[i] in (None, 0) or ebitda_t[i] <= 0)
            else nd / ebitda_t[i], 3)
        put("gross_debt_to_ebitda", i, div_(r.get("totalDebt"), ebitda_t[i]), 3)
        put("netdebt_to_fcf", i,
            None if (nd is None or fcf_t[i] in (None, 0) or fcf_t[i] <= 0)
            else nd / fcf_t[i], 3)
        put("interest_coverage_ttm", i, div_(ebit_t[i], intexp_t[i]), 3)
        put("ebitda_interest_coverage", i, div_(ebitda_t[i], intexp_t[i]), 3)
        put("fcf_to_debt_pct", i, div_(fcf_t[i], r.get("totalDebt"), 100), 3)
        put("cfo_to_debt_pct", i, div_(cfo_t[i], r.get("totalDebt"), 100), 3)
        cr_v = div_(CA, CL)
        put("current_ratio", i, cr_v, 3)
        qa = None if CA is None else CA - (r.get("inventory") or 0)
        put("quick_ratio", i, div_(qa, CL), 3)
        put("cash_ratio", i, div_(r.get("cashSTI"), CL), 3)
        put("tangible_ce_ratio", i, div_(tang_eq, tang_assets), 3)

        # efficiency / quality
        put("asset_turnover_ttm", i, div_(rev_t[i], ta_avg), 3)
        put("inventory_turnover_ttm", i, div_(cogs_t[i], r.get("inventory")), 3)
        put("dso_days", i, div_(r.get("receivables"), rev_t[i], 365), 2)
        dio = div_(r.get("inventory"), cogs_t[i], 365)
        dpo = div_(r.get("accountsPayable"), cogs_t[i], 365)
        put("dio_days", i, dio, 2)
        put("dpo_days", i, dpo, 2)
        dso = div_(r.get("receivables"), rev_t[i], 365)
        if dso is not None and dio is not None and dpo is not None:
            put("ccc_days", i, dso + dio - dpo, 2)
        put("income_quality", i,
            None if (ni_t[i] in (None, 0) or ni_t[i] <= 0 or cfo_t[i] is None)
            else cfo_t[i] / ni_t[i], 3)
        put("fcf_conversion_pct", i, div_(fcf_t[i], ebitda_t[i], 100), 3)
        put("cash_conversion_pct", i, div_(cfo_t[i], ebitda_t[i], 100), 3)
        put("fcf_to_ni", i,
            None if (ni_t[i] in (None, 0) or ni_t[i] <= 0) else div_(fcf_t[i], ni_t[i], 1, False), 3)
        put("capex_to_da", i, div_(capexabs_t[i], da_t[i]), 3)
        put("sbc_to_revenue_pct", i, div_(sbc_t[i], rev_t[i], 100), 3)
        put("capex_to_revenue_pct", i, div_(capexabs_t[i], rev_t[i], 100), 3)
        put("rnd_to_revenue_pct", i, div_(rnd_t[i], rev_t[i], 100), 3)
        put("sga_to_revenue_pct", i, div_(sga_t[i], rev_t[i], 100), 3)
        put("cogs_to_revenue_pct", i, div_(cogs_t[i], rev_t[i], 100), 3)
        put("wc_to_revenue_pct", i, div_(wc, rev_t[i], 100), 3)
        put("effective_tax_rate_pct", i,
            None if (tax_t[i] is None or pretax_t[i] in (None, 0) or pretax_t[i] <= 0)
            else clamp(tax_t[i] / pretax_t[i] * 100, -50, 100), 3)
        if ni_t[i] is not None and cfo_t[i] is not None and cfi_t[i] is not None and TA:
            put("sloan_accruals_pct", i, (ni_t[i] - cfo_t[i] - cfi_t[i]) / TA * 100, 3)

        # per-share
        put("eps_ttm", i, div_(ni_t[i], sh), 4)
        put("fcf_ps_ttm", i, div_(fcf_t[i], sh), 4)
        put("cfo_ps_ttm", i, div_(cfo_t[i], sh), 4)
        put("revenue_ps_ttm", i, div_(rev_t[i], sh), 4)
        put("book_value_ps", i, div_(EQ, sh), 4)
        put("tangible_bv_ps", i, div_(tang_eq, sh), 4)
        put("dps_ttm", i, dps_arr[i], 4)
        put("cash_ps", i, div_(r.get("cashSTI"), sh), 4)
        put("ncav_ps", i, div_(None if (CA is None or TL is None) else CA - TL, sh), 4)
        payout_v = (None if (div_t[i] is None or ni_t[i] in (None, 0) or ni_t[i] <= 0)
                    else clamp(-div_t[i] / ni_t[i] * 100, 0, 400))
        put("payout_ratio_pct", i, payout_v, 3)
        retention_v = (100 - payout_v) if payout_v is not None else (
            100.0 if (ni_t[i] is not None and ni_t[i] > 0 and div_t[i] in (None, 0)) else None)
        put("retention_pct", i, retention_v, 3)
        if roe_v is not None and retention_v is not None:
            put("sustainable_growth_pct", i, roe_v * retention_v / 100, 3)
        sh_prev = sh_arr[i - lb] if i - lb >= 0 else None
        if sh and sh_prev:
            put("share_count_yoy_pct", i, (sh / sh_prev - 1) * 100, 3)

        # growth family
        put("revenue_yoy_pct", i, g_rev[i], 3)
        put("gross_profit_yoy_pct", i, g_gp[i], 3)
        put("operating_income_yoy_pct", i, g_ebit[i], 3)
        put("ebitda_yoy_pct", i, g_ebitda[i], 3)
        put("net_income_yoy_pct", i, g_ni[i], 3)
        put("eps_yoy_pct", i, g_eps[i], 3)
        put("cfo_yoy_pct", i, g_cfo[i], 3)
        put("fcf_yoy_pct", i, g_fcf[i], 3)
        put("dps_yoy_pct", i, g_dps[i], 3)
        put("bvps_yoy_pct", i, g_bvps[i], 3)
        put("capex_yoy_pct", i, g_capex[i], 3)
        put("sbc_yoy_pct", i, g_sbc[i], 3)
        put("revenue_cagr_3y_pct", i, c_rev3[i], 3)
        put("revenue_cagr_5y_pct", i, c_rev5[i], 3)
        put("eps_cagr_3y_pct", i, c_eps3[i], 3)
        put("eps_cagr_5y_pct", i, c_eps5[i], 3)
        put("fcf_cagr_3y_pct", i, c_fcf3[i], 3)
        put("fcf_cagr_5y_pct", i, c_fcf5[i], 3)
        if g_rev[i] is not None and fcfm_v is not None:
            put("rule_of_40", i, g_rev[i] + fcfm_v, 3)

        # per-employee
        emp = at_or_before(emp_series, r["date"])
        if emp:
            put("employees", i, emp, 0)
            put("revenue_per_employee", i, div_(rev_t[i], emp), 2)
            put("net_income_per_employee", i, div_(ni_t[i], emp, 1, False), 2)

        # ── distress / quality scores ───────────────────────────────────────
        if TA and TA > 0 and TL and TL > 0:
            z_parts = [
                1.2 * (wc / TA) if wc is not None else None,
                1.4 * (r["retainedEarnings"] / TA) if r.get("retainedEarnings") is not None else None,
                3.3 * (ebit_t[i] / TA) if ebit_t[i] is not None else None,
                0.6 * (mc / TL) if mc is not None else None,
                0.999 * (rev_t[i] / TA) if rev_t[i] is not None else None,
            ]
            if all(p is not None for p in z_parts):
                put("altman_z", i, sum(z_parts), 3)
            zp = [
                6.56 * (wc / TA) if wc is not None else None,
                3.26 * (r["retainedEarnings"] / TA) if r.get("retainedEarnings") is not None else None,
                6.72 * (ebit_t[i] / TA) if ebit_t[i] is not None else None,
                1.05 * (EQ / TL) if EQ is not None else None,
            ]
            if all(p is not None for p in zp):
                put("altman_z_prime", i, sum(zp), 3)
            sp = [
                1.03 * (wc / TA) if wc is not None else None,
                3.07 * (ebit_t[i] / TA) if ebit_t[i] is not None else None,
                0.66 * (pretax_t[i] / CL) if (pretax_t[i] is not None and CL and CL > 0) else None,
                0.4 * (rev_t[i] / TA) if rev_t[i] is not None else None,
            ]
            if all(p is not None for p in sp):
                put("springate", i, sum(sp), 3)
            if ni_t[i] is not None and cr_v is not None:
                put("zmijewski_x", i,
                    -4.336 - 4.513 * (ni_t[i] / TA) + 5.679 * (TL / TA)
                    + 0.004 * cr_v, 3)
            # Fulmer H (approx; TA in $M for the log term)
            fh_ok = all(v is not None for v in (
                r.get("retainedEarnings"), rev_t[i], pretax_t[i], EQ, cfo_t[i],
                CL, wc, tang_assets)) and EQ > 0 and tang_assets and tang_assets > 0
            ratio_ei = (None if (ebit_t[i] is None or intexp_t[i] in (None, 0)
                                 or intexp_t[i] <= 0 or ebit_t[i] <= 0)
                        else ebit_t[i] / intexp_t[i])
            if fh_ok and ratio_ei:
                put("fulmer_h", i,
                    5.528 * (r["retainedEarnings"] / TA)
                    + 0.212 * (rev_t[i] / TA)
                    + 0.073 * (pretax_t[i] / EQ)
                    + 1.270 * (cfo_t[i] / TL)
                    - 0.120 * (TL / TA)
                    + 2.335 * (CL / TA)
                    + 0.575 * log10p(tang_assets / 1e6)
                    + 1.083 * (wc / TL)
                    + 0.894 * log10p(ratio_ei)
                    - 6.075, 3)
            # KZ index (K = prior-year net PP&E)
            j0 = i - lb
            K = R[j0].get("ppeNet") if j0 >= 0 else None
            if (K and K > 0 and mc is not None and EQ is not None
                    and ni_t[i] is not None and da_t[i] is not None):
                dcap = ((r.get("totalDebt") or 0)
                        / ((r.get("totalDebt") or 0) + EQ)) if ((r.get("totalDebt") or 0) + EQ) > 0 else None
                if dcap is not None:
                    put("kz_index", i,
                        -1.002 * ((ni_t[i] + da_t[i]) / K)
                        + 0.283 * ((TA + mc - EQ) / TA)
                        + 3.139 * dcap
                        - 39.368 * ((-div_t[i] if div_t[i] is not None else 0) / K)
                        - 1.315 * ((r.get("cashSTI") or 0) / K), 3)

        # Piotroski F
        j = i - lb
        if j >= 0:
            rp = R[j]
            checks, avail = 0, 0

            def chk(cond):
                nonlocal checks, avail
                if cond is None:
                    return
                avail += 1
                if cond:
                    checks += 1

            roa_now = div_(ni_t[i], TA)
            roa_prev = div_(ni_t[j], rp.get("totalAssets"))
            chk(None if roa_now is None else roa_now > 0)
            chk(None if cfo_t[i] is None else cfo_t[i] > 0)
            chk(None if (roa_now is None or roa_prev is None) else roa_now > roa_prev)
            chk(None if (cfo_t[i] is None or ni_t[i] is None) else cfo_t[i] > ni_t[i])
            lev_now = div_(r.get("longTermDebt"), TA)
            lev_prev = div_(rp.get("longTermDebt"), rp.get("totalAssets"))
            chk(None if (lev_now is None or lev_prev is None) else lev_now <= lev_prev)
            cr_prev = div_(rp.get("totalCurrentAssets"), rp.get("totalCurrentLiabilities"))
            chk(None if (cr_v is None or cr_prev is None) else cr_v > cr_prev)
            chk(None if (sh is None or sh_prev is None) else sh <= sh_prev * 1.005)
            gm_prev = div_(gp_t[j], rev_t[j])
            gm_now = div_(gp_t[i], rev_t[i])
            chk(None if (gm_now is None or gm_prev is None) else gm_now > gm_prev)
            at_now = div_(rev_t[i], TA)
            at_prev = div_(rev_t[j], rp.get("totalAssets"))
            chk(None if (at_now is None or at_prev is None) else at_now > at_prev)
            if avail >= 7:
                put("piotroski_f", i, checks, 0)

            # Beneish M-score
            def _ix(a, b):
                v = div_(a, b)
                return clamp(v, 0.05, 20.0)

            TAp = rp.get("totalAssets")
            dsri = _ix(div_(r.get("receivables"), rev_t[i]),
                       div_(rp.get("receivables"), rev_t[j]))
            gmi = _ix(gm_prev, gm_now)
            aq_now = (None if TA in (None, 0) else
                      1 - ((CA or 0) + (r.get("ppeNet") or 0)) / TA)
            aq_prev = (None if TAp in (None, 0) else
                       1 - ((rp.get("totalCurrentAssets") or 0) + (rp.get("ppeNet") or 0)) / TAp)
            aqi = _ix(aq_now, aq_prev) if (aq_now and aq_prev and aq_now > 0 and aq_prev > 0) else 1.0
            sgi = _ix(rev_t[i], rev_t[j])
            dep_now, dep_prev = da_t[i], da_t[j]
            depi = None
            if all(v is not None for v in (dep_now, dep_prev, r.get("ppeNet"), rp.get("ppeNet"))):
                a = dep_prev / (dep_prev + rp["ppeNet"]) if (dep_prev + rp["ppeNet"]) > 0 else None
                b2 = dep_now / (dep_now + r["ppeNet"]) if (dep_now + r["ppeNet"]) > 0 else None
                depi = _ix(a, b2)
            sgai = _ix(div_(sga_t[i], rev_t[i]), div_(sga_t[j], rev_t[j]))
            tata = (None if (ni_t[i] is None or cfo_t[i] is None or not TA)
                    else (ni_t[i] - cfo_t[i]) / TA)
            lv_now = (None if not TA else
                      ((r.get("longTermDebt") or 0) + (CL or 0)) / TA)
            lv_prev = (None if not TAp else
                       ((rp.get("longTermDebt") or 0) + (rp.get("totalCurrentLiabilities") or 0)) / TAp)
            lvgi = _ix(lv_now, lv_prev)
            need = (dsri, gmi, aqi, sgi, sgai, tata, lvgi)
            if all(v is not None for v in need):
                m = (-4.84 + 0.92 * dsri + 0.528 * gmi + 0.404 * aqi + 0.892 * sgi
                     + 0.115 * (depi if depi is not None else 1.0)
                     - 0.172 * sgai + 4.679 * clamp(tata, -1, 1) - 0.327 * lvgi)
                put("beneish_m", i, m, 3)

    # ── forecasts (history + future) ─────────────────────────────────────────
    EST_ALIASES = {
        "est_revenue_avg": ("revenueAvg", "estimatedRevenueAvg"),
        "est_revenue_low": ("revenueLow", "estimatedRevenueLow"),
        "est_revenue_high": ("revenueHigh", "estimatedRevenueHigh"),
        "est_eps_avg": ("epsAvg", "estimatedEpsAvg"),
        "est_eps_low": ("epsLow", "estimatedEpsLow"),
        "est_eps_high": ("epsHigh", "estimatedEpsHigh"),
        "est_ebitda_avg": ("ebitdaAvg", "estimatedEbitdaAvg"),
        "est_ebit_avg": ("ebitAvg", "estimatedEbitAvg"),
        "est_net_income_avg": ("netIncomeAvg", "estimatedNetIncomeAvg"),
        "est_sga_avg": ("sgaExpenseAvg", "estimatedSgaExpenseAvg"),
        "est_num_analysts": ("numAnalystsRevenue", "numberAnalystEstimatedRevenue",
                             "numberAnalystsEstimatedRevenue", "numAnalystsEps"),
    }
    est_sorted = sorted(
        [e for e in est_rows if str(e.get("date") or "")[:10]],
        key=lambda e: str(e["date"])[:10],
    )
    cutoff = (datetime.now(timezone.utc) - timedelta(days=3900)).strftime("%Y-%m-%d")
    for e in est_sorted:
        d = str(e["date"])[:10]
        if d < cutoff:
            continue
        for out_k, names in EST_ALIASES.items():
            v = g(e, *names)
            if v is not None:
                P.setdefault(out_k, []).append([d, rnd(v, 4)])

    for k in list(P.keys()):
        P[k].sort(key=lambda t: t[0])

    try:
        _ta = compute_ta(daily_px)
        if _ta:
            _idx = {d: i for i, (d, _) in enumerate(daily_px)}
            _wd = [d for d, _ in weekly_px]
            for _n in (20, 50, 100, 200):
                P["px_ma%d" % _n] = _ta["wk_sample"](_ta["mas"][_n], _wd, _idx)
            P["px_bb_up"] = _ta["wk_sample"](_ta["bb_up"], _wd, _idx)
            P["px_bb_dn"] = _ta["wk_sample"](_ta["bb_dn"], _wd, _idx)
            P["rsi_14"] = _ta["wk_sample"](_ta["rsi"], _wd, _idx)
            tech_doc = {"events": _ta["events"], "status": _ta["status"]}
            _vp, _vev, _vst = volume_layer(vol_px, weekly_px)
            tech_doc["status"]["volume"] = _vst
            if _vp:
                P.update(_vp)
                tech_doc["events"] = sorted(tech_doc["events"] + _vev)[-55:]
    except Exception as _te:  # noqa: BLE001
        tech_doc = {"error": str(_te)[:120]}

    flags = []
    try:
        flags = derive_flags(P, lb)
        try:
            _smr = secmed_row((profile or {}).get("sector"))
            verdicts = derive_verdicts(
                P, lb, (profile or {}).get("sector"),
                _smr.get("med"), _smr.get("bands"),
                tech_doc if tech_doc and not tech_doc.get("error")
                else None)
        except Exception as _ve:  # noqa: BLE001
            verdicts = {"greens": [], "reds": [],
                        "summary": {"error": str(_ve)[:120]}}
    except Exception:  # noqa: BLE001
        pass
    vintage_days = 0
    try:
        vintage_days = record_vintage(sym, P)
    except Exception:  # noqa: BLE001
        pass

    return {
        "ok": True,
        "engine": "fundamental-graphs",
        "version": "1.11.2",
        "marker": "FUNDGRAPH_V1_OPS3462",
        "symbol": sym,
        "period": period,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile": profile,
        "n_periods": min(n, MAX_Q if period == "quarter" else MAX_A),
        "vintage_days": vintage_days,
        "flags": flags,
        "verdicts": verdicts,
        "whales_q": whales,
        "events": events,
        "catalog_n": len(P),
        "points": P,
        "earnings": earnings,
        "price": weekly_px,
        "tech": tech_doc,
        "factor_dna": _fdna,
        "stmt_rows": {"inc": len(inc), "bal": len(bal), "cf": len(cf)},
        "sources": [
            "FMP /stable income/balance/cash-flow statements + analyst-estimates + employee-count",
            "FMP /stable historical-price-eod/light (mcap_t = close_t x diluted shares_t)",
            "Ratios, growth/CAGR, HF quality set and all scores derived in-engine per period",
            "Reverse-DCF: 2-stage 10y, r=9%%, terminal 2.5%% (implied_fcf_growth_pct)",
            "Scores: Altman Z & Z'', Piotroski F, Beneish M, Sloan, Springate, Zmijewski, Fulmer H, KZ, Tobin's Q",
        ],
    }


# ── cache + handler ──────────────────────────────────────────────────────────
DCF_R, DCF_GT, DCF_YRS = 0.09, 0.025, 10


def implied_fcf_growth(ev, fcf):
    """Mauboussin expectations: growth g the price is paying for.
    Two-stage: g for 10y, terminal 2.5%, discount 9%. Bisection; None
    when FCF<=0 or EV<=0 or outside [-50%, +100%]."""
    if not ev or not fcf or ev <= 0 or fcf <= 0:
        return None

    def pv(g):
        v, f = 0.0, fcf
        for y in range(1, DCF_YRS + 1):
            f *= (1 + g)
            v += f / (1 + DCF_R) ** y
        v += (f * (1 + DCF_GT) / (DCF_R - DCF_GT)) / (1 + DCF_R) ** DCF_YRS
        return v

    lo, hi = -0.5, 1.0
    if pv(lo) >= ev or pv(hi) <= ev:
        return None
    for _ in range(64):
        mid = (lo + hi) / 2
        if pv(mid) < ev:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2 * 100


def _norm_date(d):
    d = str(d or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", d):
        return d[:10]
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", d)
    if m:
        return "%s-%02d-%02d" % (m.group(3), int(m.group(1)), int(m.group(2)))
    return None


def parse_congress_rows(rows, sym):
    """EVENTS_ENGINE_OPS3489 — pure: senate.transactions -> markers."""
    out = []
    for t in rows or []:
        if (t.get("ticker") or "").upper() != sym:
            continue
        d = _norm_date(t.get("tx_date"))
        if not d:
            continue
        ty = (t.get("type") or "").lower()
        side = "B" if "purchase" in ty else ("S" if "sale" in ty else "?")
        out.append([d, str(t.get("filer") or "")[:40], side,
                    str(t.get("amount") or "")[:30]])
    out.sort()
    return out[-40:]


def parse_insider_feeds(buys_doc, sells_doc, sym):
    """Pure: insider-trades big_buys/clusters + sell-cluster -> markers."""
    out = []
    for b in (buys_doc or {}).get("big_buys") or []:
        if (b.get("ticker") or "").upper() != sym:
            continue
        d = _norm_date(b.get("filed_at"))
        if d:
            out.append([d, "%s (%s)" % (str(b.get("insider") or "")[:28],
                                        str(b.get("role") or "")[:14]),
                        "B", rnd(b.get("value"), 0)])
    for c in (buys_doc or {}).get("clusters") or []:
        if (c.get("ticker") or "").upper() != sym:
            continue
        d = _norm_date(c.get("last_filing"))
        if d:
            out.append([d, "CLUSTER %s insiders" % c.get("insider_count"),
                        "B", rnd(c.get("total_value"), 0)])
    # sell-cluster feed is WINDOW-based (no per-cluster dates) — mark at
    # the feed's generated_at as a "selling now" flag (real schema:
    # n_distinct_sellers / total_sale_value_usd, ops 3489 audit).
    sd = _norm_date((sells_doc or {}).get("generated_at")
                    or (sells_doc or {}).get("as_of"))
    for c in (sells_doc or {}).get("clusters") or []:
        if (c.get("ticker") or "").upper() != sym or not sd:
            continue
        out.append([sd, "SELL CLUSTER %s insiders (30d)"
                    % c.get("n_distinct_sellers"),
                    "S", rnd(c.get("total_sale_value_usd"), 0)])
    out.sort()
    return out[-40:]


_EVFEEDS = {"ts": 0, "cg": None, "ib": None, "isl": None}


SECMED_KEY = "data/fundgraph/sector-medians.json"
SECMED_MAP = [("pe_ttm", "pe_ttm", 1), ("ps_ttm", "ps_ttm", 1),
              ("peg", "peg_ttm", 1), ("fcf_yield_pct", "fcf_yield_pct", 1),
              ("gross_margin_pct", "gross_margin_pct", 1),
              ("op_margin_pct", "operating_margin_pct", 1),
              ("m_score", "beneish_m", 1),
              ("sloan_accruals", "sloan_accruals_pct", 100)]


def build_sector_medians():
    """SECMED_OPS3493 — republish sector medians from the sanctioned
    forensic-screen feed (val_med block + row-computed extras), keyed by
    fg catalog metric ids. Tiny file, zero new data sources."""
    try:
        fs = json.loads(_s3.get_object(
            Bucket=S3_BUCKET, Key="data/forensic-screen.json")["Body"].read())
    except Exception:  # noqa: BLE001
        return None
    rows = fs.get("all_results") or []
    val_med = fs.get("sector_valuation_medians") or {}
    sectors = {}
    for sec, mm in val_med.items():
        for src, fg2, mult in SECMED_MAP[:4]:
            v = mm.get(src)
            if v is not None:
                sectors.setdefault(sec, {})[fg2] = rnd(v * mult, 4)
    grp = {}
    for r in rows:
        sec = r.get("sector") or "Unknown"
        for src, fg2, mult in SECMED_MAP[4:]:
            v = r.get(src)
            if v is not None:
                grp.setdefault((sec, fg2), []).append(v * mult)
    bands = {}
    for (sec, fg2), vals in grp.items():
        if len(vals) >= 5:
            vv = sorted(vals)
            sectors.setdefault(sec, {})[fg2] = rnd(vv[len(vv) // 2], 4)
    # p10/p90 distribution bands (SECMED v2, ops 3498) — computed from
    # rows for EVERY mapped key so elite-vs-sector has real deciles
    grp2 = {}
    for r in rows:
        sec = r.get("sector") or "Unknown"
        for src, fg2, mult in SECMED_MAP:
            v = r.get(src)
            if v is not None:
                grp2.setdefault((sec, fg2), []).append(v * mult)
    for (sec, fg2), vals in grp2.items():
        if len(vals) >= 10:
            vv = sorted(vals)
            n = len(vv)
            bands.setdefault(sec, {})[fg2] = {
                "p10": rnd(vv[n // 10], 4),
                "p90": rnd(vv[(9 * n) // 10], 4), "n": n}
    doc = {"as_of": datetime.now(timezone.utc).isoformat(),
           "source": "forensic-screen (S&P 500 cross-section)",
           "n_sectors": len(sectors),
           "keys": sorted({k for m2 in sectors.values() for k in m2}),
           "sectors": sectors, "bands": bands}
    _s3.put_object(Bucket=S3_BUCKET, Key=SECMED_KEY,
                   Body=json.dumps(doc, separators=(",", ":")).encode(),
                   ContentType="application/json",
                   CacheControl="public, max-age=3600")
    return doc


def fleet_events(sym):
    now = time.time()
    if _EVFEEDS["cg"] is None or now - _EVFEEDS["ts"] > 3600:
        def _ld(key):
            try:
                return json.loads(_s3.get_object(
                    Bucket=S3_BUCKET, Key=key)["Body"].read())
            except Exception:  # noqa: BLE001
                return {}
        _EVFEEDS.update(ts=now,
                        cg=_ld("data/congress-direct.json"),
                        ib=_ld("data/insider-trades.json"),
                        isl=_ld("data/insider-sell-cluster.json"))
    cg_rows = ((_EVFEEDS["cg"] or {}).get("senate") or {}).get("transactions")
    return {"congress": parse_congress_rows(cg_rows, sym),
            "insiders": parse_insider_feeds(_EVFEEDS["ib"], _EVFEEDS["isl"], sym)}


_WHALES = {"ts": 0, "map": None}


def whale_lookup(sym):
    """Fleet fusion: latest-quarter 13F dollar flows (13f-flows-by-ticker)."""
    now = time.time()
    if _WHALES["map"] is None or now - _WHALES["ts"] > 3600:
        try:
            raw = json.loads(_s3.get_object(
                Bucket=S3_BUCKET,
                Key="data/13f-flows-by-ticker.json")["Body"].read())
            mp = raw.get("t") or raw.get("by_ticker") or raw
            _WHALES.update(map=mp if isinstance(mp, dict) else {}, ts=now)
        except Exception:  # noqa: BLE001
            _WHALES.update(map={}, ts=now)
    r = (_WHALES["map"] or {}).get(sym) or {}
    if not isinstance(r, dict) or not r:
        return None
    return {"bought_usd": g(r, "b"), "sold_usd": g(r, "s"),
            "net_usd": g(r, "n", "net", "net_usd"),
            "whale_net_usd": g(r, "wn", "whale_net_usd"),
            "n_funds": g(r, "nf", "n_funds"),
            "held_usd": g(r, "tv", "held_usd")}


VERD_FIN = {"Financial Services", "Financials", "Financial",
            "Insurance", "Real Estate", "Banks"}  # == forensic FIN_SECTORS

# (key, label, kind, dir, green, red, sev_g, sev_r, fin_suppress)
#  kind: N=norm-latest  T=trend(vs lb*3 back)  S=vs-sector-median
#  dir : H=higher-better  L=lower-better  (S rules resolve via ratio bands)
VERDICT_RULES = [
    ("roic_pct",              "ROIC",                "N", "H", 15,   5,   2, 2, True),
    ("gross_margin_pct",      "Gross margin",        "N", "H", 55,  20,   1, 1, True),
    ("operating_margin_pct",  "Operating margin",    "N", "H", 25,   5,   1, 1, True),
    ("fcf_margin_pct",        "FCF margin",          "N", "H", 20,   0,   2, 3, True),
    ("fcf_yield_pct",         "FCF yield",           "N", "H",  6, 0.5,   2, 2, False),
    ("income_quality",        "Income quality",      "N", "H", 1.2, 0.7,  1, 2, True),
    ("piotroski_f",           "Piotroski F",         "N", "H",  7,   3,   2, 2, False),
    ("altman_z",              "Altman Z",            "N", "H",  3, 1.8,   2, 3, True),
    ("interest_coverage_ttm", "Interest coverage",   "N", "H", 12,   2,   1, 3, True),
    ("revenue_cagr_3y_pct",   "Revenue 3y CAGR",     "N", "H", 12,   0,   1, 2, False),
    ("eps_cagr_3y_pct",       "EPS 3y CAGR",         "N", "H", 12,  -5,   1, 2, False),
    ("fcf_cagr_3y_pct",       "FCF 3y CAGR",         "N", "H", 12, -10,   1, 2, False),
    ("net_shareholder_yield_pct", "Net shareholder yield", "N", "H", 4, -2, 2, 3, False),
    ("beneish_m",             "Beneish M",           "N", "L", -2.5, -1.78, 2, 3, True),
    ("sloan_accruals_pct",    "Sloan accruals",      "N", "L",  3,  10,   1, 3, True),
    ("netdebt_to_ebitda_ttm", "Net debt / EBITDA",   "N", "L", 0.5,  4,   2, 3, True),
    ("sbc_to_revenue_pct",    "SBC / revenue",       "N", "L",  2,  10,   1, 2, False),
    ("current_ratio",         "Current ratio",       "N", "H",  2,   1,   1, 2, True),
    ("operating_margin_pct",  "Op-margin trend",     "T", "H",  3,  -3,   2, 2, True),
    ("gross_margin_pct",      "Gross-margin trend",  "T", "H",  3,  -3,   1, 2, True),
    ("dso_days",              "DSO trend",           "T", "L", -10,  15,  1, 2, True),
    ("roe_pct",               "ROE",                 "N", "H", 18,   5,   1, 1, False),
    ("roa_pct",               "ROA",                 "N", "H", 10,   2,   1, 1, True),
    ("dio_days",              "Inventory-days trend", "T", "L", -12,  20,  1, 2, True),
    ("pe_ttm",                "P/E vs sector",       "S", "L", 0.70, 1.60, 2, 2, False),
    ("ps_ttm",                "P/S vs sector",       "S", "L", 0.60, 1.80, 1, 1, False),
    ("peg_ttm",               "PEG vs sector",       "S", "L", 0.70, 1.80, 2, 2, False),
    ("fcf_yield_pct",         "FCF yield vs sector", "S", "H", 1.50, 0.50, 2, 2, False),
    ("beneish_m",             "Beneish vs sector",   "S", "L", -0.5,  0.5, 1, 2, True),
]

_SECMED = {"ts": 0, "doc": None}


def secmed_row(sector):
    now = time.time()
    if _SECMED["doc"] is None or now - _SECMED["ts"] > 3600:
        try:
            _SECMED["doc"] = json.loads(_s3.get_object(
                Bucket=S3_BUCKET, Key=SECMED_KEY)["Body"].read())
        except Exception:  # noqa: BLE001
            _SECMED["doc"] = {}
        _SECMED["ts"] = now
    d = _SECMED["doc"] or {}
    return {"med": (d.get("sectors") or {}).get(sector) or {},
            "bands": (d.get("bands") or {}).get(sector) or {}}


EXTREME_NORM = {"roic_pct": ("H", 60), "interest_coverage_ttm": ("H", 100),
                "altman_z": ("H", 10), "piotroski_f": ("H", 9),
                "beneish_m": ("L", -3.5), "netdebt_to_ebitda_ttm": ("L", -2),
                "gross_margin_pct": ("H", 85), "fcf_margin_pct": ("H", 40)}

ELITE_NORM = {"roic_pct": ("H", 30), "gross_margin_pct": ("H", 75),
              "operating_margin_pct": ("H", 40), "fcf_margin_pct": ("H", 30),
              "fcf_yield_pct": ("H", 10), "income_quality": ("H", 1.5),
              "piotroski_f": ("H", 9), "altman_z": ("H", 8),
              "interest_coverage_ttm": ("H", 50),
              "revenue_cagr_3y_pct": ("H", 25), "eps_cagr_3y_pct": ("H", 25),
              "fcf_cagr_3y_pct": ("H", 25),
              "net_shareholder_yield_pct": ("H", 8),
              "beneish_m": ("L", -3.0), "sloan_accruals_pct": ("L", 1.0),
              "netdebt_to_ebitda_ttm": ("L", -1.0),
              "sbc_to_revenue_pct": ("L", 0.5), "current_ratio": ("H", 3),
              "roe_pct": ("H", 40), "roa_pct": ("H", 20)}


_RANKER = {"doc": None, "ts": 0}
_FOREN = {"rows": None, "ts": 0}
FACTOR_PREF = ["piotroski", "piotroski_f", "altman_z", "beneish_m",
               "sloan_accruals_pct", "fcf_yield_pct", "roic",
               "roic_pct", "pe_ttm", "peg", "quality_score",
               "composite", "three_statement_score", "strength_score"]
FACTOR_LOW = {"beneish_m", "sloan_accruals_pct", "pe_ttm", "peg",
              "concern_score"}
FACTOR_LBL = {"piotroski": "quality (Piotroski)",
              "piotroski_f": "quality (Piotroski)",
              "altman_z": "safety (Altman)",
              "beneish_m": "earnings integrity",
              "sloan_accruals_pct": "accrual discipline",
              "fcf_yield_pct": "FCF value", "pe_ttm": "P/E value",
              "peg": "PEG value", "roic": "capital efficiency",
              "roic_pct": "capital efficiency",
              "concern_score": "low concern",
              "strength_score": "statement strength"}


def ranker_rows():
    if _RANKER["doc"] is not None and time.time() - _RANKER["ts"] < 21600:
        return _RANKER["doc"]
    try:
        d = json.loads(_s3.get_object(
            Bucket=S3_BUCKET,
            Key="data/master-ranker.json")["Body"].read())
        rows = d.get("top_tickers") or []
        _RANKER["doc"] = rows if isinstance(rows, list) else []
        _RANKER["ts"] = time.time()
    except Exception:  # noqa: BLE001
        _RANKER["doc"] = []
        _RANKER["ts"] = time.time()
    return _RANKER["doc"]


def foren_rows():
    if _FOREN["rows"] is not None and time.time() - _FOREN["ts"] < 21600:
        return _FOREN["rows"]
    try:
        d = json.loads(_s3.get_object(
            Bucket=S3_BUCKET,
            Key="data/forensic-screen.json")["Body"].read())
        _FOREN["rows"] = d.get("all_results") or []
        _FOREN["ts"] = time.time()
    except Exception:  # noqa: BLE001
        _FOREN["rows"] = []
        _FOREN["ts"] = time.time()
    return _FOREN["rows"]


def factor_dna(sym):
    """Radar over OUR 503-name forensic cross-section (real
    distributions): schema-adaptive numeric columns (>=60% coverage,
    >=8 distinct), percentiles flipped to GOODNESS for lower-better
    axes. master-ranker joins as a conviction overlay when the ticker
    is in the top set. Named dormancy otherwise."""
    rows = foren_rows()
    if len(rows) < 30:
        return {"state": "insufficient",
                "why": "forensic universe rows <30"}
    me = next((r for r in rows
               if (r.get("ticker") or r.get("symbol")) == sym), None)
    conv = None
    for i, rr in enumerate(ranker_rows()):
        if (rr.get("ticker") or rr.get("symbol")) == sym:
            conv = {"rank": i + 1, "score": rnd(rr.get("score"), 1),
                    "n_systems": rr.get("n_systems"),
                    "systems": rr.get("systems"),
                    "rationale": (rr.get("rationale") or "")[:140]}
            break
    if not me:
        return {"state": "insufficient",
                "why": "%s not in the forensic universe" % sym,
                "conviction": conv}
    n = len(rows)
    axes, seen = [], set()
    cand = FACTOR_PREF + [k for k in sorted(me.keys())
                          if isinstance(me.get(k), (int, float))]
    for k in cand:
        if k in seen or len(axes) >= 7:
            continue
        seen.add(k)
        if not isinstance(me.get(k), (int, float)):
            continue
        col = [r.get(k) for r in rows]
        col = [v for v in col if isinstance(v, (int, float))]
        if len(col) < 0.6 * n or len(set(col)) < 8:
            continue
        below = sum(1 for v in col if v < me[k])
        eq = sum(1 for v in col if v == me[k])
        pct = 100.0 * (below + 0.5 * eq) / len(col)
        if k in FACTOR_LOW:
            pct = 100.0 - pct
        axes.append({"k": k, "label": FACTOR_LBL.get(
            k, k.replace("_", " ")), "val": rnd(me[k], 3),
            "pct": rnd(pct, 1),
            "dir": "L" if k in FACTOR_LOW else "H"})
    if len(axes) < 4:
        return {"state": "insufficient",
                "why": "only %d comparable factor columns" % len(axes),
                "conviction": conv}
    return {"state": "ok", "n_universe": n, "axes": axes,
            "conviction": conv}


def derive_verdicts(P, lb, sector, med, bands=None, tech=None):
    """VERDICTS_ENGINE_OPS3495 — green/red verdict layer over the doc.
    Pure; NEVER emits a verdict without a numeric value; financial-sector
    rules suppressed per fleet doctrine (list reported)."""
    def v(key, back=0):
        a = P.get(key) or []
        i = len(a) - 1 - back
        return a[i][1] if 0 <= i < len(a) else None

    fin = (sector or "") in VERD_FIN
    bands = bands or {}
    out, suppressed = [], []

    def emit(side, sev, key, label, why, val, ref, basis):
        e = {"k": key, "side": side, "sev": sev, "label": label,
             "why": why, "val": rnd(val, 3),
             "ref": rnd(ref, 3) if ref is not None else None,
             "basis": basis}
        # ELITE upgrade (ops 3498): astonishing-by-norm or sector
        # top-decile, greens only, real value required
        if side == "G" and val is not None:
            norm_el = decile_el = False
            en = ELITE_NORM.get(key)
            if en and ((en[0] == "H" and val >= en[1])
                       or (en[0] == "L" and val <= en[1])):
                norm_el = True
                e["elite"] = True
                e["why"] += " \u2014 ELITE (%s %.2f)" % (
                    "\u2265" if en[0] == "H" else "\u2264", en[1])
            bd = bands.get(key)
            if bd and bd.get("n", 0) >= 10:
                dr9 = next((r5[3] for r5 in VERDICT_RULES
                            if r5[0] == key), "H")
                if (dr9 == "H" and bd.get("p90") is not None
                        and val >= bd["p90"]) or \
                   (dr9 == "L" and bd.get("p10") is not None
                        and val <= bd["p10"]):
                    decile_el = True
            xn = EXTREME_NORM.get(key)
            if e.get("elite") and (
                    (norm_el and decile_el)
                    or (xn and ((xn[0] == "H" and val >= xn[1])
                                or (xn[0] == "L" and val <= xn[1])))):
                e["extreme"] = True
            if not e.get("elite") and bd and bd.get("n", 0) >= 10:
                dr2 = next((r5[3] for r5 in VERDICT_RULES
                            if r5[0] == key), "H")
                if dr2 == "H" and bd.get("p90") is not None \
                        and val >= bd["p90"]:
                    e["elite"] = True
                    e["why"] += " \u2014 ELITE top decile of sector (p90 %.2f)" % bd["p90"]
                elif dr2 == "L" and bd.get("p10") is not None \
                        and val <= bd["p10"]:
                    e["elite"] = True
                    e["why"] += " \u2014 ELITE top decile of sector (p10 %.2f)" % bd["p10"]
        out.append(e)

    for key, label, kind, dr, gthr, rthr, sg, sr, fsup in VERDICT_RULES:
        if fsup and fin:
            suppressed.append(label)
            continue
        if kind == "N":
            val = v(key)
            if val is None:
                continue
            if dr == "H":
                if val >= gthr:
                    emit("G", sg, key, label,
                         "%s %.2f \u2265 %.2f norm" % (label, val, gthr),
                         val, gthr, "norm")
                elif val <= rthr:
                    emit("R", sr, key, label,
                         "%s %.2f \u2264 %.2f norm" % (label, val, rthr),
                         val, rthr, "norm")
            else:
                if val <= gthr:
                    emit("G", sg, key, label,
                         "%s %.2f \u2264 %.2f norm" % (label, val, gthr),
                         val, gthr, "norm")
                elif val >= rthr:
                    emit("R", sr, key, label,
                         "%s %.2f \u2265 %.2f norm" % (label, val, rthr),
                         val, rthr, "norm")
        elif kind == "T":
            back = lb * 3
            now_v, old_v = v(key), v(key, back)
            if now_v is None or old_v is None:
                continue
            d = now_v - old_v
            if dr == "H":
                if d >= gthr:
                    emit("G", sg, key, label,
                         "%s +%.1f over 3y (%.1f\u2192%.1f)"
                         % (label, d, old_v, now_v), d, old_v, "trend")
                elif d <= rthr:
                    emit("R", sr, key, label,
                         "%s %.1f over 3y (%.1f\u2192%.1f)"
                         % (label, d, old_v, now_v), d, old_v, "trend")
            else:
                pct = (d / abs(old_v) * 100) if old_v else None
                if pct is None:
                    continue
                if pct <= gthr:
                    emit("G", sg, key, label,
                         "%s %.0f%% over 3y (%.1f\u2192%.1f)"
                         % (label, pct, old_v, now_v), pct, old_v, "trend")
                elif pct >= rthr and d >= 8:
                    emit("R", sr, key, label,
                         "%s +%.0f%% over 3y (%.1f\u2192%.1f)"
                         % (label, pct, old_v, now_v), pct, old_v, "trend")
        else:  # S — vs sector median
            val, mv = v(key), (med or {}).get(key)
            if val is None or mv is None:
                continue
            if key == "beneish_m":
                d = val - mv
                if d <= gthr:
                    emit("G", sg, key, label,
                         "%s %.2f vs sector %.2f (cleaner)"
                         % (label, val, mv), val, mv, "sector")
                elif d >= rthr:
                    emit("R", sr, key, label,
                         "%s %.2f vs sector %.2f" % (label, val, mv),
                         val, mv, "sector")
                continue
            if val <= 0 or mv <= 0:
                continue
            ratio = val / mv
            cheap = ratio <= gthr if dr == "L" else ratio >= gthr
            rich = ratio >= rthr if dr == "L" else ratio <= rthr
            if cheap:
                emit("G", sg, key, label,
                     "%s %.2f vs sector %.2f (%.0f%%)"
                     % (label, val, mv, (ratio - 1) * 100), val, mv, "sector")
            elif rich:
                emit("R", sr, key, label,
                     "%s %.2f vs sector %.2f (%+.0f%%)"
                     % (label, val, mv, (ratio - 1) * 100), val, mv, "sector")

    # ── TECH-BASIS VERDICTS (ops 3505): judge the technical state too.
    # Never fin-suppressed, never elite; regime-level (state now), not
    # event spam. Requires a clean tech doc.
    try:
        tst = (tech or {}).get("status") or {}
        tev = (tech or {}).get("events") or []
        if tst and not (tech or {}).get("error"):
            lc, m200 = tst.get("last_close"), tst.get("ma200")
            if lc is not None and m200:
                pv = tst.get("pct_vs_200")
                if tst.get("above_200"):
                    emit("G", 1, "px_vs_200", "Price vs 200-DMA",
                         "Price %+.1f%% above the 200-DMA" % (pv or 0),
                         pv, m200, "tech")
                else:
                    emit("R", 1, "px_vs_200", "Price vs 200-DMA",
                         "Price %.1f%% below the 200-DMA" % (pv or 0),
                         pv, m200, "tech")
            if tst.get("bull_stack"):
                emit("G", 1, "ma_stack", "MA stack",
                     "Bull MA stack 20>50>100>200", 1, None, "tech")
            ma50 = tst.get("ma50")
            if ma50 and m200:
                reg_up = ma50 > m200
                want = "GC_50_200" if reg_up else "DC_50_200"
                since = ([e[0] for e in tev if e[1] == want] or [None])[-1]
                if reg_up:
                    emit("G", 1, "ma_regime", "50/200 regime",
                         "Golden-cross regime (50>200)%s"
                         % (" since %s" % since if since else ""),
                         1, None, "tech")
                else:
                    emit("R", 2, "ma_regime", "50/200 regime",
                         "Death-cross regime (50<200)%s"
                         % (" since %s" % since if since else ""),
                         -1, None, "tech")
            for p9 in (tst.get("patterns") or []):
                if not p9.get("confirmed"):
                    continue
                if p9.get("type") == "DBL_TOP":
                    emit("R", 2, "dbl_top", "Double top",
                         "Double top confirmed %s (neck %.2f)"
                         % (p9.get("d"), p9.get("neck") or 0),
                         p9.get("level"), p9.get("neck"), "tech")
                elif p9.get("type") == "DBL_BOTTOM":
                    emit("G", 2, "dbl_bottom", "Double bottom",
                         "Double bottom confirmed %s (neck %.2f)"
                         % (p9.get("d"), p9.get("neck") or 0),
                         p9.get("level"), p9.get("neck"), "tech")
            r14 = tst.get("rsi14")
            if r14 is not None and r14 >= 80:
                emit("R", 1, "rsi_hot", "RSI-14",
                     "RSI %.0f overbought (\u226580)" % r14,
                     r14, 80, "tech")
            vst9 = tst.get("volume") or {}
            if vst9.get("state") == "ok" and (vst9.get("rvol") or 0) >= 3:
                emit("R", 1, "rvol_hot", "Relative volume",
                     "RVOL %.1fx 20d average \u2014 unusual activity"
                     % vst9["rvol"], vst9["rvol"], 3, "tech")
    except Exception:  # noqa: BLE001
        pass

    out.sort(key=lambda x: (-(1 if x.get("elite") else 0),
                            -x["sev"], x["k"]))
    # caps apply to fundamentals only; tech verdicts are few (<=7) and
    # always shown, appended after the capped fundamental block
    g_f = [x for x in out if x["side"] == "G" and x["basis"] != "tech"]
    r_f = [x for x in out if x["side"] == "R" and x["basis"] != "tech"]
    g_t = [x for x in out if x["side"] == "G" and x["basis"] == "tech"]
    r_t = [x for x in out if x["side"] == "R" and x["basis"] == "tech"]
    greens = g_f[:14] + g_t
    reds = r_f[:12] + r_t
    return {"greens": greens, "reds": reds,
            "summary": {"n_elite": len([x for x in out
                                        if x.get("elite")]),
                        "n_green": len([x for x in out if x["side"] == "G"]),
                        "n_red": len([x for x in out if x["side"] == "R"]),
                        "fin_suppressed": suppressed if fin else []}}


def derive_flags(P, lb):
    """Auto-forensic digest: rule engine over the 200-series doc.
    Reads latest vs 1y (lb periods) and 2y back; returns top-6 by severity.
    FLAGS_ENGINE_OPS3480"""
    def v(key, back=0):
        a = P.get(key) or []
        i = len(a) - 1 - back
        return a[i][1] if 0 <= i < len(a) else None

    F = []

    def add(fid, sev, msg, keys, latest, prior):
        F.append({"id": fid, "sev": sev, "msg": msg, "keys": keys,
                  "latest": rnd(latest, 3), "prior": rnd(prior, 3)})

    dso, dso1 = v("dso_days"), v("dso_days", lb)
    rg = v("revenue_yoy_pct")
    if dso and dso1 and rg is not None and dso >= dso1 * 1.2 and rg < 5:
        add("DSO_STRETCH", 2,
            "Receivables days +%d%% YoY while revenue growth is %.1f%% — "
            "channel stuffing risk" % (round((dso / dso1 - 1) * 100), rg),
            ["dso_days", "revenue_yoy_pct"], dso, dso1)
    cd, cd2 = v("capex_to_da"), v("capex_to_da", 2 * lb)
    if cd is not None and cd < 0.8 and (cd2 is None or cd < cd2):
        add("CAPEX_STARVED", 1,
            "Capex only %.2fx D&A and falling — asset base under-reinvested"
            % cd, ["capex_to_da"], cd, cd2)
    dpo, dpo1 = v("dpo_days"), v("dpo_days", lb)
    if dpo and dpo1 and dpo >= dpo1 * 1.25:
        add("DPO_GAMES", 1,
            "Days payable +%d%% YoY — cash flow flattered by stretching "
            "suppliers" % round((dpo / dpo1 - 1) * 100),
            ["dpo_days"], dpo, dpo1)
    iq, iq1 = v("income_quality"), v("income_quality", lb)
    if iq is not None and iq1 is not None and iq < 0.8 and iq < iq1:
        add("EARNINGS_QUALITY", 3,
            "CFO covers only %.0f%% of net income and deteriorating — "
            "accrual-driven earnings" % (iq * 100),
            ["income_quality", "sloan_accruals_pct"], iq, iq1)
    dl, dl1 = v("share_count_yoy_pct"), v("share_count_yoy_pct", lb)
    if dl is not None and dl > 3 and (dl1 is None or dl > dl1):
        add("DILUTION_ACCEL", 2,
            "Share count +%.1f%%/yr and accelerating — dilution eating "
            "returns" % dl, ["share_count_yoy_pct", "sbc_to_revenue_pct"],
            dl, dl1)
    gm, gm1 = v("gross_margin_pct"), v("gross_margin_pct", lb)
    if gm is not None and gm1 is not None and gm <= gm1 - 2:
        add("MARGIN_ROLL", 2,
            "Gross margin -%.0fbp YoY — pricing power eroding"
            % ((gm1 - gm) * 100), ["gross_margin_pct"], gm, gm1)
    sl = v("sloan_accruals_pct")
    if sl is not None and sl > 10:
        add("SLOAN_HIGH", 2,
            "Sloan accruals %.1f%% of assets — earnings persistence risk"
            % sl, ["sloan_accruals_pct", "income_quality"], sl, None)
    nd, nd1 = v("netdebt_to_ebitda_ttm"), v("netdebt_to_ebitda_ttm", lb)
    if nd is not None and nd1 is not None and nd >= nd1 + 1 and nd > 2:
        add("LEVERAGE_UP", 2,
            "Net debt/EBITDA %.1fx, +%.1f turns YoY — balance sheet "
            "loading up" % (nd, nd - nd1),
            ["netdebt_to_ebitda_ttm", "netDebt"], nd, nd1)
    bm = v("beneish_m")
    if bm is not None and bm > -1.78:
        add("BENEISH", 3,
            "Beneish M %.2f above -1.78 — manipulation-risk zone" % bm,
            ["beneish_m", "sloan_accruals_pct", "dso_days"], bm, None)
    sb, sb1 = v("sbc_to_revenue_pct"), v("sbc_to_revenue_pct", lb)
    if sb is not None and sb1 is not None and sb >= sb1 + 1.5:
        add("SBC_CREEP", 1,
            "SBC/revenue +%.1fpp YoY to %.1f%% — comp creep diluting "
            "owners" % (sb - sb1, sb),
            ["sbc_to_revenue_pct", "share_count_yoy_pct"], sb, sb1)
    F.sort(key=lambda f: -f["sev"])
    return F[:6]


VINTAGE_PREFIX = "data/fundgraph/vintage/"
VINTAGE_CAP = 500


def record_vintage(sym, P):
    """Daily snapshot of forward estimates -> self-built IBES-style ledger.

    One row per calendar day per symbol: the street's CURRENT view of the
    next ~6 forward periods for revenue + EPS. Revision-momentum series
    become computable automatically once history accrues (nobody retail
    has this; FMP only serves current vintage). Idempotent per day."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def fwd(key):
        return [[d, v] for d, v in (P.get(key) or []) if d > today][:6]

    eps, rev = fwd("est_eps_avg"), fwd("est_revenue_avg")
    if not eps and not rev:
        return 0
    key = f"{VINTAGE_PREFIX}{sym}.json"
    try:
        led = json.loads(_s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
        if not isinstance(led.get("rows"), list):
            led = {"symbol": sym, "rows": []}
    except Exception:  # noqa: BLE001
        led = {"symbol": sym, "rows": []}
    if led["rows"] and led["rows"][-1].get("d") == today:
        return len(led["rows"])
    led["rows"].append({"d": today, "eps": eps, "rev": rev})
    led["rows"] = led["rows"][-VINTAGE_CAP:]
    try:
        _s3.put_object(Bucket=S3_BUCKET, Key=key,
                       Body=json.dumps(led, separators=(",", ":")).encode(),
                       ContentType="application/json")
    except Exception:  # noqa: BLE001
        pass
    return len(led["rows"])


def cache_key(sym, period):
    return f"{CACHE_PREFIX}{sym}_{period}_{CACHE_VER}.json"


def load_cache(sym, period):
    try:
        obj = _s3.get_object(Bucket=S3_BUCKET, Key=cache_key(sym, period))
        doc = json.loads(obj["Body"].read())
        ts = datetime.fromisoformat(doc["generated_at"].replace("Z", "+00:00"))
        if (datetime.now(timezone.utc) - ts).total_seconds() < CACHE_TTL_SEC:
            return doc
    except Exception:  # noqa: BLE001
        pass
    return None


def save_cache(doc):
    try:
        _s3.put_object(
            Bucket=S3_BUCKET,
            Key=cache_key(doc["symbol"], doc["period"]),
            Body=json.dumps(doc, separators=(",", ":")).encode(),
            ContentType="application/json",
            CacheControl="public, max-age=900",
        )
    except Exception as e:  # noqa: BLE001
        print(f"cache write failed: {e}")


STATIC_CORE = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
               "AVGO", "BRK-B", "JPM", "V", "MA", "UNH", "LLY", "XOM", "CVX",
               "HD", "COST", "PG", "JNJ", "ABBV", "MRK", "NFLX", "AMD", "CRM",
               "ORCL", "ADBE", "WMT", "KO", "PEP", "CSCO", "QCOM", "TXN",
               "GE", "CAT", "CHTR"]


def touch_hit(sym):
    """Demand marker — the daily warmer refreshes symbols hit in the last 7d."""
    try:
        _s3.put_object(Bucket=S3_BUCKET, Key=f"data/fundgraph/hits/{sym}", Body=b"")
    except Exception:  # noqa: BLE001
        pass


def recent_hits(days=7, cap=80):
    out = []
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        tok = None
        while True:
            kw = {"Bucket": S3_BUCKET, "Prefix": "data/fundgraph/hits/",
                  "MaxKeys": 500}
            if tok:
                kw["ContinuationToken"] = tok
            r = _s3.list_objects_v2(**kw)
            for o in r.get("Contents", []):
                if o["LastModified"] >= cutoff:
                    out.append(o["Key"].rsplit("/", 1)[-1])
            tok = r.get("NextContinuationToken")
            if not tok:
                break
    except Exception:  # noqa: BLE001
        pass
    return out[:cap]


def get_doc(sym, period, refresh=False, track=False):
    if track:
        touch_hit(sym)
    if not refresh:
        cached = load_cache(sym, period)
        if cached:
            cached["cached"] = True
            return cached
    doc = build_doc(sym, period)
    save_cache(doc)
    doc["cached"] = False
    return doc


def _resp(status, doc, headers_in):
    body = json.dumps(doc, separators=(",", ":"))
    hdrs = {
        "Content-Type": "application/json",
        "Cache-Control": "public, max-age=600",
        # NOTE: no CORS headers here — the Function URL's Cors config is the
        # single authority. Emitting ACAO from the function too produces
        # duplicate headers ("*, *") which browsers reject (ops 3464).
    }
    ae = ""
    if isinstance(headers_in, dict):
        low = {str(k).lower(): str(v) for k, v in headers_in.items()}
        ae = low.get("accept-encoding", "")
        if low.get("x-gz") == "1":
            ae = "gzip"
    if "gzip" in ae.lower() and len(body) > 1400:
        gz = gzip.compress(body.encode())
        hdrs["Content-Encoding"] = "gzip"
        return {"statusCode": status, "headers": hdrs,
                "body": base64.b64encode(gz).decode(), "isBase64Encoded": True}
    return {"statusCode": status, "headers": hdrs, "body": body}


def _valid_symbol(s):
    s = (s or "").strip().upper()
    if 0 < len(s) <= 12 and all(c.isalnum() or c in ".-^" for c in s):
        return s
    return None


def lambda_handler(event, context):  # noqa: ARG001
    event = event or {}
    if not FMP_KEY:
        return _resp(500, {"ok": False, "error": "FMP_KEY not set"}, {})

    if isinstance(event, dict) and event.get("warm_auto"):
        t0 = time.time()
        syms = list(dict.fromkeys(
            STATIC_CORE + [s2 for s2 in recent_hits()
                           if _valid_symbol(s2)]))[:60]
        annual_too = datetime.now(timezone.utc).weekday() == 0
        symdir_n = None
        secmed_n = None
        try:  # NIGHTLY (ops 3508): cross-sectional medians+bands daily
            sm = build_sector_medians()
            secmed_n = sm["n_sectors"] if sm else -1
        except Exception:  # noqa: BLE001
            secmed_n = -1
        if annual_too:
            try:
                symdir_n = len(load_symdir(force=True))
            except Exception:  # noqa: BLE001
                symdir_n = -1
        built, errors, skipped = [], {}, []

        def time_left():
            try:
                return context.get_remaining_time_in_millis() / 1000.0
            except Exception:  # noqa: BLE001
                return 900 - (time.time() - t0)

        for sym in syms:
            if time_left() < 50:
                skipped = syms[syms.index(sym):]
                break
            try:
                get_doc(sym, "quarter", refresh=True)
                if annual_too and sym in STATIC_CORE and time_left() > 70:
                    get_doc(sym, "annual", refresh=True)
                built.append(sym)
            except Exception as e:  # noqa: BLE001
                errors[sym] = str(e)[:120]
        return {"ok": True, "mode": "warm_auto", "version": "1.11.2",
                "marker": "FUNDGRAPH_V1_OPS3462",
                "symbols_n": len(syms), "built": len(built),
                "annual_pass": annual_too, "symdir_n": symdir_n, "secmed_n": secmed_n, "errors": errors,
                "skipped_for_time": skipped,
                "elapsed_s": round(time.time() - t0, 1)}

    if isinstance(event, dict) and event.get("warm"):
        out = {}
        periods = event.get("periods") or ["quarter"]
        for s in event["warm"][:12]:
            sym = _valid_symbol(s)
            if not sym:
                continue
            for p in periods:
                if p not in ("quarter", "annual"):
                    continue
                try:
                    d = get_doc(sym, p, refresh=bool(event.get("refresh")))
                    out[f"{sym}_{p}"] = {"ok": True, "n": d.get("n_periods"),
                                         "keys": len(d.get("points", {}))}
                except Exception as e:  # noqa: BLE001
                    out[f"{sym}_{p}"] = {"ok": False, "error": str(e)[:180]}
        return {"ok": True, "warmed": out, "marker": "FUNDGRAPH_V1_OPS3462",
                "version": "1.11.2"}

    qp = event.get("queryStringParameters") or {}
    if not qp and event.get("rawQueryString"):
        qp = dict(urllib.parse.parse_qsl(event["rawQueryString"]))
    if not qp and event.get("symbol"):
        qp = {"symbol": event.get("symbol"), "period": event.get("period", "quarter"),
              "refresh": "1" if event.get("refresh") else ""}
    headers_in = event.get("headers") or {}

    if str(qp.get("symdir") or "") == "1":
        try:
            rows = load_symdir(force=True)
            return _resp(200, {"ok": True, "n": len(rows),
                               "diag": _SYMDIR.get("diag"),
                               "sample": rows[:3],
                               "version": "1.11.2"}, headers_in)
        except Exception as e:  # noqa: BLE001
            return _resp(502, {"ok": False, "error": str(e)[:240],
                               "diag": _SYMDIR.get("diag")}, headers_in)

    if str(qp.get("secmed") or "") == "1" or (
            isinstance(event, dict) and event.get("sector_medians")):
        d = build_sector_medians()
        return _resp(200 if d else 502,
                     d or {"ok": False, "error": "forensic feed unavailable"},
                     headers_in)

    srch = (qp.get("search") or "").strip()
    if srch:
      try:
        q = "".join(c for c in srch if c.isalnum() or c in " .-&")[:40]

        dir_hits = None
        try:
            dir_hits = symdir_search(q)
        except Exception:  # noqa: BLE001
            dir_hits = None
        if dir_hits:
            return _resp(200, {"ok": True, "query": q, "results": dir_hits,
                               "src": "symdir",
                               "marker": "FUNDGRAPH_V1_OPS3462"}, headers_in)

        def _srch(ep):
            try:
                rows = _fmp(f"{ep}?query={urllib.parse.quote(q)}&limit=10")
                return rows if isinstance(rows, list) else []
            except Exception:  # noqa: BLE001
                return []

        # name matches first (company-name intent: "micro" -> Microsoft),
        # then ticker matches ("aap" -> AAPL); US listings outrank foreign.
        merged, seen = [], set()
        for r0 in _srch("search-name") + _srch("search-symbol"):
            sy = str(r0.get("symbol") or "").upper()
            if not sy or sy in seen:
                continue
            seen.add(sy)
            merged.append({
                "symbol": sy,
                "name": r0.get("name") or r0.get("companyName") or "",
                "exchange": r0.get("exchangeShortName")
                or r0.get("exchange") or ""})
        qU = q.upper()
        # GOTCHA (ops 3469): never call list.index() inside a sort key of the
        # list being sorted — CPython empties the list during sort -> ValueError.
        ranked = sorted(
            enumerate(merged),
            key=lambda t: (0 if t[1]["symbol"] == qU else 1,
                           0 if "." not in t[1]["symbol"] else 1,
                           t[0]))
        results = [r for _, r in ranked][:8]
        if not results:
            return _resp(502, {"ok": False, "error": "search unavailable"},
                         headers_in)
        return _resp(200, {"ok": True, "query": q, "results": results,
                           "src": "fmp",
                           "marker": "FUNDGRAPH_V1_OPS3462"}, headers_in)
      except Exception as e:  # noqa: BLE001
        return _resp(502, {"ok": False, "error": "search: " + str(e)[:180]},
                     headers_in)

    sym = _valid_symbol(qp.get("symbol") or qp.get("s"))
    if not sym:
        return _resp(400, {"ok": False,
                           "error": "symbol required, e.g. ?symbol=AAPL&period=quarter",
                           "marker": "FUNDGRAPH_V1_OPS3462"}, headers_in)
    period = (qp.get("period") or "quarter").lower()
    if period == "ttm":
        period = "quarter"
    if period not in ("quarter", "annual"):
        return _resp(400, {"ok": False, "error": "period must be quarter|annual"},
                     headers_in)
    if str(qp.get("gz") or "") == "1":
        headers_in = dict(headers_in or {})
        headers_in["x-gz"] = "1"
    try:
        doc = get_doc(sym, period, refresh=str(qp.get("refresh") or "") in ("1", "true"), track=True)
        return _resp(200, doc, headers_in)
    except Exception as e:  # noqa: BLE001
        return _resp(502, {"ok": False, "symbol": sym, "period": period,
                           "error": str(e)[:300]}, headers_in)
