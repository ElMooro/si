"""wide.py - the strength desk's whole-market table: every listed common stock and plain ETF against the S&P 500.

Khalid 2026-11-20 12:44 ET, on a day the S&P 500 was down 2%: "Strength vs S&P 500 needs improvement like today
everything is dumping hard however i cant see which stocks and etfs are showing strength and dumping less than sp500".

The desk ranked about 850 names from last night's closes and only its 163 sleeve names carried a live price, so a dump
day could not be read from it.  This module measures EVERY listed common stock and plain ETF that trades (the factor
book's identity, the same $250k-a-day floor the Wyckoff scan and the volatility desk use) from the Polygon EOD bank and
writes one row per name into data/strength-wide.json:

  * the last session's own bar (open, high, low, close, volume, VWAP) and the close before it - so the page can place
    TODAY's forming bar (data/warm/polygon-full/today.json.gz, the 5-minute Polygon lane) against the last close with
    no further arithmetic on the server, and can show the last session the same way when the market is shut;
  * excess return against SPY on 1 / 5 / 21 / 63 / 126 sessions and `rs`, the percentile of the desk's own composite
    (cross-sectional z, 10 / 20 / 30 / 25 / 15) among the name's kind - stocks among stocks, ETFs among ETFs - plus
    `rsp`, the same rank as it stood one session earlier (the rank a name carried INTO the last session);
  * the dump-day record of the last 63 sessions: on how many of SPY's dump days (down 1% or more) the name held up
    (flat, up, or down less than SPY), its streak of the most recent ones, its mean excess on them; the same for
    rally days; the mean excess on all of SPY's down days and up days;
  * ATR, the place in the 52-week range, the 50 / 200-day averages, and why a row is set aside (a stock pinned to a
    deal price is flat on every dump day by construction; an unexplained 3-to-1 jump; thin trading).

`last_session` is the engine's own read of the last session by exactly the rules the page applies to today's forming
bars: who was up, flat, down less than SPY, down more - by size, sector, asset class and by the rank carried in.

A READER: S3 only (the bank, the factor book, Polygon's split reference, the banked S&P 500 list).  A partial table is
never published: when the read runs out of time or measures too few names, the previous publication stands.
History hygiene is the leaders engine's (leaders_tape, a byte-identical copy): a reused ticker's previous holder is cut
off, unadjusted splits are rebased, a separation is not read as a crash.
"""
from __future__ import annotations

import gzip
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import bank_enrich          # aws/shared: the whole-market factor book (identity, market cap, dollar volume) - S3 only
import fmp_bank             # aws/shared: the banked S&P 500 list - S3 only
import polygon_bankfirst    # aws/shared: the Polygon EOD bank's layout (ROOT, MANIFEST, FIELDS)

import leaders_tape as T    # the leaders engine's pure tape module, byte-identical copy

CONTRACT = "strength-wide.v1"
BENCH = "SPY"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
TODAY_KEY = "data/warm/polygon-full/today.json.gz"                 # justhodl-polygon-today: read by the page, named here as a source
BANK_SPLITS_ROOT = "data/warm/polygon-bank/splits/"
MIN_DV = float(os.environ.get("WIDE_MIN_DOLLAR_VOL", "250000"))    # 20-session average dollar volume a name must trade to be measured
MIN_SESSIONS = 22
MIN_SCANNED = int(os.environ.get("WIDE_MIN_SCANNED", "2500"))      # fewer than this is a broken read, not a market
MAX_ERROR_SHARE = 0.05
READ_TAIL = 420                # bars parsed per name: 252 for the year's range, the rest for the averages and the hygiene scans
RANK_MIN = 64                  # aligned sessions a name needs to be ranked: the month and the quarter must both be measurable
RECORD_N = 63                  # the dump-day / rally-day record: trailing sessions
PIN_SPREAD_M = 3.5             # a stock whose last 21 sessions fit inside this percent, low to high, trades like a pending deal
JUMP_FLAG = 3.0                # a close-to-close move beyond 3-to-1 either way that no split explains
JUMP_ASIDE = 126               # ... inside the longest horizon the rank reads
THIN_WINDOW = 63
THIN_MISS = 3
SUSPECT = (0.56, 1.8)          # a move beyond these ratios with no recorded split is not counted on the page until a close confirms it
SPLIT_AHEAD_DAYS = 21
BOARD_MIN_MCAP = 1e9
BIG_MCAP = 10e9
ETF_MIN_ADV = 5e6
GEARED_CLS = "leveraged"       # a fund the desk's class map calls leveraged or inverse (an inverse fund tops every dump day and says nothing about strength): measured, set aside
BOARD_SKIP_CLS = ("volatility",)   # long-volatility funds rise when SPY falls by construction: counted in their class, kept off the ETF boards
RECORD_MIN = 3                 # dump days a name needs before its record is read
IND_MIN = 5
BOARD_N = 25
THREADS = int(os.environ.get("WIDE_THREADS", "16"))
RESERVE_S = 40.0                # seconds kept for the ranks, the read and the put
MIN_BUDGET_S = 60.0             # with less than this left after the reserve the pass does not start
BOOK_FIELDS = ["mcap", "dollar_vol_20d", "ipo_date", "exchange"]
NOT_COMMON_SUFFIX = re.compile(r"-(R|RI|RT|RW|WS|WT|W|U|UN|CVR)$")
GEARED_RX = re.compile(r"\b(\d(\.\d+)?x|ultrapro|ultrashort|inverse|leveraged|bull|bear)\b|\bultra\b(?![\s-]*short)", re.I)
COLS = ["t", "name", "kind", "cls", "sector", "industry", "mcap", "adv", "sp",
        "pc", "o", "h", "l", "c", "v", "vw", "av",
        "x1", "x5", "x21", "x63", "x126", "rs", "rsp",
        "dn", "dh", "ds", "dx", "rn", "rb", "rx", "dnx", "upx", "dnp", "dhp",
        "atr", "p52", "ma", "n", "why", "jd"]
HKEY = {"d": "x1", "w": "x5", "m": "x21", "q": "x63", "h": "x126"}
OUTCOMES = ("up", "flat", "down_less", "down_more")
SOURCES = [
    {"engine": "justhodl-polygon-eod", "key": "data/warm/polygon-full/eod/manifest.json", "role": "daily bars of every US stock and ETF (grouped daily folded per ticker)", "schedule": "daily 03:10 UTC"},
    {"engine": "justhodl-bank-factors", "key": "data/warm/factor-book/manifest.json", "role": "identity of every listed security: kind, sector, industry, market value, dollar volume", "schedule": "daily 02:50 UTC"},
    {"engine": "justhodl-polygon-bank", "key": "data/warm/polygon-bank/manifest.json", "role": "Polygon's split reference (execution date, split_from / split_to), past and announced", "schedule": "daily 22:41 UTC"},
    {"engine": "justhodl-fmp-walker", "key": "data/warm/fmp-walker/manifest.json", "role": "the S&P 500 constituent list", "schedule": "daily"},
    {"engine": "justhodl-polygon-today", "key": TODAY_KEY, "role": "today's forming day bar of every US stock and ETF, 15 minutes delayed - read by the page, not by this engine", "schedule": "every 5 minutes, 09:30-16:55 New York, Monday to Friday"},
]


# --------------------------------------------------------------------------------------------------------------- reads
def missing(exc):
    code = str(((getattr(exc, "response", None) or {}).get("Error") or {}).get("Code") or "")
    return type(exc).__name__ == "NoSuchKey" or code in ("NoSuchKey", "404", "NotFound") or "NoSuchKey" in str(exc)


def optional(s3, key):
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw), None
    except Exception as exc:  # noqa: BLE001
        return None, "%s: %s" % (type(exc).__name__, str(exc)[:120])


def canon(sym):
    return str(sym or "").upper().strip().replace("-", ".")


def bank_key(ticker):
    return polygon_bankfirst.ROOT + "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in str(ticker).upper())[:80] + ".json.gz"


def read_bars(s3, ticker, tail=READ_TAIL, through=None):
    """The last `tail` banked daily bars of one ticker up to and including `through`, oldest first:
    {date, o, h, l, c, v, vw}; [] when unbanked. Thread-safe."""
    try:
        doc = json.loads(gzip.decompress(s3.get_object(Bucket=S3_BUCKET, Key=bank_key(ticker))["Body"].read()))
    except Exception as exc:  # noqa: BLE001
        if missing(exc):
            return []
        raise
    rows = [r for r in (doc or {}).get("rows") or [] if isinstance(r, list) and len(r) >= 6 and r[0] is not None and r[4] is not None]
    out = []
    for r in rows[-(tail + 8):]:
        d = polygon_bankfirst._row_date(r[0]).isoformat()
        if through and d > through:
            continue                                   # a bar after the session the desk stands on is left out, never deleted
        b = {k: v for k, v in zip(("o", "h", "l", "c", "v"), r[1:6]) if v is not None}
        b["date"] = d
        if len(r) > 6 and r[6] is not None:
            b["vw"] = r[6]
        out.append(b)
    return out[-tail:]


def months_between(first_iso, last_iso):
    y, m = int(first_iso[:4]), int(first_iso[5:7])
    ly, lm = int(last_iso[:4]), int(last_iso[5:7])
    out = []
    while (y, m) <= (ly, lm):
        out.append("%04d-%02d" % (y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def split_reference(s3, first_iso, as_of, want, ahead_days=SPLIT_AHEAD_DAYS):
    """Polygon's split reference for the tickers in `want`: (recorded, ahead, windows read).
    recorded = {ticker: [{date, split_from, split_to}]} executed from `first_iso` through `as_of` (the bars are rebased
    by them); ahead = {ticker: {date, from, to}} announced for the sessions after `as_of` - the page divides the last
    close by it on the execution day, so a 10-for-1 split is not read as a 90% fall."""
    horizon = (date.fromisoformat(as_of) + timedelta(days=ahead_days)).isoformat()
    recorded, ahead, n = {}, {}, 0

    def one(month):
        doc, _ = optional(s3, BANK_SPLITS_ROOT + month + ".json.gz")
        return doc if isinstance(doc, list) else (doc.get("results") if isinstance(doc, dict) else None)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for rows in pool.map(one, months_between(first_iso, horizon)):
            if rows is None:
                continue
            n += 1
            for r in rows:
                if not isinstance(r, dict):
                    continue
                t, d = canon(r.get("ticker")), str(r.get("execution_date") or "")[:10]
                if t not in want or not r.get("split_from") or not r.get("split_to"):
                    continue
                if first_iso <= d <= as_of:
                    recorded.setdefault(t, []).append({"date": d, "split_from": r["split_from"], "split_to": r["split_to"]})
                elif as_of < d <= horizon and (t not in ahead or d < ahead[t]["date"]):
                    ahead[t] = {"date": d, "from": r["split_from"], "to": r["split_to"]}
    return recorded, ahead, n


# ------------------------------------------------------------------------------------------------------------ universe
def _num(v):
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v else None


def kind_of(r):
    """'stock' for a listed common stock, 'etf' for a plain ETF, else the reason the symbol is left out. Pure."""
    sym = str(r.get("symbol") or "").upper()
    if not sym:
        return None, "no symbol"
    if r.get("is_otc"):
        return None, "otc"
    if r.get("is_etf"):
        if GEARED_RX.search(str(r.get("name") or "")):
            return None, "geared or inverse fund"
        return "etf", None
    if r.get("is_common") is False or r.get("is_fund") or r.get("is_preferred") or r.get("is_warrant_unit") or r.get("is_shell") or r.get("is_debt_or_structured"):
        return None, "not a common stock"
    if r.get("is_common") is not True or NOT_COMMON_SUFFIX.search(sym):
        return None, "not a common stock"
    return "stock", None


def select_universe(book_rows, min_dv=MIN_DV):
    """({ticker: meta}, stats) from the factor book - the Wyckoff scan's and the volatility desk's rule. Pure."""
    out, stats = {}, {"book_rows": len(book_rows), "stock": 0, "etf": 0, "left_out": {}}
    for r in book_rows:
        kind, why = kind_of(r)
        if kind:
            dv = _num(r.get("dollar_vol_20d"))
            if dv is None or dv < min_dv:
                kind, why = None, "thin (under the dollar-volume floor)"
        if not kind:
            stats["left_out"][why] = stats["left_out"].get(why, 0) + 1
            continue
        t = canon(r.get("symbol"))
        if t in out:
            continue
        stats[kind] += 1
        out[t] = {"ticker": t, "name": r.get("name") or None, "kind": kind, "sector": r.get("sector") or None, "industry": r.get("industry") or None,
                  "mcap": _num(r.get("mcap")), "adv_usd": _num(r.get("dollar_vol_20d")), "ipo_date": str(r.get("ipo_date"))[:10] if r.get("ipo_date") else None}
    return out, stats


# ------------------------------------------------------------------------------------------------------------ measures
def _r(v, d=2):
    return None if v is None or v != v or v in (float("inf"), float("-inf")) else round(float(v), d)


def _px(v):
    return None if v is None else round(float(v), 4 if abs(v) < 10 else 3 if abs(v) < 1000 else 2)


def clean(bars):
    """Bars a measure can read: every price finite and above zero, the high not under the low. Pure."""
    out = []
    for b in bars:
        try:
            o, h, l, c = float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"])
        except (TypeError, ValueError, KeyError):
            continue
        if not (o > 0 and h > 0 and l > 0 and c > 0 and h >= l) or o != o or h != h or l != l or c != c or h == float("inf"):
            continue
        v, vw = b.get("v"), b.get("vw")
        x = {"date": b["date"], "o": o, "h": max(h, o, c), "l": min(l, o, c), "c": c, "v": float(v) if isinstance(v, (int, float)) and v == v and v >= 0 else 0.0}
        if isinstance(vw, (int, float)) and vw == vw and vw > 0:
            x["vw"] = float(vw)
        out.append(x)
    return out


def excess(c, s, n, end=-1):
    """Excess return in pp of the aligned closes `c` over the benchmark's `s` across the n sessions ending at `end`."""
    i = len(c) + end
    j = i - n
    if j < 0 or c[j] <= 0 or s[j] <= 0:
        return None
    return (c[i] / c[j] - 1.0) * 100.0 - (s[i] / s[j] - 1.0) * 100.0


def record(c, s, n, dump_pct, rally_pct, flat_pct):
    """The dump-day / rally-day record over the last n aligned sessions - the strength engine's capture() arithmetic,
    with the counts kept: dn dump days, dh held up on (flat, up, or down less than SPY), ds the streak of the most
    recent dump days held, dx the mean excess on them; rn rally days, rb beat SPY on, rx the mean excess; dnx / upx the
    mean excess on all of SPY's down days / up days. None when the window is not covered. Pure."""
    m = len(c)
    if m < n + 1:
        return None
    dn_r, dn_s, up_r, up_s, dump, rally = [], [], [], [], [], []
    for i in range(m - n, m):
        if c[i - 1] <= 0 or s[i - 1] <= 0:
            continue
        r = (c[i] / c[i - 1] - 1.0) * 100.0
        sp = (s[i] / s[i - 1] - 1.0) * 100.0
        if sp < 0:
            dn_r.append(r)
            dn_s.append(sp)
        elif sp > 0:
            up_r.append(r)
            up_s.append(sp)
        if sp <= dump_pct:
            dump.append((r >= -flat_pct or r > sp, r - sp))
        if sp >= rally_pct:
            rally.append((r > sp, r - sp))
    streak = 0
    for held, _ in reversed(dump):
        if not held:
            break
        streak += 1
    return {"dn": len(dump), "dh": sum(1 for h, _ in dump if h), "ds": streak, "dx": _r(sum(x for _, x in dump) / len(dump)) if dump else None,
            "rn": len(rally), "rb": sum(1 for b, _ in rally if b), "rx": _r(sum(x for _, x in rally) / len(rally)) if rally else None,
            "dnx": _r(sum(dn_r) / len(dn_r) - sum(dn_s) / len(dn_s)) if dn_r else None,
            "upx": _r(sum(up_r) / len(up_r) - sum(up_s) / len(up_s)) if up_r else None}


def missed_sessions(dates, bench_tail):
    """How many of the benchmark's last sessions this symbol has no bar on, from its own first bar on. Pure."""
    have, first = set(dates[-(len(bench_tail) + 30):]), dates[0]
    return sum(1 for d in bench_tail if d >= first and d not in have)


def flagged_jump(jumps):
    hits = [j for j in jumps or [] if j.get("ratio") and j.get("ago", 10 ** 6) <= JUMP_ASIDE and (j["ratio"] >= JUMP_FLAG or j["ratio"] <= 1.0 / JUMP_FLAG)]
    return hits[-1] if hits else None


def measure(meta, bars, bench_close, bench_tail, prior_session, rules, jump=None):
    """One row (a dict over COLS plus the private vs / vs_prev the ranks read) from a name's repaired bars. Pure."""
    n = len(bars)
    last = bars[-1]
    dates = [b["date"] for b in bars]
    c_al, s_al = [], []
    for b in bars:
        sp = bench_close.get(b["date"])
        if sp is not None:
            c_al.append(b["c"])
            s_al.append(sp)
    vs = {k: _r(excess(c_al, s_al, hn)) for k, hn in T.HORIZONS if k in HKEY}
    vs_prev = {k: _r(excess(c_al, s_al, hn, end=-2)) for k, hn in T.HORIZONS if k in HKEY}
    rec = record(c_al, s_al, RECORD_N, rules["dump_pct"], rules["rally_pct"], rules["flat_pct"]) or {}
    rec_prev = record(c_al[:-1], s_al[:-1], RECORD_N, rules["dump_pct"], rules["rally_pct"], rules["flat_pct"]) or {}      # the record a name carried INTO the last session
    prev = bars[-2] if n >= 2 else None
    vols = [b["v"] for b in bars[-21:-1]]
    month_hi, month_lo = max(b["h"] for b in bars[-21:]), min(b["l"] for b in bars[-21:])
    sp_m = (month_hi / month_lo - 1.0) * 100.0 if n >= 21 and month_lo > 0 else None
    miss = missed_sessions(dates, bench_tail) if bench_tail else 0
    closes = [b["c"] for b in bars]
    s50, s200 = T.sma(closes, 50), T.sma(closes, 200)
    why = ("geared" if meta.get("cls") == GEARED_CLS else "thin" if miss > THIN_MISS
           else "pinned" if (meta["kind"] == "stock" and sp_m is not None and sp_m <= PIN_SPREAD_M) else "jump" if jump else None)
    row = {"t": meta["ticker"], "name": meta["name"], "kind": meta["kind"], "cls": meta.get("cls"), "sector": meta["sector"], "industry": meta["industry"],
           "mcap": _r(meta["mcap"], 0), "adv": _r(meta["adv_usd"], 0), "sp": 1 if meta.get("sp500") else 0,
           "pc": _px(prev["c"]) if (prev and prev["date"] == prior_session) else None,
           "o": _px(last["o"]), "h": _px(last["h"]), "l": _px(last["l"]), "c": _px(last["c"]), "v": _r(last["v"], 0), "vw": _px(last.get("vw")),
           "av": _r(sum(vols) / len(vols), 0) if len(vols) >= 5 else None,
           "rs": None, "rsp": None, "atr": _r(T.atr_pct(bars, 14)), "p52": T.context_52w(bars)["pos_52w"],
           "ma": None if s50 is None else (1 if last["c"] > s50 else 0) + (0 if s200 is None else 2 if last["c"] > s200 else 0),
           "n": n, "why": why, "jd": jump["date"] if jump else None}
    for k, col in HKEY.items():
        row[col] = vs.get(k)
    for k in ("dn", "dh", "ds", "dx", "rn", "rb", "rx", "dnx", "upx"):
        row[k] = rec.get(k)
    row["dnp"], row["dhp"] = rec_prev.get("dn"), rec_prev.get("dh")
    ranked = why is None and len(c_al) >= RANK_MIN and vs.get("m") is not None and vs.get("q") is not None
    row["_vs"] = vs if ranked else None
    row["_vs_prev"] = vs_prev if (ranked and vs_prev.get("m") is not None and vs_prev.get("q") is not None) else None
    return row


def one_ticker(s3, meta, recorded, as_of, prior_session, bench_close, bench_tail, listings, first_bar, rules, on_refused=None):
    """(status, row, note): read, repair and measure one symbol."""
    raw = read_bars(s3, meta["ticker"], through=as_of)
    if not raw:
        return "unbanked", None, None
    own, _ = T.own_history(raw, meta.get("ipo_date"))
    refused = []
    bars, ev = T.rebase(own, recorded or [], refused)
    if refused and on_refused:
        on_refused(meta["ticker"], refused)
    if meta["kind"] == "stock" and meta.get("mcap") and meta.get("sector"):
        bars, _ = T.separation_cut(bars, ev, meta["ticker"], meta["mcap"], meta["sector"], listings, first_bar)
    bars = clean(bars)
    if not bars:
        return "unreadable", None, None
    if bars[-1]["date"] != as_of:
        return "not_current", None, bars[-1]["date"]
    if len(bars) < MIN_SESSIONS:
        return "short", None, len(bars)
    return "scanned", measure(meta, bars, bench_close, bench_tail, prior_session, rules, flagged_jump(T.jumps_of(bars, ev))), len(bars)


def rank(rows):
    """`rs` and `rsp`: the desk's composite percentile among the name's kind, now and one session earlier. Mutates."""
    counts = {}
    for kind in ("stock", "etf"):
        grp = [r for r in rows if r["kind"] == kind and r.get("_vs")]
        now = [{"vs": r["_vs"]} for r in grp]
        T.rank_rows(now, key="rs")
        for r, x in zip(grp, now):
            r["rs"] = x.get("rs")
        prev_grp = [r for r in grp if r.get("_vs_prev")]
        prev = [{"vs": r["_vs_prev"]} for r in prev_grp]
        T.rank_rows(prev, key="rs")
        for r, x in zip(prev_grp, prev):
            r["rsp"] = x.get("rs")
        counts[kind] = sum(1 for r in grp if r["rs"] is not None)
    for r in rows:
        r.pop("_vs", None)
        r.pop("_vs_prev", None)
    return counts


# ------------------------------------------------------------------------------------------------- the session's read
def outcome(ret, spy, flat):
    """up / flat / down_less / down_more: the thesis block's own words (bucket_stats). Pure."""
    if ret > flat:
        return "up"
    if ret >= -flat:
        return "flat"
    return "down_less" if ret > spy else "down_more"


def regime_of(spy, rules):
    f = rules["flat_pct"]
    return "dump" if spy <= rules["dump_pct"] else "rally" if spy >= rules["rally_pct"] else "down" if spy < -f else "up" if spy > f else "flat"


def _median(xs):
    xs = sorted(xs)
    n = len(xs)
    return None if not n else (xs[n // 2] if n % 2 else 0.5 * (xs[n // 2 - 1] + xs[n // 2]))


def stats_of(items, spy):
    """items: [(ret, outcome)] -> the counts and shares of one group. Pure."""
    n = len(items)
    out = {"n": n}
    if not n:
        return out
    for k in OUTCOMES:
        out[k] = sum(1 for _, o in items if o == k)
    held = n - out["down_more"]
    rets = [r for r, _ in items]
    out.update(held=held, held_pct=_r(100.0 * held / n, 1), up_pct=_r(100.0 * out["up"] / n, 1), beat=sum(1 for r in rets if r > spy),
               median_ret=_r(_median(rets)), mean_excess=_r(sum(rets) / n - spy))
    out["beat_pct"] = _r(100.0 * out["beat"] / n, 1)
    return out


RECORD_BUCKETS = (("all", "held up on every dump day"), ("most", "on two thirds or more"), ("some", "on a third to two thirds"), ("few", "on fewer than a third"))


def record_bucket(dn, dh):
    """Which of RECORD_BUCKETS a dump-day record falls in; None under RECORD_MIN dump days. Integer arithmetic."""
    if dn is None or dh is None or dn < RECORD_MIN:
        return None
    return "all" if dh == dn else "most" if 3 * dh >= 2 * dn else "some" if 3 * dh >= dn else "few"


def session_read(rows, spy_ret, rules, rank_col="rsp", class_labels=None, rec=("dnp", "dhp")):
    """The read of one session over the table: every counted name's move against SPY's. `rows` are dicts carrying
    `ret` (the session's move in percent, None when it cannot be measured) beside the table's columns; a row that is
    set aside (`why`) or has no `ret` is not counted. `rank_col` and `rec` name the rank and the dump-day record each
    name carried INTO the session: rsp and (dnp, dhp) for the table's own last session, rs and (dn, dh) when the page
    reads today's forming bars. Pure."""
    dn_col, dh_col = rec
    flat = rules["flat_pct"]
    pop = [dict(r, out=outcome(r["ret"], spy_ret, flat), x=r["ret"] - spy_ret) for r in rows if r.get("ret") is not None and not r.get("why")]
    stocks = [r for r in pop if r["kind"] == "stock"]
    etfs = [r for r in pop if r["kind"] == "etf"]
    big = [r for r in stocks if (r.get("mcap") or 0) >= BOARD_MIN_MCAP]

    def st(group):
        return stats_of([(r["ret"], r["out"]) for r in group], spy_ret)
    groups = {"stocks": st(stocks), "stocks_1b": st(big), "stocks_10b": st([r for r in stocks if (r.get("mcap") or 0) >= BIG_MCAP]),
              "sp500": st([r for r in stocks if r.get("sp")]), "etfs": st(etfs), "etfs_liquid": st([r for r in etfs if (r.get("adv") or 0) >= ETF_MIN_ADV])}
    cap = {k: _r(sum((r.get("mcap") or 0) for r in big if r["out"] == k), 0) for k in OUTCOMES}
    by_sector = []
    for sec in sorted({r.get("sector") or "Unclassified" for r in big}):
        g = st([r for r in big if (r.get("sector") or "Unclassified") == sec])
        g["sector"] = sec
        g["mcap"] = _r(sum((r.get("mcap") or 0) for r in big if (r.get("sector") or "Unclassified") == sec), 0)
        by_sector.append(g)
    by_sector.sort(key=lambda g: (-(g.get("held_pct") or 0), g["sector"]))
    by_class = []
    liquid = [r for r in etfs if (r.get("adv") or 0) >= ETF_MIN_ADV]
    for cls in sorted({r.get("cls") or "equity_etf" for r in liquid}):
        g = st([r for r in liquid if (r.get("cls") or "equity_etf") == cls])
        g["cls"] = cls
        g["label"] = (class_labels or {}).get(cls, cls)
        by_class.append(g)
    by_class.sort(key=lambda g: (-(g.get("median_ret") if g.get("median_ret") is not None else -999), g["cls"]))
    industries = []
    for ind in sorted({r.get("industry") for r in big if r.get("industry")}):
        members = [r for r in big if r.get("industry") == ind]
        if len(members) < IND_MIN:
            continue
        g = st(members)
        g["industry"] = ind
        g["sector"] = members[0].get("sector")
        industries.append(g)
    industries.sort(key=lambda g: (-(g.get("held_pct") or 0), -(g.get("median_ret") or 0), g["industry"]))
    ranked = sorted([r for r in big if r.get(rank_col) is not None], key=lambda r: (r[rank_col], r["t"]))
    m = len(ranked)
    deciles = [[] for _ in range(10)]
    for i, r in enumerate(ranked):
        deciles[min(9, int(10 * i / m))].append(r)
    by_rank = [st(d) for d in deciles] if m >= 20 else []

    def names(group, key, n=BOARD_N, reverse=True):
        return [r["t"] for r in sorted(group, key=lambda r: (-(r[key]) if reverse else r[key], r["t"]))[:n]]
    by_record = []
    for key, label in RECORD_BUCKETS:
        g = st([r for r in big if record_bucket(r.get(dn_col), r.get(dh_col)) == key])
        g["key"] = key
        g["label"] = label
        by_record.append(g)
    etf_board = [r for r in liquid if (r.get("cls") or "equity_etf") not in BOARD_SKIP_CLS]
    strong = [r for r in big if (r.get(rank_col) or 0) >= 80]
    weak = [r for r in big if r.get(rank_col) is not None and r[rank_col] <= 20]
    boards = {"up": names([r for r in big if r["out"] == "up"], "ret"),
              "down_less": names([r for r in big if r["out"] == "down_less"], "x"),
              "strong_holding": names([r for r in strong if r["out"] != "down_more"], "x"),
              "strong_breaking": names([r for r in strong if r["out"] == "down_more"], "x", reverse=False),
              "weak_bid": names([r for r in weak if r["out"] == "up"], "ret"),
              "repeat": names([r for r in big if r["out"] != "down_more" and record_bucket(r.get(dn_col), r.get(dh_col)) == "all"], "x"),
              "etf_up": names([r for r in etf_board if r["out"] == "up"], "ret"),
              "etf_down_most": names(etf_board, "ret", reverse=False)}
    return {"n_counted": len(pop), "regime": regime_of(spy_ret, rules), "groups": groups, "mcap_1b": cap, "by_sector": by_sector, "by_class": by_class,
            "industries": {"n": len(industries), "holding": industries[:15], "breaking": industries[-15:][::-1]},
            "by_rank": by_rank, "rank_col": rank_col, "n_ranked_1b": m, "by_record": by_record, "record_cols": [dn_col, dh_col], "boards": boards}


# ------------------------------------------------------------------------------------- the page's live join, stated once
def bar_read(bar, prev, bench=None):
    """What one day bar [o, h, l, c, v, vw] says against the close before it: the move, the move from the open, where the
    last price sits in the day's range, the last price against the day's VWAP - and, given the benchmark's own read,
    the same against it. Pure; the page's JHStrong.barRead is this function."""
    o, h, l, c = bar[0], bar[1], bar[2], bar[3]
    vw = bar[5] if len(bar) > 5 else None
    out = {"ret": (c / prev - 1.0) * 100.0, "fo": (c / o - 1.0) * 100.0 if o else None, "pos": 100.0 * (c - l) / (h - l) if (h is not None and l is not None and h > l) else None,
           "vwx": (c / vw - 1.0) * 100.0 if vw else None}
    if bench:
        out["x"] = out["ret"] - bench["ret"]
        out["fox"] = out["fo"] - bench["fo"] if (out["fo"] is not None and bench.get("fo") is not None) else None
    return out


def live_join(doc, today):
    """The table laid under today's forming bars - the join the page makes in the browser, kept here as its reference
    (the proofs recompute the page's figures with it). Returns None when `today` holds no session after the table's
    own; else {session, as_of, spy: {...}, rows: [dict]} where each row carries `ret` (None when the name has not
    printed today or its move is unconfirmed), `live` (the bar's read) and `unconfirmed`. Pure."""
    session = str((today or {}).get("session") or "")
    as_of = str(doc.get("as_of") or "")
    trows = (today or {}).get("rows") or {}
    bm = doc.get("benchmark") or {}
    sbar = trows.get(bm.get("ticker") or BENCH)
    if not session or not as_of or session <= as_of or not sbar or not bm.get("c"):
        return None
    lo, hi = (doc.get("rules") or {}).get("suspect") or SUSPECT
    ahead = doc.get("splits_ahead") or {}
    spy = bar_read(sbar, bm["c"])
    cols = doc["cols"]
    out = []
    for raw in doc["rows"]:
        r = dict(zip(cols, raw))
        bar = trows.get(r["t"])
        r["ret"], r["live"], r["unconfirmed"], r["split"] = None, None, False, None
        if bar and bar[3] and r.get("c"):
            prev = r["c"]
            sa = ahead.get(r["t"])
            if sa and as_of < sa["date"] <= session and sa.get("to"):
                prev = prev * float(sa["from"]) / float(sa["to"])          # 10-for-1: the last close divided by ten
                r["split"] = sa
            ratio = bar[3] / prev
            if ratio < lo or ratio > hi:
                r["unconfirmed"] = True                                     # no recorded split explains it: shown, not counted
            else:
                r["live"] = bar_read(bar, prev, spy)
                r["ret"] = r["live"]["ret"]
            r["bar"], r["prev"] = bar[:6], prev
        out.append(r)
    return {"session": session, "as_of": as_of, "bars_as_of": today.get("as_of"), "delay_minutes": today.get("delay_minutes"), "spy": dict(spy, bar=sbar[:6], prev=bm["c"]), "rows": out}


def live_read(doc, today, class_labels=None):
    """session_read over the live join: the page's counts for today, by the rank each name carried in (`rs`). None when
    there is no live session."""
    j = live_join(doc, today)
    if not j:
        return None
    rules = doc["rules"]
    read = session_read(j["rows"], j["spy"]["ret"], rules, rank_col="rs", class_labels=class_labels, rec=("dn", "dh"))
    read.update({"date": j["session"], "rank_as_of": j["as_of"], "spy_ret": _r(j["spy"]["ret"], 4), "bars_as_of": j["bars_as_of"],
                 "n_no_print": sum(1 for r in j["rows"] if not r.get("bar")), "n_unconfirmed": sum(1 for r in j["rows"] if r["unconfirmed"]),
                 "unconfirmed": [r["t"] for r in j["rows"] if r["unconfirmed"]][:60]})
    return read


# --------------------------------------------------------------------------------------------------------------- build
def build(s3, now, as_of, version, rules, classify=None, class_labels=None, log=print, remaining_s=600.0):
    """The whole-market table as a document. `as_of` is the session the desk stands on (the main run's own read of the
    bank); `rules` = {flat_pct, dump_pct, rally_pct, weights}: the desk's constants, one source of truth."""
    started = time.time()
    budget = remaining_s - RESERVE_S
    if budget < MIN_BUDGET_S:
        raise RuntimeError("only %.0fs left in the run: the whole-market pass is not started; the previous table stands" % max(0.0, remaining_s))
    deadline = started + budget
    errors = []
    book = bank_enrich.rows(fields=BOOK_FIELDS)
    if not book:
        raise RuntimeError("the factor book is not readable: the universe cannot be drawn")
    universe, ustats = select_universe(book)
    bm_meta = universe.pop(BENCH, None)                 # the benchmark is the yardstick, not a row: against itself it is "down more" on every dump day
    if bm_meta:
        ustats[bm_meta["kind"]] -= 1
        ustats["left_out"]["the benchmark"] = 1
    if len(universe) < MIN_SCANNED:
        raise RuntimeError("only %d symbols from %d factor-book rows: the book is short; the previous publication stands" % (len(universe), len(book)))
    bench = clean(read_bars(s3, BENCH, tail=READ_TAIL + 8, through=as_of))
    if len(bench) < RANK_MIN + 2 or bench[-1]["date"] != as_of:
        raise RuntimeError("SPY's last banked bar is %s, the desk stands on %s: the bank is mid-fold" % (bench[-1]["date"] if bench else None, as_of))
    prior_session = bench[-2]["date"]
    bench_close = {b["date"]: b["c"] for b in bench}
    bench_tail = [b["date"] for b in bench[-THIN_WINDOW:]]
    try:
        sp = fmp_bank.walker("sp500-constituent") or []
        sp500 = {canon(x.get("symbol")) for x in sp if isinstance(x, dict)}
    except Exception as exc:  # noqa: BLE001
        sp500 = set()
        errors.append({"input": "sp500-constituent", "error": "%s: %s" % (type(exc).__name__, str(exc)[:120])})
    for t, meta in universe.items():
        meta["sp500"] = t in sp500
        meta["cls"] = "stock" if meta["kind"] == "stock" else ((classify(t, "fund", meta["name"])[0] if classify else None) or "equity_etf")
    recorded, ahead, n_windows = split_reference(s3, bench[0]["date"], as_of, set(universe) | {BENCH})
    listings = [(m["ipo_date"], t, m["mcap"], m["sector"]) for t, m in universe.items() if m["kind"] == "stock" and m.get("ipo_date") and m.get("mcap")]
    first_bars, refused = {}, {}

    def first_bar(t):
        if t not in first_bars:
            try:
                own, _ = T.own_history(read_bars(s3, t, tail=2000, through=as_of), (universe.get(t) or {}).get("ipo_date"))
                first_bars[t] = own[0]["date"] if own else None
            except Exception:  # noqa: BLE001
                first_bars[t] = None
        return first_bars[t]

    def work(t):
        if time.time() > deadline:
            return t, "timeout", None, None
        try:
            return (t,) + one_ticker(s3, universe[t], recorded.get(t), as_of, prior_session, bench_close, bench_tail, listings, first_bar, rules, refused.__setitem__)
        except Exception as exc:  # noqa: BLE001
            return t, "error", None, "%s: %s" % (type(exc).__name__, str(exc)[:100])

    status, rows, not_current = {}, [], []
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        for t, st, row, note in pool.map(work, sorted(universe)):
            status[st] = status.get(st, 0) + 1
            if st == "scanned":
                rows.append(row)
            elif st == "error":
                errors.append({"ticker": t, "error": note})
            elif st == "not_current":
                not_current.append(t)
    n_scanned = status.get("scanned", 0)
    if status.get("timeout"):
        raise RuntimeError("the read ran out of time with %d of %d symbols unread: a partial table is not published; the previous one stands" % (status["timeout"], len(universe)))
    if n_scanned < MIN_SCANNED:
        raise RuntimeError("only %d of %d symbols measured (%s): the previous publication stands" % (n_scanned, len(universe), json.dumps(status)))
    if status.get("error", 0) > MAX_ERROR_SHARE * len(universe):
        raise RuntimeError("%d symbols failed to measure: %s" % (status["error"], json.dumps(errors[:5])))
    ranked = rank(rows)
    rows.sort(key=lambda r: (r["kind"] != "stock", -(r["rs"] if r["rs"] is not None else -1), r["t"]))
    b_last, b_prev = bench[-1], bench[-2]
    bvols = [b["v"] for b in bench[-21:-1]]
    bm = {"ticker": BENCH, "pc": _px(b_prev["c"]), "o": _px(b_last["o"]), "h": _px(b_last["h"]), "l": _px(b_last["l"]), "c": _px(b_last["c"]), "v": _r(b_last["v"], 0),
          "vw": _px(b_last.get("vw")), "av": _r(sum(bvols) / len(bvols), 0) if bvols else None,
          "h_ret": {k: T.f(T.ret_n([b["c"] for b in bench], hn)) for k, hn in T.HORIZONS}}
    spy_ret = (bm["c"] / bm["pc"] - 1.0) * 100.0          # from the published prices, as every row's move is: the page restates the read to the last digit
    bm["ret"] = _r(spy_ret, 4)
    for r in rows:
        r["ret"] = (r["c"] / r["pc"] - 1.0) * 100.0 if r.get("pc") else None           # from the published (rounded) prices: the page restates the same figure
    read = session_read(rows, spy_ret, rules, rank_col="rsp", class_labels=class_labels, rec=("dnp", "dhp"))
    read.update({"date": as_of, "prior_session": prior_session, "rank_as_of": prior_session, "spy_ret": _r(spy_ret, 4)})
    dump_days, rally_days = [], []
    for i in range(max(1, len(bench) - RECORD_N), len(bench)):
        sr = (bench[i]["c"] / bench[i - 1]["c"] - 1.0) * 100.0
        if sr <= rules["dump_pct"]:
            dump_days.append({"date": bench[i]["date"], "spy_ret": _r(sr)})
        if sr >= rules["rally_pct"]:
            rally_days.append({"date": bench[i]["date"], "spy_ret": _r(sr)})
    aside = {}
    for r in rows:
        if r["why"]:
            aside[r["why"]] = aside.get(r["why"], 0) + 1
    out = {
        "engine": "justhodl-strength", "version": version, "contract": CONTRACT, "generated_at": now.isoformat(timespec="seconds"), "as_of": as_of, "prior_session": prior_session,
        "benchmark": bm,
        "rules": {"flat_pct": rules["flat_pct"], "dump_pct": rules["dump_pct"], "rally_pct": rules["rally_pct"], "weights": rules.get("weights"), "record_sessions": RECORD_N,
                  "rank_min_sessions": RANK_MIN, "board_min_mcap": BOARD_MIN_MCAP, "big_mcap": BIG_MCAP, "etf_min_adv": ETF_MIN_ADV, "industry_min": IND_MIN, "board_n": BOARD_N,
                  "record_min": RECORD_MIN, "record_buckets": [list(x) for x in RECORD_BUCKETS], "board_skip_cls": list(BOARD_SKIP_CLS), "geared_cls": GEARED_CLS,
                  "suspect": list(SUSPECT), "pin_spread_m": PIN_SPREAD_M, "jump_flag": JUMP_FLAG, "jump_sessions": JUMP_ASIDE, "thin_miss": THIN_MISS, "thin_window": THIN_WINDOW,
                  "min_dollar_vol": MIN_DV},
        "universe": {"book_rows": ustats["book_rows"], "eligible": len(universe), "eligible_stock": ustats["stock"], "eligible_etf": ustats["etf"], "left_out": ustats["left_out"],
                     "scanned": n_scanned, "scanned_stock": sum(1 for r in rows if r["kind"] == "stock"), "scanned_etf": sum(1 for r in rows if r["kind"] == "etf"),
                     "ranked": ranked, "set_aside": aside, "short_history": status.get("short", 0), "unbanked": status.get("unbanked", 0), "unreadable": status.get("unreadable", 0),
                     "not_current": len(not_current), "not_current_sample": not_current[:40], "sp500_members": sum(1 for r in rows if r["sp"]), "split_windows_read": n_windows,
                     "splits_refused": {"n": len(refused), "sample": [{"t": t, "date": refused[t][-1]["date"], "ratio": refused[t][-1].get("ratio")} for t in sorted(refused)[:40]]}},
        "dump_days": dump_days, "rally_days": rally_days,
        "splits_ahead": {t: ahead[t] for t in sorted(ahead) if t in universe},
        "last_session": read,
        "cols": COLS, "rows": [[r.get(c) for c in COLS] for r in rows],
        "definitions": definitions(rules), "sources": SOURCES, "n_errors": len(errors), "errors": errors[:40],
    }
    out["run_seconds"] = round(time.time() - started, 1)
    log("strength-wide: %d measured of %d eligible (%s) in %.1fs; ranked %s; SPY %+.2f%% on %s; $1B+ stocks held up %s%%" % (
        n_scanned, len(universe), json.dumps(status), out["run_seconds"], json.dumps(ranked), spy_ret, as_of, read["groups"]["stocks_1b"].get("held_pct")))
    return out


def definitions(rules):
    f, d, r = rules["flat_pct"], rules["dump_pct"], rules["rally_pct"]
    return {
        "universe": "every listed common stock and plain ETF in the factor book that trades $250k a day or more (20-session average); OTC names, funds that are geared or inverse, preferred shares, notes, warrants, units and shells are left out",
        "move": "the session's close over the close before it, in percent; on the page, today's forming bar (15 minutes delayed) over the last close",
        "outcome": "up = more than +%.2f%%; flat = within %.2f%% either way; down less = down more than %.2f%% but less than SPY; down more = down at least as much as SPY. Held up = up, flat or down less" % (f, f, f),
        "regime": "dump = SPY down %.0f%% or more; rally = SPY up %.0f%% or more; down / up = beyond %.2f%%; flat otherwise" % (abs(d), r, f),
        "excess": "x1 / x5 / x21 / x63 / x126 = the name's return minus SPY's over 1 / 5 / 21 / 63 / 126 common sessions, in percentage points",
        "rs": "the desk's composite (cross-sectional z-scores of those excess returns, weighted 10 / 20 / 30 / 25 / 15, each clipped at 3) as a percentile among the name's kind: a stock among all ranked stocks, an ETF among all ranked ETFs. A name needs %d common sessions with SPY" % RANK_MIN,
        "rsp": "the same rank one session earlier - the rank a name carried INTO the last session, so the last session's read is walk-forward",
        "record": "over the last %d sessions: dn = SPY's dump days the name traded, dh = how many it held up on, ds = the streak of the most recent ones held, dx = its mean excess on them; rn / rb / rx = the same for rally days (beat SPY); dnx / upx = mean excess on all of SPY's down days / up days; dnp / dhp = dn / dh one session earlier, the record a name carried INTO the last session. A record is read from %d dump days on: held up on every one, on two thirds or more, on a third to two thirds, on fewer" % (RECORD_N, RECORD_MIN),
        "set_aside": "pinned = a stock whose last 21 sessions fit inside %.1f%% (a pending deal: flat on every dump day by construction); geared = a fund the desk's class map calls leveraged or inverse (an inverse fund rises on every dump day and says nothing about strength); jump = an unexplained close-to-close move beyond %.0f-to-1 inside %d sessions; thin = missed more than %d of the last %d sessions. Such a row is measured, shown, and kept out of the ranks and the counts. Long-volatility funds are counted in their class and kept off the ETF boards" % (PIN_SPREAD_M, JUMP_FLAG, JUMP_ASIDE, THIN_MISS, THIN_WINDOW),
        "splits_ahead": "splits Polygon's reference lists for the sessions after the table's own: on the execution day the page divides the last close by the ratio. A live move beyond x%.2f / x%.1f with no such record is shown as unconfirmed and not counted" % SUSPECT,
        "ma": "1 = the close is above its 50-day average, 2 = above its 200-day average, 3 = above both",
        "live": "the page joins this table with data/warm/polygon-full/today.json.gz (justhodl-polygon-today, every 5 minutes in the session, 15 minutes delayed): today's move = the forming bar's last price over this table's close",
    }
