"""justhodl-stock-volatility - how volatile every listed stock is, and how wide its price spread has been.

Khalid 2026-11-19: "create an engine that would measure volatility. Most and least volatile stocks. Also it would
measure the price spreads for all stocks and rank them based on that. Price spread last day, week, month, quarter".

One row for every listed common stock and every plain ETF in the warehouse, read from the Polygon EOD bank:

    volatility     hv21 / hv63 / hv252 - the sample standard deviation of daily log returns over the last 21, 63 and
                   252 sessions, annualized, in percent; yz21 - the same from the whole bar (overnight gap, open to
                   close, intraday range: Yang-Zhang); atr - the chart's own "ATR %" (average true range of 14
                   sessions over the last close); hvp - where today's hv21 stands among the stock's own last 252
                   readings; hv21p - the reading 21 sessions ago; mx / mxd - the largest one-session move in 63
    price spread   for the last session (d), 5 (w), 21 (m) and 63 (q) sessions: sp - how far the highest high stood
                   above the lowest low, in percent; hi, lo - those two prices; pos - where the last close sits
                   between them (0 at the low, 100 at the high); own - where that spread stands among the stock's own
                   last 252 windows of the same length
    ranks          rk_hv21, rk_hv63, rk_d, rk_w, rk_m, rk_q - 1 is the most volatile / the widest, counted inside
                   the row's own kind (stocks against stocks, ETFs against ETFs)

Four kinds of row are measured but set aside from the rankings (`aside` names the measures, `why` the reason), so a
ranking is of stocks that trade freely and nothing is hidden:
    jump     an unexplained one-session move beyond 3-to-1 inside the last 63 sessions (the longest ranked window)
    print    a high or a low more than 3-to-1 away from every close around it inside the last 63 sessions: one print
             no trade beside it supports (ops 6855: MYND's banked bar of 2026-10-19 opens at 0.0001 between closes of
             0.26 and 0.22, which alone made its quarter spread 649,900%)
    thin     a symbol that missed more than three of the last 63 sessions
    pinned   a stock whose last 21 sessions fit inside 3.5% from low to high - the tape of a company waiting for a
             takeover to close, not of a calm business (ops 6851: the 25 "least volatile" $1B+ stocks were all such)
The boards (most / least volatile, widest / narrowest spread per window, volatility rising / falling) are drawn from
companies worth $1B or more, so a list is not twenty-five shells.

The factor book lists some exchange-traded notes, preferred shares and warrants as common stock (ops 6852: ATLCL
"6.125% Senior Notes due 2026", AGNCO, HBANL ...). They are not stocks, and they would own the quiet end of the table:
other_issues() names them by what their own name says or by NASDAQ's fifth-letter convention, and they are left out.

Reads S3 only - the Polygon EOD bank (data/warm/polygon-full/eod/<T>.json.gz), the factor book's identity and size
columns and Polygon's split reference. No provider call, no invoke, no event. Daily.

What a row is not: realized volatility and a price spread describe what the tape already printed. Neither is a
forecast, and neither is implied volatility - no option price is read here.
"""
from __future__ import annotations

import gzip
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
from botocore.config import Config

import bank_enrich          # aws/shared: the whole-market factor book (identity, market cap, dollar volume) - S3 only
import chain_guard          # the fleet-wide trigger cap: every engine start is admitted, counted and stamped
import polygon_bankfirst    # aws/shared: the Polygon EOD bank's layout (ROOT, MANIFEST, FIELDS)

import leaders_tape as T    # the leaders engine's pure tape module, byte-identical copy: own_history, rebase, separation_cut, atr_pct
import vol_measures as M    # this engine's own arithmetic: realized volatility and the price spread

VERSION = "1.0.1"
CONTRACT = "stock-volatility.v1"
ENGINE = "justhodl-stock-volatility"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("OUT_KEY", "data/stock-volatility.json")
BANK_SPLITS_ROOT = "data/warm/polygon-bank/splits/"
MIN_DV = float(os.environ.get("MIN_DOLLAR_VOL", "250000"))       # 20-session average dollar volume a name must trade to be measured
MIN_SESSIONS = int(os.environ.get("MIN_SESSIONS", "22"))         # 21 returns: the shortest volatility window
MIN_SCANNED = int(os.environ.get("MIN_SCANNED", "2500"))         # fewer than this is a broken read, not a market
BOARD_MIN_MCAP = float(os.environ.get("BOARD_MIN_MCAP", "1000000000"))
BOARD_N = int(os.environ.get("BOARD_N", "25"))
STALE_SESSIONS = 3             # a name whose last bar is older than this many sessions is not trading; it is not measured
THIN_WINDOW, THIN_MISS = 63, 3 # more than THIN_MISS of the last THIN_WINDOW sessions without a bar: the windows span more days than they say
OWN_WINDOW, OWN_MIN = 252, 126 # "own history" is the last 252 readings; fewer than 126 is not a history
TAIL = OWN_WINDOW + 63 + 6     # the bars the rolling series need: 252 readings of the longest (63-session) window
PRIOR = 21                     # the volatility reading this many sessions ago, to say whether it is rising
PIN_SPREAD_M = 3.5             # a stock whose last 21 sessions fit inside this percent, low to high, trades like a pending deal
JUMP_FLAG = 3.0                # a close-to-close move beyond 3-to-1 either way that no split explains is flagged on the row, never hidden
JUMP_SESSIONS = 300
PRINT_FLAG = 3.0               # a high or low this many times away from every close around it is one print, not a range
HISTORY_SESSIONS = 252
THREADS = int(os.environ.get("THREADS", "16"))
RESERVE_S = 45.0
MAX_ERROR_SHARE = 0.05
BOOK_FIELDS = ["mcap", "dollar_vol_20d", "ipo_date", "exchange"]
NOT_COMMON_SUFFIX = re.compile(r"-(R|RI|RT|RW|WS|WT|W|U|UN|CVR)$")
# The campaign scan's universe rule, kept identical on purpose (tests/deployment/test_stock_volatility_engine.py runs
# both on the same rows): a geared or inverse fund's tape is a derivative of another tape.
GEARED_RX = re.compile(r"\b(\d(\.\d+)?x|ultrapro|ultrashort|inverse|leveraged|bull|bear)\b|\bultra\b(?![\s-]*short)"
                       r"|(?<!long/)(?<!long-)(?<!long )\bshort\b(?![\s-]*(term|duration|maturity|dated|income|bond|treasury|treasuries|muni|municipal|high yield|corporate|credit))", re.I)
# What a "common stock" row of the factor book can really be (ops 6852, read-only audit of the book):
DEBT_NAME_RX = re.compile(r"\d(\.\d+)?\s*%|\bnotes?\b|\bdebentures?\b|\bsr\.?\s+nt\b|\b\dm\s+bd\b", re.I)                       # a coupon, "Senior Notes due 2031", "SR NT", "1M BD"
PREF_NAME_RX = re.compile(r"\bpreferred\s+(stock|shares?|units?|securities|l\.?p\.?)|\bdepositary\s+shares?\b.*\bpreferred\b|\bperpetual\b|\bcumulative\b", re.I)
NASDAQ_OTHER_ISSUE = frozenset("GHIJLMNOPZW")   # NASDAQ's fifth letter: G-J a convertible bond, L and Z a miscellaneous issue (baby bonds), M-P a preferred share, W a warrant
OTHER_ISSUE_DV_SHARE = 0.10                    # ... when the line trades under a tenth of the dollar volume of the ticker it extends (GOOGL out-trades GOOG: a share class)
KINDS = ("stock", "etf")
# measure -> the sessions it looks back over. A flagged jump inside the longest of them (JUMP_ASIDE sessions back or
# fewer) sets the whole row aside: one rule, so a row is either ranked on every measure or on none.
JUMP_ASIDE = 62
RANKED = (("hv21", 21, "vol"), ("hv63", 63, "vol"), ("sp_d", 1, "spread"), ("sp_w", 5, "spread"), ("sp_m", 21, "spread"), ("sp_q", 63, "spread"))
RANK_COL = {"hv21": "rk_hv21", "hv63": "rk_hv63", "sp_d": "rk_d", "sp_w": "rk_w", "sp_m": "rk_m", "sp_q": "rk_q"}
COLS = ["t", "name", "kind", "sector", "industry", "mcap", "adv", "px", "chg", "n",
        "hv21", "hv63", "hv252", "yz21", "atr", "hvp", "hv21p", "mx", "mxd",
        "sp_d", "hi_d", "lo_d", "pos_d", "own_d",
        "sp_w", "hi_w", "lo_w", "pos_w", "own_w",
        "sp_m", "hi_m", "lo_m", "pos_m", "own_m",
        "sp_q", "hi_q", "lo_q", "pos_q", "own_q",
        "rk_hv21", "rk_hv63", "rk_d", "rk_w", "rk_m", "rk_q",
        "miss", "jd", "jr", "ja", "pd", "pr", "pa", "aside", "why"]
SIZES = (("mega", 200e9, None, "$200B and over"), ("large", 10e9, 200e9, "$10B to $200B"), ("mid", 2e9, 10e9, "$2B to $10B"), ("small", 300e6, 2e9, "$300M to $2B"),
         ("micro", None, 300e6, "under $300M"))
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=5, read_timeout=30, max_pool_connections=THREADS * 2)

SOURCES = [
    {"engine": "justhodl-polygon-eod", "key": polygon_bankfirst.MANIFEST, "role": "daily bars of every US stock and ETF since 2021-08-26, one file a ticker", "schedule": "daily 03:10 UTC"},
    {"engine": "justhodl-bank-factors", "key": "data/warm/factor-book/matrix/index.json.gz", "role": "what each symbol is (common stock, ETF, OTC ...), its name, sector, industry, market cap and 20-session dollar volume", "schedule": "daily 02:50 UTC"},
    {"engine": "justhodl-polygon-bank", "key": BANK_SPLITS_ROOT, "role": "Polygon's split reference, to rebase the sessions the bank still holds unadjusted", "schedule": "daily 22:41 UTC"},
]

DEFINITIONS = {
    "hv21 / hv63 / hv252": "Realized volatility, close to close: the sample standard deviation of the daily log returns of the last 21, 63 and 252 sessions, multiplied by the square root of 252, in percent a year. 16% a year is about 1% a day.",
    "yz21": "Yang-Zhang volatility over the last 21 sessions, annualized the same way: the variance of the overnight gap, plus the variance of the open-to-close move, plus the intraday range (Rogers-Satchell), weighted as Yang and Zhang (2000) weight them. It reads the whole bar, so a stock that gaps and reverses inside the day is not read as quiet.",
    "atr": "The chart's ATR %: the average true range of the last 14 sessions as a percent of the last close. The true range of a session is the largest of its high minus its low, its high minus the previous close, and the previous close minus its low.",
    "hvp": "Where today's 21-session volatility stands among the stock's own last 252 daily readings of it, 0 to 100: the share of those readings below it plus half the share equal to it. 95 means the stock has rarely been this volatile in a year; 5 that it has rarely been this quiet. Blank under 126 readings.",
    "hv21p": "The stock's 21-session volatility as it stood 21 sessions ago. Today's reading above it: volatility is rising; below it: falling. Change is today's reading minus that one, in points.",
    "mx / mxd": "The largest one-session close-to-close move of the last 63 sessions, with its sign, and the session it printed on.",
    "sp_d / sp_w / sp_m / sp_q": "Price spread: how far the highest high of the window stood above its lowest low, in percent - (high / low - 1) x 100. d is the last session's own bar, w the last 5 sessions, m the last 21, q the last 63.",
    "hi / lo": "The highest high and the lowest low of that window, in dollars, on bars rebased for splits.",
    "pos": "Where the last close sits inside the window's spread: 0 at the low, 100 at the high.",
    "own": "Where the window's spread stands among the stock's own last 252 windows of the same length (one ending on each session), 0 to 100. 95 means an unusually wide spread for this stock; 5 an unusually narrow one. Blank under 126 windows.",
    "rk_*": "Rank inside the row's own kind - stocks against stocks, ETFs against ETFs - 1 is the most volatile (21 and 63 sessions) or the widest spread (day, week, month, quarter). A measure that is set aside has no rank.",
    "aside / why": "A row that is set aside is measured but left out of every ranking, every board and the market's medians; why says which of four reasons applies - jump (a flagged jump inside the last 63 sessions), print (a flagged print inside them), thin, or pinned - and aside names the measures (all six). The figures still print.",
    "pinned": "A stock whose last 21 sessions fit inside 3.5% from the lowest low to the highest high. A freely trading company does not do that (the S&P 500 fund's own month is wider); a company whose takeover price is agreed does. Pinned names are listed on their own and kept out of the rankings and the market's medians.",
    "jd / jr / ja": "A flagged jump: the date, the ratio and the sessions since a one-session close-to-close move beyond 3-to-1 either way, inside the last 300 sessions, that no recorded or inferred split explains. It can be a real collapse or a corporate action the record does not carry.",
    "pd / pr / pa": "A flagged print: the date, the ratio and the sessions since a bar whose high stood more than 3 times above, or whose low more than 3 times below, every close around it - the close before, its own and the close after. No trade beside it supports that price, so it is read as one bad print rather than a range; ratio is that high or low over the nearest of those closes.",
    "miss": "How many of the last 63 sessions the symbol printed no bar on (sessions before its first bar are not counted). More than 3 and the row is thin: its windows cover more calendar days than they say, so it is set aside.",
    "boards": "Each board is the 25 names at one end of one measure, among common stocks worth $1B or more whose measure is not set aside. Rising / falling is the 21-session volatility minus its reading 21 sessions ago.",
    "bands": "Stocks counted by their 21-session volatility: under 15%, 15-25%, 25-40%, 40-60%, 60-100%, 100% and over - with the market value in each band.",
    "universe": "Listed common stocks (no OTC, preferreds, warrants, units, rights, shells) and ETFs that are not geared or inverse, with a 20-session average dollar volume of at least the floor, a bar in the last 3 sessions and at least 22 sessions of their own continuous history. A line the factor book calls common stock is still left out when its own name says it is a note or a preferred share, or when it is a second NASDAQ issue of a listed company (fifth letter G-J, L-P, Z or W, the issuer's name, under a tenth of the dollar volume of the ticker it extends).",
}

LIMITATIONS = [
    "This is realized volatility: what the closes and bars already printed. It is not implied volatility - no option is read - and it is not a forecast.",
    "The windows are sessions, not calendar periods: a week is the last 5 sessions, a month 21, a quarter 63.",
    "Pinned is read from the tape alone (21 sessions inside 3.5%); no deal record is consulted. A deal agreed inside the last month still shows as a jump in its windows, and a cash-and-stock deal can keep floating.",
    "Bars are rebased for splits here (recorded, then inferred) but not for dividends. A large special dividend prints as a move.",
    "A split inferred from the tape alone is accepted only when the session opened at the new price (the gap from the prior close carries at least half of the move). A fall that happened inside the session is kept as a fall, and flagged as a jump when it is beyond 3-to-1.",
    "A collapse is a wide spread: a stock that fell 99% in the quarter prints a spread near 10,000%, because the spread is the high over the low. Reverse splits are rebased, so such a fall is the holder's own.",
    "The bank begins 2021-08-26; a listing younger than a window leaves that window blank.",
]


# ----------------------------------------------------------------------------------------------------------------- s3
def _s3():
    return boto3.client("s3", region_name="us-east-1", config=CFG)


def missing(exc):
    code = str(((getattr(exc, "response", None) or {}).get("Error") or {}).get("Code") or "")
    return type(exc).__name__ in ("NoSuchKey", "KeyError") or code in ("NoSuchKey", "404", "NotFound") or "NoSuchKey" in str(exc)


def get_json(s3, key):
    raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def optional(s3, key):
    try:
        return get_json(s3, key), None
    except Exception as exc:  # noqa: BLE001
        return None, "%s: %s" % (type(exc).__name__, str(exc)[:120])


def put_json(s3, key, doc, cache="max-age=300"):
    body = json.dumps(doc, separators=(",", ":"), default=str).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json", CacheControl=cache)
    return len(body)


def canon(sym):
    return str(sym or "").upper().strip().replace("-", ".")


def bank_key(ticker):
    return polygon_bankfirst.ROOT + "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in str(ticker).upper())[:80] + ".json.gz"


def read_bars(s3, ticker):
    """Every banked daily bar of one ticker, oldest first: {date, o, h, l, c, v}; [] when unbanked. Thread-safe."""
    try:
        doc = json.loads(gzip.decompress(s3.get_object(Bucket=S3_BUCKET, Key=bank_key(ticker))["Body"].read()))
    except Exception as exc:  # noqa: BLE001
        if missing(exc):
            return []
        raise
    out = []
    for r in (doc or {}).get("rows") or []:
        if not isinstance(r, list) or len(r) < 6 or r[0] is None or r[4] is None:
            continue
        d = {k: v for k, v in zip(("o", "h", "l", "c", "v"), r[1:6]) if v is not None}
        d["date"] = polygon_bankfirst._row_date(r[0]).isoformat()
        out.append(d)
    return out


def months_between(first_iso, last_iso):
    y, m = int(first_iso[:4]), int(first_iso[5:7])
    ly, lm = int(last_iso[:4]), int(last_iso[5:7])
    out = []
    while (y, m) <= (ly, lm):
        out.append("%04d-%02d" % (y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def recorded_splits(s3, first_iso, last_iso, want):
    """Polygon's split reference rows for the tickers in `want`: {ticker: [{date, split_from, split_to}]}, windows read."""
    out, n = {}, 0

    def one(month):
        doc, _ = optional(s3, BANK_SPLITS_ROOT + month + ".json.gz")
        return doc if isinstance(doc, list) else (doc.get("results") if isinstance(doc, dict) else None)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for rows in pool.map(one, months_between(first_iso, last_iso)):
            if rows is None:
                continue
            n += 1
            for r in rows:
                if not isinstance(r, dict):
                    continue
                t, d = canon(r.get("ticker")), str(r.get("execution_date") or "")
                if t in want and first_iso <= d <= last_iso and r.get("split_from") and r.get("split_to"):
                    out.setdefault(t, []).append({"date": d, "split_from": r["split_from"], "split_to": r["split_to"]})
    return out, n


# ----------------------------------------------------------------------------------------------------------- universe
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
    """({ticker: meta}, stats) from the factor book. Pure."""
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


def listings_of(universe):
    return [(m["ipo_date"], t, m["mcap"], m["sector"]) for t, m in universe.items() if m["kind"] == "stock" and m.get("ipo_date") and m.get("mcap")]


def other_issues(book_rows):
    """{ticker: reason} for the rows the factor book calls common stock that are another kind of security. Pure.

    By name: a coupon or "Notes" / "Debentures" (a baby bond), "Preferred Shares" / "Depositary Shares ... Preferred".
    By structure: a five-letter NASDAQ ticker that extends a listed ticker of the same issuer name with one of
    NASDAQ's fifth letters for a bond, a note, a preferred share or a warrant, and trades under a tenth of its dollar
    volume. A share class (GOOGL / GOOG, FOXA / FOX, NWSA / NWS) fails one of those tests and stays."""
    stocks = [r for r in book_rows if kind_of(r)[0] == "stock"]
    by_name = {}
    for r in stocks:
        by_name.setdefault(str(r.get("name") or "").strip().lower(), []).append(r)
    out = {}
    for r in stocks:
        sym, name = str(r.get("symbol") or "").upper(), str(r.get("name") or "")
        if DEBT_NAME_RX.search(name):
            out[canon(sym)] = "a note by its own name"
        elif PREF_NAME_RX.search(name):
            out[canon(sym)] = "a preferred share by its own name"
        elif len(sym) == 5 and sym.isalpha() and sym[-1] in NASDAQ_OTHER_ISSUE and "NASDAQ" in str(r.get("exchange") or "").upper() and name.strip():
            dv = _num(r.get("dollar_vol_20d")) or 0.0
            for base in by_name.get(name.strip().lower(), []):
                b = str(base.get("symbol") or "").upper()
                if b != sym and len(b) in (3, 4) and sym.startswith(b) and dv < OTHER_ISSUE_DV_SHARE * (_num(base.get("dollar_vol_20d")) or 0.0):
                    out[canon(sym)] = "a second issue of %s (NASDAQ fifth letter %s)" % (b, sym[-1])
                    break
    return out


# --------------------------------------------------------------------------------------------------------- one ticker
def _r(v, d=2):
    return None if v is None or v != v or v in (float("inf"), float("-inf")) else round(float(v), d)


def _px(v):
    return None if v is None else _r(v, 4 if abs(v) < 10 else 2)


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
        v = b.get("v")
        out.append({"date": b["date"], "o": o, "h": max(h, o, c), "l": min(l, o, c), "c": c, "v": float(v) if isinstance(v, (int, float)) and v == v and v >= 0 else 0.0})
    return out


def flagged_jump(jumps):
    """The most recent unexplained one-session move beyond JUMP_FLAG-to-1 inside JUMP_SESSIONS, or None. Pure."""
    hits = [j for j in jumps or [] if j.get("ratio") and j.get("ago", 10 ** 6) <= JUMP_SESSIONS and (j["ratio"] >= JUMP_FLAG or j["ratio"] <= 1.0 / JUMP_FLAG)]
    return {"date": hits[-1]["date"], "ratio": hits[-1]["ratio"], "ago": hits[-1]["ago"]} if hits else None


def prints_of(bars, sessions=JUMP_SESSIONS):
    """Bars of the last `sessions` whose high stands PRINT_FLAG times above, or whose low PRINT_FLAG times below, every
    close around them (the close before, their own, the close after): [{date, ratio, ago}], ratio above 1 for a high,
    under 1 for a low. The open is not consulted - it can be the bad print itself. Pure."""
    out, n = [], len(bars)
    for i in range(max(1, n - sessions), n):
        b = bars[i]
        near = [bars[i - 1]["c"], b["c"]] + ([bars[i + 1]["c"]] if i + 1 < n else [])
        up, dn = b["h"] / max(near), b["l"] / min(near)
        if up >= PRINT_FLAG:
            out.append({"date": b["date"], "ratio": round(up, 4), "ago": n - 1 - i})
        elif dn <= 1.0 / PRINT_FLAG:
            out.append({"date": b["date"], "ratio": round(dn, 6), "ago": n - 1 - i})
    return out


def flagged_print(prints):
    """The most recent flagged print, or None. Pure."""
    return dict(prints[-1]) if prints else None


def rebase(own, recorded=None):
    """leaders_tape.rebase, also returning the inferred splits it refused: a split inferred from the tape alone must
    be in force at the open (leaders_tape.at_open). Returns (bars, events applied, inferred events refused). Pure."""
    refused = []
    bars, ev = T.rebase(own, recorded or [], refused)
    return bars, ev, refused


def aside_of(jump, miss, pinned=False, bad_print=None):
    """(measures, why): the ranked measures a row is set aside from, in RANKED's order, and the reason. Pure."""
    if miss is not None and miss > THIN_MISS:
        return [m for m, _, _ in RANKED], "thin"
    if pinned:
        return [m for m, _, _ in RANKED], "pinned"
    if jump and jump["ago"] <= JUMP_ASIDE:
        return [m for m, _, _ in RANKED], "jump"
    if bad_print and bad_print["ago"] <= JUMP_ASIDE:
        return [m for m, _, _ in RANKED], "print"
    return [], None


def is_pinned(kind, sp_m):
    """A stock whose last 21 sessions fit inside PIN_SPREAD_M percent: the tape of a pending deal. Funds are never pinned
    (a cash fund is that quiet by design). Pure."""
    return kind == "stock" and sp_m is not None and sp_m <= PIN_SPREAD_M


def missed_sessions(dates, bench_tail):
    """How many of the benchmark's last sessions this symbol has no bar on, from its own first bar on. Pure."""
    have, first = set(dates[-(len(bench_tail) + 30):]), dates[0]
    return sum(1 for d in bench_tail if d >= first and d not in have)


def _own(series):
    """Where the last reading of a rolling series stands among its own last OWN_WINDOW readings (itself included)."""
    vals = [v for v in series[-OWN_WINDOW:] if v is not None]
    return M.pct_rank(series[-1], vals) if (series and series[-1] is not None and len(vals) >= OWN_MIN) else None


def measure(meta, bars, jump=None, bench_tail=None, bad_print=None):
    """(row, tail) for one symbol from its repaired bars: the row's measures, and the last sessions' (date, hv21, sp_d)
    for the market's own history. Pure."""
    dates = [b["date"] for b in bars]
    O, H, L, C = [b["o"] for b in bars], [b["h"] for b in bars], [b["l"] for b in bars], [b["c"] for b in bars]
    n = len(C)
    miss = missed_sessions(dates, bench_tail) if bench_tail else None
    row = {"t": meta["ticker"], "name": meta["name"], "kind": meta["kind"], "sector": meta["sector"], "industry": meta["industry"], "mcap": _r(meta["mcap"], 0),
           "adv": _r(meta["adv_usd"], 0), "px": _px(C[-1]), "chg": _r((C[-1] / C[-2] - 1) * 100) if n >= 2 else None, "n": n}
    for k in M.HV_WINDOWS:
        row["hv%d" % k] = _r(M.hv(C, k))
    row["yz21"] = _r(M.yang_zhang(O, H, L, C, 21))
    row["atr"] = _r(T.atr_pct(bars, 14))
    t0 = max(0, n - TAIL)
    Ct, Ht, Lt, Dt = C[t0:], H[t0:], L[t0:], dates[t0:]
    hv21s = M.hv_series(Ct, 21)
    row["hvp"] = _r(_own(hv21s), 1)
    row["hv21p"] = _r(hv21s[-1 - PRIOR]) if len(hv21s) > PRIOR else None
    mx, mxd = M.max_move(C, dates, 63)
    row["mx"], row["mxd"] = _r(mx), mxd
    spd = None
    for key, k in M.WINDOWS:
        sp = M.spread(H, L, C, k)
        series = M.spread_series(Ht, Lt, k)
        if key == "d":
            spd = series
        row["sp_" + key] = _r(sp["sp"]) if sp else None
        row["hi_" + key] = _px(sp["hi"]) if sp else None
        row["lo_" + key] = _px(sp["lo"]) if sp else None
        row["pos_" + key] = _r(sp["pos"], 1) if sp else None
        row["own_" + key] = _r(_own(series), 1)
    for col in RANK_COL.values():
        row[col] = None
    month = M.spread(H, L, C, 21)
    aside, why = aside_of(jump, miss, is_pinned(meta["kind"], month["sp"] if month else None), bad_print)
    row.update({"miss": miss, "jd": jump["date"] if jump else None, "jr": jump["ratio"] if jump else None, "ja": jump["ago"] if jump else None,
                "pd": bad_print["date"] if bad_print else None, "pr": bad_print["ratio"] if bad_print else None, "pa": bad_print["ago"] if bad_print else None,
                "aside": ",".join(aside) or None, "why": why})
    tail = [(Dt[i], hv21s[i], spd[i]) for i in range(max(0, len(Dt) - HISTORY_SESSIONS), len(Dt))]
    return row, tail


def one_ticker(s3, meta, recorded, bank_last_ord, listings, first_bar, bench_tail, on_refused=None):
    """(status, row, tail, note): read, repair and measure one symbol."""
    raw = read_bars(s3, meta["ticker"])
    if not raw:
        return "unbanked", None, None, None
    own, _ = T.own_history(raw, meta.get("ipo_date"))
    bars, ev, refused = rebase(own, recorded or [])
    if refused and on_refused:
        on_refused(meta["ticker"], refused)
    if meta["kind"] == "stock" and meta.get("mcap") and meta.get("sector"):
        bars, _ = T.separation_cut(bars, ev, meta["ticker"], meta["mcap"], meta["sector"], listings, first_bar)
    bars = clean(bars)
    if not bars:
        return "unreadable", None, None, None
    if bank_last_ord is not None and bank_last_ord.get(bars[-1]["date"], -10 ** 6) < bank_last_ord["__last__"] - STALE_SESSIONS:
        return "stale", None, None, bars[-1]["date"]
    if len(bars) < MIN_SESSIONS:
        return "short", None, None, len(bars)
    row, tail = measure(meta, bars, flagged_jump(T.jumps_of(bars, ev)), bench_tail, flagged_print(prints_of(bars)))
    return "scanned", row, tail, len(bars)


# --------------------------------------------------------------------------------------------------------------- build
def rank(rows):
    """Write the rank columns in place: inside each kind, 1 is the largest reading; a measure set aside has none. Pure."""
    counts = {k: {} for k in KINDS}
    for kind in KINDS:
        sub = [r for r in rows if r["kind"] == kind]
        for m, _, _ in RANKED:
            ranked = [r for r in sub if r.get(m) is not None and m not in (r.get("aside") or "").split(",")]
            ranked.sort(key=lambda r: (-r[m], r["t"]))
            for i, r in enumerate(ranked):
                r[RANK_COL[m]] = i + 1
            counts[kind][m] = len(ranked)
    return counts


def board_set(rows, m):
    """The rows a board of measure m is drawn from: common stocks at or above the board's size floor, ranked for m."""
    return [r for r in rows if r["kind"] == "stock" and (r.get("mcap") or 0) >= BOARD_MIN_MCAP and r.get(RANK_COL[m]) is not None]


def boards_of(rows, n=BOARD_N):
    """{board: [tickers]} - the two ends of every ranked measure, and volatility rising / falling. Pure."""
    out = {}
    names = {"hv21": ("most_volatile", "least_volatile"), "sp_d": ("widest_d", "narrowest_d"), "sp_w": ("widest_w", "narrowest_w"), "sp_m": ("widest_m", "narrowest_m"),
             "sp_q": ("widest_q", "narrowest_q")}
    for m, (top, bottom) in names.items():
        sub = sorted(board_set(rows, m), key=lambda r: r[RANK_COL[m]])
        out[top] = [r["t"] for r in sub[:n]]
        out[bottom] = [r["t"] for r in sub[::-1][:n]]
    moving = [r for r in board_set(rows, "hv21") if r.get("hv21p") is not None and (r.get("ja") is None or r["ja"] > 21 + PRIOR)]
    moving.sort(key=lambda r: (-(r["hv21"] - r["hv21p"]), r["t"]))
    out["vol_rising"] = [r["t"] for r in moving[:n]]
    out["vol_falling"] = [r["t"] for r in moving[::-1][:n]]
    return out


def _med(rows, key, digits=2):
    return _r(M.median([r.get(key) for r in rows]), digits)


MEDIANS = ("hv21", "hv63", "hv252", "yz21", "atr", "hv21p", "sp_d", "sp_w", "sp_m", "sp_q")


def group_stats(rows):
    """Medians of a group of rows, over the readings that are not set aside; how many are expanding. Pure."""
    def clear(m):
        return [r for r in rows if m not in (r.get("aside") or "").split(",")] if m in RANK_COL else [r for r in rows if not r.get("aside")]
    out = {"n": len(rows), "mcap": _r(sum(r.get("mcap") or 0 for r in rows), 0), "med": {m: _med(clear(m), m) for m in MEDIANS}}
    both = [r for r in clear("hv21") if r.get("hv21") is not None and r.get("hv63") is not None]
    out["expanding"] = sum(1 for r in both if r["hv21"] > r["hv63"])
    out["contracting"] = sum(1 for r in both if r["hv21"] < r["hv63"])
    rising = [r for r in clear("hv21") if r.get("hv21") is not None and r.get("hv21p") is not None]
    out["rising"] = sum(1 for r in rising if r["hv21"] > r["hv21p"])
    out["falling"] = sum(1 for r in rising if r["hv21"] < r["hv21p"])
    return out


def bands_of(rows):
    """Stocks counted by the band their hv21 falls in, with the market value in each. Pure."""
    out = [{"band": label, "from": lo, "to": hi, "n": 0, "mcap": 0.0} for lo, hi, label in M.BANDS]
    for r in rows:
        if r["kind"] != "stock" or r.get("rk_hv21") is None:
            continue
        i = M.band_of(r["hv21"])
        out[i]["n"] += 1
        out[i]["mcap"] += r.get("mcap") or 0
    for b in out:
        b["mcap"] = _r(b["mcap"], 0)
    return out


def size_of(mcap):
    if mcap is None:
        return None
    for key, lo, hi, _ in SIZES:
        if (lo is None or mcap >= lo) and (hi is None or mcap < hi):
            return key
    return None


def history_of(tails, bench_dates):
    """The market's own history: for each of the benchmark's last sessions, the median, quartiles and count of the stocks'
    hv21 and the median of their one-session spread. Pure."""
    want = bench_dates[-HISTORY_SESSIONS:]
    hv, sp = {d: [] for d in want}, {d: [] for d in want}
    for tail in tails:
        for d, a, b in tail:
            if d in hv:
                if a is not None:
                    hv[d].append(a)
                if b is not None:
                    sp[d].append(b)
    out = []
    for d in want:
        if not hv[d]:
            continue
        out.append({"date": d, "n": len(hv[d]), "p25": _r(M.quantile(hv[d], 0.25)), "med": _r(M.median(hv[d])), "p75": _r(M.quantile(hv[d], 0.75)), "sp_d": _r(M.median(sp[d]))})
    return out


def changes_since(prev, boards, as_of):
    """Tickers that joined or left the two volatility boards since the previous session's publication. Pure."""
    if not isinstance(prev, dict) or not prev.get("boards"):
        return {"prev_as_of": None}
    if prev.get("as_of") == as_of:
        return prev.get("changes") or {"prev_as_of": None}
    out = {"prev_as_of": prev.get("as_of")}
    for b in ("most_volatile", "least_volatile"):
        before, now = list(prev["boards"].get(b) or []), boards[b]
        out[b] = {"new": [t for t in now if t not in before], "gone": [t for t in before if t not in now]}
    return out


def build(s3, now, log=print, remaining_s=600.0):
    started = time.time()
    deadline = started + max(30.0, remaining_s - RESERVE_S)
    errors = []
    book = bank_enrich.rows(fields=BOOK_FIELDS)
    if not book:
        raise RuntimeError("the factor book is not readable: the universe cannot be drawn")
    universe, ustats = select_universe(book)
    others = {t: why for t, why in other_issues(book).items() if t in universe}
    for t in others:
        ustats[universe.pop(t)["kind"]] -= 1
    if others:
        ustats["left_out"]["a note, preferred share or warrant the book lists as common stock"] = len(others)
    if len(universe) < MIN_SCANNED:
        raise RuntimeError("only %d symbols from %d factor-book rows: the book is short; the previous publication stands" % (len(universe), len(book)))
    man, man_err = optional(s3, polygon_bankfirst.MANIFEST)
    if man_err or not (man or {}).get("last_session"):
        raise RuntimeError("the Polygon EOD bank's manifest is not readable (%s): the last session is unknown" % man_err)
    bank_last, bank_first = man["last_session"], man.get("first_session") or polygon_bankfirst.BANK_FIRST_SESSION
    bench = clean(read_bars(s3, "SPY"))
    if not bench or bench[-1]["date"] != bank_last:
        raise RuntimeError("SPY's last banked bar is %s, the manifest says %s: the bank is mid-fold" % (bench[-1]["date"] if bench else None, bank_last))
    bench_dates = [b["date"] for b in bench]
    order = {d: i for i, d in enumerate(bench_dates)}
    order["__last__"] = len(bench) - 1
    bench_tail = bench_dates[-THIN_WINDOW:]
    recorded, n_windows = recorded_splits(s3, bank_first, bank_last, set(universe))
    listings = listings_of(universe)
    first_bars, refused = {}, {}

    def first_bar(t):
        if t not in first_bars:
            try:
                own, _ = T.own_history(read_bars(s3, t), (universe.get(t) or {}).get("ipo_date"))
                first_bars[t] = own[0]["date"] if own else None
            except Exception:  # noqa: BLE001
                first_bars[t] = None
        return first_bars[t]

    def work(t):
        if time.time() > deadline:
            return t, "timeout", None, None, None
        try:
            return (t,) + one_ticker(s3, universe[t], recorded.get(t), order, listings, first_bar, bench_tail, refused.__setitem__)
        except Exception as exc:  # noqa: BLE001
            return t, "error", None, None, "%s: %s" % (type(exc).__name__, str(exc)[:100])

    status, rows, tails, notes, sessions_read = {}, [], [], {"stale": [], "short": 0, "unbanked": 0, "unreadable": 0}, []
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        for t, st, row, tail, note in pool.map(work, sorted(universe)):
            status[st] = status.get(st, 0) + 1
            if st == "scanned":
                rows.append(row)
                sessions_read.append(note)
                if row["kind"] == "stock" and not row["aside"] and row["ja"] is None and row["pa"] is None:
                    tails.append(tail)
            elif st == "error":
                errors.append({"ticker": t, "error": note})
            elif st == "stale":
                notes["stale"].append(t)
            elif st in notes:
                notes[st] += 1
    n_scanned = status.get("scanned", 0)
    if status.get("timeout"):
        raise RuntimeError("the read ran out of time with %d of %d symbols unread: a partial table is not published; the previous one stands" % (status["timeout"], len(universe)))
    if n_scanned < MIN_SCANNED:
        raise RuntimeError("only %d of %d symbols measured (%s): the previous publication stands" % (n_scanned, len(universe), json.dumps(status)))
    if status.get("error", 0) > MAX_ERROR_SHARE * len(universe):
        raise RuntimeError("%d symbols failed to measure: %s" % (status["error"], json.dumps(errors[:5])))
    ranked = rank(rows)
    stocks, etfs = [r for r in rows if r["kind"] == "stock"], [r for r in rows if r["kind"] == "etf"]
    boards = boards_of(rows)
    by_sector = []
    for sec in sorted({r["sector"] or "Unclassified" for r in stocks}):
        g = group_stats([r for r in stocks if (r["sector"] or "Unclassified") == sec])
        g["sector"] = sec
        by_sector.append(g)
    by_sector.sort(key=lambda g: -(g["med"]["hv21"] or 0))
    by_size = []
    for key, lo, hi, label in SIZES:
        g = group_stats([r for r in stocks if size_of(r.get("mcap")) == key])
        g.update({"size": key, "label": label})
        by_size.append(g)
    bm = next((r for r in rows if r["t"] == "SPY"), None)
    if bm is None:
        bm, _ = measure({"ticker": "SPY", "name": "SPDR S&P 500 ETF Trust", "kind": "etf", "sector": None, "industry": None, "mcap": None, "adv_usd": None}, bench, None, bench_tail)
    prev, _ = optional(s3, OUT_KEY)
    out = {
        "engine": ENGINE, "version": VERSION, "contract": CONTRACT, "generated_at": now.isoformat(timespec="seconds"), "as_of": bank_last,
        "universe": {"book_rows": ustats["book_rows"], "eligible": len(universe), "eligible_stock": ustats["stock"], "eligible_etf": ustats["etf"], "left_out": ustats["left_out"],
                     "scanned": n_scanned, "scanned_stock": len(stocks), "scanned_etf": len(etfs), "short_history": notes["short"], "not_trading": len(notes["stale"]),
                     "unbanked": notes["unbanked"], "unreadable": notes["unreadable"], "min_dollar_vol": MIN_DV, "min_sessions": MIN_SESSIONS, "bank_first_session": bank_first,
                     "bank_sessions": len(bench), "median_sessions_read": sorted(sessions_read)[len(sessions_read) // 2] if sessions_read else None, "split_windows_read": n_windows,
                     "ranked": ranked, "set_aside": {k: sum(1 for r in rows if r["kind"] == k and r["aside"]) for k in KINDS},
                     "flagged_jump": {k: sum(1 for r in rows if r["kind"] == k and r["ja"] is not None) for k in KINDS},
                     "flagged_print": {k: sum(1 for r in rows if r["kind"] == k and r["pa"] is not None) for k in KINDS},
                     "splits_refused": {"n": len(refused), "sample": [{"t": t, "date": refused[t][-1]["date"], "ratio": refused[t][-1].get("ratio")} for t in sorted(refused)[:60]]},
                     "thin": {k: sum(1 for r in rows if r["kind"] == k and r["why"] == "thin") for k in KINDS},
                     "pinned": {k: sum(1 for r in rows if r["kind"] == k and r["why"] == "pinned") for k in KINDS},
                     "reclassified": [{"t": t, "why": others[t]} for t in sorted(others)]},
        "windows": {"spread": {k: v for k, v in M.WINDOWS}, "volatility": list(M.HV_WINDOWS), "own_history": OWN_WINDOW, "prior": PRIOR, "atr": 14, "largest_move": 63, "pin_spread_m": PIN_SPREAD_M,
                    "jump_flag": JUMP_FLAG, "print_flag": PRINT_FLAG, "aside_sessions": JUMP_ASIDE + 1},
        "board_rule": {"min_mcap": BOARD_MIN_MCAP, "n": BOARD_N, "population": {m: len(board_set(rows, m)) for m, _, _ in RANKED}},
        "market": {"stock": group_stats(stocks), "etf": group_stats(etfs), "benchmark": {k: bm.get(k) for k in COLS if not k.startswith("rk_")}},
        "bands": bands_of(rows),
        "by_sector": by_sector, "by_size": by_size,
        "history": history_of(tails, bench_dates),
        "boards": boards,
        "pinned": [r["t"] for r in sorted(stocks, key=lambda r: (-(r.get("mcap") or 0), r["t"])) if r["why"] == "pinned"],
        "cols": COLS, "rows": [[r.get(c) for c in COLS] for r in rows],
        "definitions": DEFINITIONS, "limitations": LIMITATIONS, "sources": SOURCES,
        "n_errors": len(errors), "errors": errors[:40],
    }
    out["changes"] = changes_since(prev, boards, bank_last)
    out["run_seconds"] = round(time.time() - started, 1)
    log("stock-volatility: %d measured of %d eligible (%s) in %.1fs" % (n_scanned, len(universe), json.dumps(status), out["run_seconds"]))
    return out


def lambda_handler(event, context):
    chain_guard.begin(event, context)   # trigger cap: a root run (the schedule) or whatever chain started it
    now = datetime.now(timezone.utc)
    s3 = _s3()
    try:
        remaining = context.get_remaining_time_in_millis() / 1000.0
    except Exception:  # noqa: BLE001
        remaining = 600.0
    out = build(s3, now, remaining_s=remaining)
    trig = event.get("trigger") if isinstance(event, dict) else None
    out["trigger"] = {"kind": trig or "scheduled"}
    n = put_json(s3, OUT_KEY, out)
    med = out["market"]["stock"]["med"]
    print("stock-volatility %s: %d stocks, %d ETFs; median stock hv21 %s%%, median day spread %s%%; %d bytes, %.1fs, errors %d" % (
        out["as_of"], out["universe"]["scanned_stock"], out["universe"]["scanned_etf"], med["hv21"], med["sp_d"], n, out["run_seconds"], out["n_errors"]))
    return {"statusCode": 200, "key": OUT_KEY, "bytes": n, "as_of": out["as_of"], "scanned": out["universe"]["scanned"], "stocks": out["universe"]["scanned_stock"],
            "etfs": out["universe"]["scanned_etf"], "median_hv21": med["hv21"], "run_seconds": out["run_seconds"], "errors": out["n_errors"]}
