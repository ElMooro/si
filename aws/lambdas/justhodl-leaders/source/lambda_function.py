"""justhodl-leaders - the leaders of every industry against the S&P 500: who leads, who is being accumulated or
distributed, what smart money did, where the money goes, and what the tape is printing - daily into data/leaders.json
(Khalid, 2026-11-04: "build a new engine called leaders and track how leaders on each industry are doing compared to
sp500 and whether they are in accumulation or distribution. Is smart money buying or selling them. Buying inflow and
outflows. The industry etfs inflows and outflows, how are they compared to their 250, 200, 100, 50, 20 moving average.
Which are seeing golden cross and death cross. Also which are seeing double tops and double bottoms. Climax, tight
price. I want to be able to sort them ascending and descending.").

A READER. Nothing is fetched from a provider; every figure is derived from dated artifacts on S3 and stamped with
their dates, and a figure that cannot be computed is null, never guessed.

LEADERS   the five largest companies of each FMP industry by market capitalization, from the factor book's
          whole-market identity (US-listed common stocks and ADRs; no ETFs, OTC lines, preferreds, warrants, rights,
          units, shells or notes). Share classes of one company (same CIK) fold into the class that trades the most
          dollars. A leader needs $2B of market cap and $5M of daily dollar volume; an industry with no such company
          keeps its largest one above $300M / $1M. `rank` counts ADRs, `rank_us` does not - the five largest ex-ADR
          names are carried too, so the desk can be read with or without foreign listings.
VS S&P    excess return against SPY on 1 / 5 / 21 / 63 / 126 / 252 sessions from the Polygon EOD bank (600 sessions,
          splits rebased); `strength` = percentile within the leaders of the strength desk's z composite; RS trend,
          RS line at a 52-week high / low.
AVERAGES  the close against its 20 / 50 / 100 / 200 / 250-session simple averages, how many it is above, the stack
          (close > 20 > 50 > 100 > 200 > 250), the 50 / 200 golden or death cross (how long ago, how far since, whether
          one is near), the whipsaw count.
PATTERNS  double tops and double bottoms from swing pivots (forming / confirmed, neckline, measured target, volume on
          the second pivot against the first); Wyckoff buying and selling climaxes on the bottom desk's own gates; the
          strength desk's tight-base (coil) detector.
A / D     tape accumulation or distribution, -100..+100: Chaikin money flow on 21 and 63 sessions, up-volume against
          down-volume on 50, on-balance volume on 63, accumulation minus distribution sessions in the last 25.
MONEY     signed dollar volume in / out / net on 5 / 21 / 63 sessions (a tape estimate, never a reported flow).
SMART     the 13F cohort's read per ticker (holders now / prior, bull / bear manager votes, dollars in / out / net at
          reported prices, stance) and per industry (the 13F book's industry layer); insiders' open-market purchases
          and sales on Form 4 over 90 days from the banked feed. The verdict is those two legs, each shown.
ETFS      the funds of each industry (aws/shared/industry_etf_catalog): reported creations / redemptions on 1 / 5 / 21
          observations in USD and % of assets - the native ETF desk first, the canonical 300-fund packet second, the
          Polygon bank's per-fund ETF Global history third (only a file whose every row names the fund) - and the same
          averages, crosses, patterns, climaxes and bases as the leaders.
"""
from __future__ import annotations

import gzip
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import boto3

import bank_enrich          # aws/shared: the whole-market factor book (identity, market cap, short interest) - S3 only
import chain_guard          # the fleet-wide trigger cap: every engine start is admitted, counted and stamped
import fmp_bank             # aws/shared: the FMP warehouse layout (the insider feed's root)
import industry_etf_catalog  # aws/shared: the reviewed industry -> ETF map
import polygon_bankfirst    # aws/shared: the Polygon EOD bank's layout (ROOT, MANIFEST, FIELDS); rows are read here, thread-safely

import leaders_patterns as P
import leaders_tape as T

VERSION = "1.0.0"
CONTRACT = "leaders.v1"
ENGINE = "justhodl-leaders"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("OUT_KEY", "data/leaders.json")
DESK_KEY = "data/etf-desk-research.json"
CANON_KEY = "data/provider-fund-flow-research.json"
TICKERS_KEY = "data/13f-tickers.json"
BOOK_KEY = "data/13f-book.json"
PHASE_KEY = "data/phase-detector.json"
BOTTOM_KEY = "data/bottom.json"
BANK_FLOW_ROOT = "data/warm/polygon-bank/etf-global-fund-flows/"
BANK_SPLITS_ROOT = "data/warm/polygon-bank/splits/"
BANK_EVENTS_ROOT = "data/warm/polygon-bank/ticker-events/"     # Polygon's ticker-change record, one file per ticker
SYMBOL_CHANGE_KEY = fmp_bank.WALKER_ROOT + "symbol-change/latest.json.gz"   # FMP's last hundred symbol changes
INSIDER_ROOT = fmp_bank.WALKER_ROOT + "insider-trading-latest/"
BENCH = "SPY"
TOP_N = int(os.environ.get("TOP_N", "5"))
MIN_CAP = float(os.environ.get("MIN_CAP_USD", "2000000000"))
MIN_DV = float(os.environ.get("MIN_DOLLAR_VOL", "5000000"))
FLOOR_CAP, FLOOR_DV = 3e8, 1e6                              # the one leader of an industry with no $2B company
MIN_LEADERS = int(os.environ.get("MIN_LEADERS", "100"))     # fewer than this from the factor book is a broken read, not a market
LOOKBACK = int(os.environ.get("LOOKBACK_SESSIONS", "600"))
INSIDER_DAYS = int(os.environ.get("INSIDER_DAYS", "90"))
MIN_SESSIONS = 70
SPARK = 60
THREADS = 12
RESERVE_S = 60.0
FLOW_STALE_DAYS = 7                                        # a Polygon-bank fund history older than this against the last session is not current
BOOK_FIELDS = ["mcap", "dollar_vol_20d", "country", "cik", "short_pct_float", "days_to_cover", "short_volume_ratio", "beta", "ipo_date"]
NOT_COMMON_SUFFIX = re.compile(r"-(R|RI|RT|RW|WS|WT|W|U|UN|CVR)$")      # rights, warrants, units, contingent value rights carried under the issuer's market cap
F13_KEEP = ("n_holders", "n_prior_holders", "holder_delta", "value_usd", "value_prior_usd", "inflow_usd", "outflow_usd", "net_usd", "bull", "bear", "stance", "conc_n_holders", "conc_holder_delta")
F13_IND_KEEP = ("value_usd", "value_prior_usd", "value_change_pct", "n_identities", "n_managers", "tilt_pct", "stance", "inflow_usd", "outflow_usd", "net_usd", "net_pct_of_prior", "money_stance")
SOURCES = [
    {"engine": "justhodl-bank-factors", "key": "data/warm/factor-book/manifest.json", "role": "whole-market identity: industry, market cap, CIK, short interest", "schedule": "daily 02:50 UTC", "expected_h": 24},
    {"engine": "justhodl-polygon-eod", "key": "data/warm/polygon-full/eod/manifest.json", "role": "daily bars (grouped daily folded per ticker)", "schedule": "daily 03:10 UTC", "expected_h": 24},
    {"engine": "justhodl-13f-book", "key": TICKERS_KEY, "role": "13F cohort read per ticker", "schedule": "daily 16:37 UTC + hand-off", "expected_h": 24},
    {"engine": "justhodl-13f-book", "key": BOOK_KEY, "role": "13F cohort money by industry", "schedule": "daily 16:37 UTC + hand-off", "expected_h": 24},
    {"engine": "justhodl-etf-global-desk", "key": DESK_KEY, "role": "native ETF desk: reported fund flows (116 funds)", "schedule": "daily 23:05 UTC", "expected_h": 24},
    {"engine": "justhodl-etf-fund-flows", "key": CANON_KEY, "role": "canonical 300-fund flow packet (ETF Global via Massive/Polygon)", "schedule": "daily 22:00 UTC", "expected_h": 24},
    {"engine": "justhodl-polygon-bank", "key": "data/warm/polygon-bank/manifest.json", "role": "per-fund ETF Global flow histories, the split reference file, ticker-change events", "schedule": "daily 22:41 UTC", "expected_h": 24},
    {"engine": "justhodl-fmp-walker", "key": "data/warm/fmp/manifest.json", "role": "insider (Form 4) feed, one file per filing day; the symbol-change list", "schedule": "daily 23:27 UTC", "expected_h": 24},
    {"engine": "justhodl-phase-detector", "key": PHASE_KEY, "role": "Wyckoff phase per ticker (corroboration)", "schedule": "daily 23:42 UTC", "expected_h": 24},
    {"engine": "justhodl-bottom", "key": BOTTOM_KEY, "role": "Wyckoff bottom desk states (corroboration)", "schedule": "TUE-SAT 03:45 UTC", "expected_h": 72},
]
f = T.f


# ------------------------------------------------------------------------------------------------------------ universe
def canon(sym):
    """FMP spells share classes BRK-B; the bank and the 13F master say BRK.B."""
    return str(sym or "").upper().strip().replace("-", ".")


def spellings(t):
    """The ways one ticker is written across the artifacts: BRK.B / BRK-B / BRK/B / BRKB."""
    t = str(t or "").upper()
    out = [t]
    for alt in (t.replace(".", "-"), t.replace(".", "/"), t.replace("-", "."), t.replace(".", "")):
        if alt not in out:
            out.append(alt)
    return out


def is_common(r):
    """The factor book's common-stock verdict (bank_enrich.screen's rule) plus a suffix guard for rights / warrants /
    units / CVRs that FMP lists under the issuer's market cap."""
    if r.get("is_common") is False or r.get("is_etf") or r.get("is_otc") or r.get("is_fund") or r.get("is_preferred") or r.get("is_warrant_unit") or r.get("is_shell") or r.get("is_debt_or_structured"):
        return False
    return not NOT_COMMON_SUFFIX.search(str(r.get("symbol") or "").upper())


def _num(v):
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v else None


def select_leaders(book_rows, top_n=TOP_N, min_cap=MIN_CAP, min_dv=MIN_DV):
    """The leaders universe from the factor book's rows. Returns (leaders, industries, stats).

    leaders     [{ticker, symbol, name, sector, industry, country, adr, mcap, dv20, rank, rank_us, classes, small, ...}]
    industries  {industry: {sector, n_members, mcap_usd, n_eligible}}
    Pure."""
    cand = []
    stats = {"rows": len(book_rows), "common": 0, "no_cap": 0, "no_industry": 0, "no_tape": 0, "folded": 0, "suffix": 0}
    for r in book_rows:
        sym = str(r.get("symbol") or "").upper()
        if not sym:
            continue
        if NOT_COMMON_SUFFIX.search(sym) and r.get("is_common") is not False:
            stats["suffix"] += 1
        if not is_common(r):
            continue
        stats["common"] += 1
        mcap, dv = _num(r.get("mcap")), _num(r.get("dollar_vol_20d"))
        ind = str(r.get("industry") or "").strip()
        if not mcap or mcap <= 0:
            stats["no_cap"] += 1
            continue
        if not ind or ind == "Shell Companies":
            stats["no_industry"] += 1
            continue
        if not dv or dv <= 0:
            stats["no_tape"] += 1
            continue
        cand.append({"symbol": sym, "ticker": canon(sym), "name": r.get("name"), "sector": r.get("sector") or None, "industry": ind, "country": r.get("country") or None,
                     "adr": bool(r.get("is_adr")), "mcap": mcap, "dv20": dv, "cik": (str(r.get("cik")).lstrip("0") or None) if r.get("cik") not in (None, "") else None,
                     "short_pct_float": _num(r.get("short_pct_float")), "days_to_cover": _num(r.get("days_to_cover")), "short_volume_ratio": _num(r.get("short_volume_ratio")), "beta": _num(r.get("beta")),
                     "ipo_date": str(r["ipo_date"])[:10] if r.get("ipo_date") else None})
    # one company, one line: the share class that trades the most dollars
    by_cik = {}
    for x in cand:
        if x["cik"]:
            by_cik.setdefault(x["cik"], []).append(x)
    drop = set()
    for group in by_cik.values():
        if len(group) > 1:
            keep = max(group, key=lambda x: (x["dv20"], x["mcap"]))
            keep["classes"] = sorted(x["ticker"] for x in group if x is not keep)
            keep["mcap"] = max(x["mcap"] for x in group)
            for x in group:
                if x is not keep:
                    drop.add(id(x))
    stats["folded"] = len(drop)
    cand = [x for x in cand if id(x) not in drop]
    by_ind = {}
    for x in cand:
        by_ind.setdefault(x["industry"], []).append(x)
    leaders, industries = [], {}
    for ind, members in by_ind.items():
        members.sort(key=lambda x: (-x["mcap"], x["ticker"]))
        sectors = {}
        for x in members:
            sectors[x["sector"]] = sectors.get(x["sector"], 0.0) + x["mcap"]
        sector = max(sectors, key=sectors.get) if sectors else None
        total = sum(x["mcap"] for x in members)
        elig = [x for x in members if x["mcap"] >= min_cap and x["dv20"] >= min_dv]
        small = False
        if not elig:
            first = [x for x in members if x["mcap"] >= FLOOR_CAP and x["dv20"] >= FLOOR_DV][:1]
            elig, small = first, bool(first)
        rank_us = 0
        picked = []
        for i, x in enumerate(elig):
            x["rank"] = i + 1
            if not x["adr"]:
                rank_us += 1
                x["rank_us"] = rank_us
            else:
                x["rank_us"] = None
            if x["rank"] <= top_n or (x["rank_us"] is not None and x["rank_us"] <= top_n):
                x["small"] = small
                x["mcap_share_pct"] = f(100.0 * x["mcap"] / total, 1) if total else None
                x.setdefault("classes", [])
                picked.append(x)
        industries[ind] = {"sector": sector, "n_members": len(members), "mcap_usd": f(total, 0), "n_eligible": len(elig), "small": small}
        leaders.extend(picked)
    stats["candidates"] = len(cand)
    stats["industries"] = len(industries)
    stats["industries_with_leader"] = sum(1 for ind in industries if any(x["industry"] == ind for x in leaders))
    stats["leaders"] = len(leaders)
    return leaders, industries, stats


# ----------------------------------------------------------------------------------------------------------------- S3
def _s3():
    return boto3.client("s3", region_name="us-east-1")


def missing(exc):
    code = str(((getattr(exc, "response", None) or {}).get("Error") or {}).get("Code") or "")
    return type(exc).__name__ in ("NoSuchKey", "KeyError") or code in ("NoSuchKey", "404", "NotFound") or "NoSuchKey" in str(exc)


def get_json(s3, key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    raw = obj["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw), obj.get("LastModified")


def optional(s3, key):
    try:
        doc, mod = get_json(s3, key)
        return doc, (mod.isoformat(timespec="seconds") if mod else None), None
    except Exception as exc:  # noqa: BLE001
        return None, None, "%s: %s" % (type(exc).__name__, str(exc)[:120])


def head(s3, key, now, expected_h):
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=key)
        age = (now - h["LastModified"]).total_seconds() / 3600
        return {"key": key, "bytes": h["ContentLength"], "last_modified": h["LastModified"].isoformat(timespec="seconds"), "age_h": round(age, 1),
                "tier": "fresh" if age <= max(30, expected_h * 1.25) else "stale" if age <= expected_h * 7 else "dead"}
    except Exception as exc:  # noqa: BLE001
        return {"key": key, "bytes": None, "last_modified": None, "age_h": None, "tier": "missing", "error": type(exc).__name__}


def bank_key(ticker):
    return polygon_bankfirst.ROOT + "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in str(ticker).upper())[:80] + ".json.gz"


def read_bars(s3, ticker, sessions=LOOKBACK):
    """The last `sessions` daily bars of one ticker from the Polygon EOD bank, oldest first: {date, o, h, l, c, v};
    [] when unbanked. Thread-safe (no shared cache)."""
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=bank_key(ticker))["Body"].read()
        doc = json.loads(gzip.decompress(raw))
    except Exception as exc:  # noqa: BLE001
        if missing(exc):
            return []
        raise
    out = []
    for r in [r for r in (doc or {}).get("rows") or [] if isinstance(r, list) and len(r) >= 5 and r[0] is not None][-sessions:]:
        d = {k: v for k, v in zip(polygon_bankfirst.FIELDS, r) if v is not None}
        if d.get("c") is None:
            continue
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
    """Polygon's split reference rows (the bank's monthly windows) for the tickers in `want` between two dates:
    {ticker: [{date, split_from, split_to}]}, plus how many windows were read."""
    out, n = {}, 0

    def one(month):
        try:
            doc, _ = get_json(s3, BANK_SPLITS_ROOT + month + ".json.gz")
        except Exception:  # noqa: BLE001
            return None
        return doc if isinstance(doc, list) else (doc.get("results") if isinstance(doc, dict) else None)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for rows in pool.map(one, months_between(first_iso, last_iso)):
            if rows is None:
                continue
            n += 1
            for r in rows:
                t = canon(r.get("ticker")) if isinstance(r, dict) else None
                d = str(r.get("execution_date") or "") if isinstance(r, dict) else ""
                if t in want and first_iso <= d <= last_iso and r.get("split_from") and r.get("split_to"):
                    out.setdefault(t, []).append({"date": d, "split_from": r["split_from"], "split_to": r["split_to"]})
    return out, n


# -------------------------------------------------------------------------------------------------------- continuity
def listings_of(book_rows):
    """[(listing date, ticker, market cap, sector)] of the common stocks in the factor book - what a separation is
    weighed against. Pure."""
    out = []
    for r in book_rows:
        d, m = r.get("ipo_date"), _num(r.get("mcap"))
        if d and m and m > 0 and len(str(d)) >= 10 and is_common(r):
            out.append((str(d)[:10], canon(r.get("symbol")), m, r.get("sector") or None))
    return out


def continuous(s3, ticker, bars, ipo_date, changes, deadline=None):
    """One ticker's banked bars cut down to its own continuous history, with the previous symbol's bars put back in
    front when the record of the change and the tape agree (leaders_tape: own_history / predecessor / stitch).
    Returns (bars, cut, stitched, refused, old tickers)."""
    own, cut = T.own_history(bars, ipo_date)
    stitched, refused, olds = [], None, []
    if not own or len(own) >= LOOKBACK:
        return own, cut, stitched, refused, olds
    events, _, _ = optional(s3, BANK_EVENTS_ROOT + bank_key(ticker).rsplit("/", 1)[-1])
    events = T.ticker_events(events)
    cur, cur_cut = ticker, cut
    for _ in range(T.STITCH_DEPTH):
        if len(own) >= LOOKBACK or (deadline and time.time() > deadline):
            break
        pred = T.predecessor(cur, own[0]["date"], cur_cut, events, changes)
        if not pred:
            break
        old, source = pred
        try:
            old_bars = read_bars(s3, old, 10 ** 6)
        except Exception as exc:  # noqa: BLE001
            refused = "%s: %s" % (old, type(exc).__name__)
            break
        own, info, why = T.stitch(own, old_bars, old, source)
        if not info:
            refused = "%s: %s" % (old, why)
            break
        stitched.append(info)
        olds.append(old)
        cur, cur_cut = old, None
    return own[-LOOKBACK:], cut, stitched, refused, olds


# --------------------------------------------------------------------------------------------------------- fund flows
def _window(w):
    if not w or w.get("status") != "matched_reporting_window" or w.get("flow_usd_decimal") is None:
        return None
    try:
        usd = float(w["flow_usd_decimal"])
    except (TypeError, ValueError):
        return None
    pct = w.get("flow_to_end_assets_pct_decimal")
    return {"usd": f(usd, 0), "pct_aum": f(float(pct), 3) if pct not in (None, "") else None, "end": w.get("end_date")}


def packet_flow(fl, eligible, now_iso, source):
    """One fund's flow read from a desk / canonical measurement block: the fund's own latest 1 / 5 / 21 windows while
    its source is current. Pure."""
    valid = str(fl.get("source_valid_until") or "")
    current = bool(eligible) and valid > now_iso
    own = fl.get("latest_windows") or {}
    latest = fl.get("latest_observation") or {}
    try:
        aum = float(latest.get("reported_assets_usd_decimal")) if latest.get("reported_assets_usd_decimal") not in (None, "") else None
    except (TypeError, ValueError):
        aum = None
    return {"source": source, "current": current, "effective": fl.get("latest_effective_date"), "valid_until": valid or None, "aum_usd": f(aum, 0),
            "w1": _window(own.get("1")) if current else None, "w5": _window(own.get("5")) if current else None, "w21": _window(own.get("21")) if current else None, "w63": None}


def bank_flow(rows, ticker, last_session):
    """One fund's flow read from the Polygon bank's ETF Global history (rows {composite_ticker, effective_date,
    processed_date, fund_flow, nav, shares_outstanding}). The file is used only when EVERY row names this fund - the
    provider answers an unknown filter with the whole dataset, and a file of other funds' rows is not this fund's
    history. Windows are the fund's own last 1 / 5 / 21 / 63 reporting observations; % of assets uses NAV x shares on
    the last observation. Returns None when the file is not this fund's or holds no usable row. Pure."""
    if not isinstance(rows, list) or not rows:
        return None
    by_day = {}
    for r in rows:
        if not isinstance(r, dict) or str(r.get("composite_ticker") or "").upper() != str(ticker).upper():
            return None
        d = str(r.get("effective_date") or "")
        if len(d) != 10 or r.get("fund_flow") is None:
            continue
        cur = by_day.get(d)
        if cur is None or str(r.get("processed_date") or "") >= str(cur.get("processed_date") or ""):
            by_day[d] = r
    days = sorted(by_day)
    if not days:
        return None
    last = by_day[days[-1]]
    try:
        aum = float(last["nav"]) * float(last["shares_outstanding"]) if last.get("nav") is not None and last.get("shares_outstanding") is not None else None
    except (TypeError, ValueError):
        aum = None

    def window(n):
        if len(days) < n:
            return None
        try:
            usd = sum(float(by_day[d]["fund_flow"]) for d in days[-n:])
        except (TypeError, ValueError):
            return None
        return {"usd": f(usd, 0), "pct_aum": f(100.0 * usd / aum, 3) if aum and aum > 0 else None, "end": days[-1], "start": days[-n]}
    try:
        age = (date.fromisoformat(last_session) - date.fromisoformat(days[-1])).days if last_session else None
    except ValueError:
        age = None
    current = age is not None and age <= FLOW_STALE_DAYS
    return {"source": "polygon-bank", "current": current, "effective": days[-1], "valid_until": None, "aum_usd": f(aum, 0), "age_days": age, "observations": len(days),
            "w1": window(1) if current else None, "w5": window(5) if current else None, "w21": window(21) if current else None, "w63": window(63) if current else None}


# ----------------------------------------------------------------------------------------------------------- insiders
def insider_days(end, days):
    return [(end - timedelta(days=i)).isoformat() for i in range(days)]


def insider_row(r):
    """A Form 4 open-market purchase or sale as (symbol, side, usd, who, transaction date), or None for every other
    code (awards, option exercises, tax withholding, gifts) and for amendments."""
    if not isinstance(r, dict) or str(r.get("formType") or "") != "4":
        return None
    tt = str(r.get("transactionType") or "")
    side = "buy" if tt.startswith("P-") and str(r.get("acquisitionOrDisposition") or "") == "A" else "sell" if tt.startswith("S-") and str(r.get("acquisitionOrDisposition") or "") == "D" else None
    if side is None:
        return None
    try:
        qty, px = float(r.get("securitiesTransacted") or 0), float(r.get("price") or 0)
    except (TypeError, ValueError):
        return None
    if qty <= 0 or px <= 0:
        return None
    return canon(r.get("symbol")), side, qty * px, str(r.get("reportingCik") or r.get("reportingName") or ""), str(r.get("transactionDate") or r.get("filingDate") or "")[:10], qty, px


def fold_insiders(day_rows, want, since_iso):
    """{ticker: {buys, sells, buyers, sellers, buy_usd, sell_usd, net_usd, last}} from the filtered feed rows.
    One transaction counts once (the feed repeats a filing across days and pages). Pure."""
    seen, out = set(), {}
    for rows in day_rows:
        for r in rows or []:
            x = insider_row(r)
            if x is None:
                continue
            t, side, usd, who, when, qty, px = x
            if t not in want or when < since_iso:
                continue
            k = (t, side, who, when, round(qty, 2), round(px, 4))
            if k in seen:
                continue
            seen.add(k)
            o = out.setdefault(t, {"buys": 0, "sells": 0, "buy_usd": 0.0, "sell_usd": 0.0, "_b": set(), "_s": set(), "last": ""})
            if side == "buy":
                o["buys"] += 1
                o["buy_usd"] += usd
                o["_b"].add(who)
            else:
                o["sells"] += 1
                o["sell_usd"] += usd
                o["_s"].add(who)
            o["last"] = max(o["last"], when)
    for o in out.values():
        o["buyers"], o["sellers"] = len(o.pop("_b")), len(o.pop("_s"))
        o["net_usd"] = f(o["buy_usd"] - o["sell_usd"], 0)
        o["buy_usd"], o["sell_usd"] = f(o["buy_usd"], 0), f(o["sell_usd"], 0)
    return out


def read_insiders(s3, want, end, days=INSIDER_DAYS, deadline=None):
    """The banked Form 4 feed for the last `days` filing days, filtered to the wanted tickers as it is read."""
    spelled = {canon(t) for t in want}

    def one(day):
        if deadline and time.time() > deadline:
            return day, None, "skipped"
        try:
            rows, _ = get_json(s3, "%s%s/%s.json.gz" % (INSIDER_ROOT, day[:4], day))
        except Exception as exc:  # noqa: BLE001
            return day, None, None if missing(exc) else type(exc).__name__
        keep = [r for r in rows if isinstance(r, dict) and canon(r.get("symbol")) in spelled and str(r.get("transactionType") or "")[:2] in ("P-", "S-")] if isinstance(rows, list) else []
        return day, keep, None
    got, read, errs = [], [], []
    with ThreadPoolExecutor(max_workers=6) as pool:
        for day, rows, err in pool.map(one, insider_days(end, days)):
            if rows is not None:
                got.append(rows)
                read.append(day)
            elif err:
                errs.append({"day": day, "error": err})
    since = (end - timedelta(days=days)).isoformat()
    return fold_insiders(got, spelled, since), {"window_days": days, "days_read": len(read), "newest_day": max(read) if read else None, "oldest_day": min(read) if read else None, "errors": errs[:5]}


# ------------------------------------------------------------------------------------------------------------ the rows
def tape_row(bars, bench, bank_last, kind="stock"):
    """Everything the tape says about one instrument: returns and excess vs SPY, RS, 52-week context, the averages and
    the 50 / 200 cross, double tops / bottoms, climaxes, the tight base, accumulation / distribution, signed dollar
    volume. Pure."""
    c = [float(b["c"]) for b in bars]
    row = {"px": f(c[-1]) if c else None, "as_of": bars[-1]["date"] if bars else None, "sessions": len(bars), "stale": bool(bars and bank_last and bars[-1]["date"] < bank_last),
           "h": T.horizons(c) if len(c) >= 2 else {k: None for k, _ in T.HORIZONS}, "vs": None, "composite_pp": None, "z": None, "strength": None,
           "ma": T.ma_block(c), "cross": T.cross_50_200(c, [b["date"] for b in bars]), "dtop": None, "dbot": None, "climax": None, "tight": None,
           "ad": T.ad_block(bars), "mf": T.money_flows(bars), "volx": T.volume_now(bars), "spark": [f(x, 4) for x in c[-SPARK:]], "rs_spark": None}
    row.update({k: None for k in ("rs_trend_63", "rs_vs_52w", "rs_52w_pos", "rs_new_high", "rs_new_low")})
    row.update(T.context_52w(bars))
    pat = P.double_patterns(bars)
    row["dtop"], row["dbot"], row["theta_pct"] = pat["double_top"], pat["double_bottom"], pat["theta_pct"]
    row["climax"] = P.climax(bars, kind)
    if len(bars) < MIN_SESSIONS or not bench:
        return row
    _, c_al, s_al = T.align(bars, bench)
    if len(c_al) < MIN_SESSIONS:
        return row
    sh, hh = T.horizons(s_al), T.horizons(c_al)
    row["vs"] = {k: f(hh[k] - sh[k]) if (hh.get(k) is not None and sh.get(k) is not None) else None for k, _ in T.HORIZONS}
    row["composite_pp"] = T.composite(row["vs"])
    row.update(T.rs_metrics(c_al, s_al))
    row["tight"] = T.pin_flag(T.coil(bars, c_al, s_al, row["rs_trend_63"]), kind == "fund")
    n = min(SPARK, len(c_al))
    if n >= 2 and s_al[-n] > 0 and c_al[-n] > 0:
        base = c_al[-n] / s_al[-n]
        row["rs_spark"] = [f(100.0 * (c_al[i] / s_al[i]) / base, 2) for i in range(len(c_al) - n, len(c_al))]
    return row


def signals_of(row):
    """The short list of what this row is printing now - the vocabulary of the boards and the page's filters. Pure."""
    out = []
    cr = row.get("cross") or {}
    if cr.get("type") and cr.get("fresh"):
        out.append("GOLDEN CROSS" if cr["type"] == "GOLDEN" else "DEATH CROSS")
    if cr.get("near"):
        out.append(cr["near"])
    for k in ("dtop", "dbot"):
        p = row.get(k)
        if p and p["state"] in ("FORMING", "CONFIRMED"):         # a FAILED pattern stays on the row and off the signals
            out.append("%s %s" % (p["kind"], p["state"]))
    cl = row.get("climax")
    if cl:
        out.append("%s CLIMAX" % cl["type"])
    tg = row.get("tight") or {}
    if tg.get("tier") in ("VERY TIGHT", "TIGHT") and not tg.get("flag"):
        out.append(tg["tier"])
    ad = (row.get("ad") or {}).get("label")
    if ad and ad != "NEUTRAL":
        out.append(ad)
    st = (row.get("ma") or {}).get("stack")
    if st in ("BULL", "BEAR"):
        out.append("%s STACK" % st)
    if row.get("rs_new_high"):
        out.append("RS HIGH")
    if row.get("rs_new_low"):
        out.append("RS LOW")
    sm = (row.get("smart") or {}).get("verdict")
    if sm in ("BUYING", "SELLING"):
        out.append("SMART %s" % sm)
    return out


SMART_LEAN_SHARE = 0.10        # a contested 13F read leans only when the cohort's net dollars are at least 10% of the larger reported value
                               # (ops 6744: the median contested name moves 15% - a 1% bar leaned on 95% of them)


def smart_verdict(f13, insider):
    """Is smart money buying or selling: the 13F cohort's stance (two dated disclosures) and insiders' open-market
    trades over the feed window. score = 13F leg (+2 ACCUMULATING, -2 DISTRIBUTING, +-1 when contested but net dollars
    are at least 10% of the larger of the two reported values) + insider leg (+1 on purchases of at least $100K that
    exceed sales; -1 on sales of at least $10M by three or more insiders with no purchase). BUYING >= 2, SELLING <= -2,
    LEANING at +-1, QUIET at 0 with a leg present, NO READ with none. Pure."""
    legs, score, have = {}, 0, False
    if f13:
        have = True
        st, net, val = f13.get("stance"), f13.get("net_usd"), max(f13.get("value_usd") or 0, f13.get("value_prior_usd") or 0)
        leg = 2 if st == "ACCUMULATING" else -2 if st == "DISTRIBUTING" else 0
        if leg == 0 and net is not None and val and abs(net) >= SMART_LEAN_SHARE * val:
            leg = 1 if net > 0 else -1
        legs["f13"] = leg
        score += leg
    if insider:
        have = True
        b, s = insider.get("buy_usd") or 0, insider.get("sell_usd") or 0
        leg = 1 if (insider.get("buys") and b >= 1e5 and b > s) else -1 if (not insider.get("buys") and s >= 1e7 and (insider.get("sellers") or 0) >= 3) else 0
        legs["insider"] = leg
        score += leg
    verdict = "NO READ" if not have else "BUYING" if score >= 2 else "SELLING" if score <= -2 else "LEANING BUY" if score == 1 else "LEANING SELL" if score == -1 else "QUIET"
    return {"verdict": verdict, "score": score if have else None, "legs": legs}


def f13_of(tbl_rows, cols, ticker):
    for t in spellings(ticker):
        row = tbl_rows.get(t)
        if row:
            d = dict(zip(cols, row))
            out = {k: d.get(k) for k in F13_KEEP}
            prior = d.get("value_prior_usd")
            out["net_pct"] = f(100.0 * d.get("net_usd") / prior, 2) if d.get("net_usd") is not None and prior else None
            return out
    return None


def weighted(rows, getter, weight):
    s = w = 0.0
    for r in rows:
        v, wt = getter(r), weight(r)
        if v is not None and wt:
            s += v * wt
            w += wt
    return f(s / w) if w else None


def industry_rows(leaders, industries, book_ind, etf_by):
    """One row per industry from its leaders (the five largest by market cap, ADRs included): cap-weighted and
    equal-weighted excess return, breadth against the averages, accumulation / distribution, smart money, tape money
    flow, the 13F book's industry layer, the industry's funds and their reported flows. Pure."""
    by = {}
    for r in leaders:
        if r.get("rank") is not None and r["rank"] <= TOP_N:
            by.setdefault(r["industry"], []).append(r)
    out = []
    for ind, meta in industries.items():
        ls = sorted(by.get(ind) or [], key=lambda r: r["rank"])
        live = [r for r in ls if r.get("vs")]
        row = {"industry": ind, "sector": meta["sector"], "n_members": meta["n_members"], "mcap_usd": meta["mcap_usd"], "n_leaders": len(ls), "leaders": [r["ticker"] for r in ls],
               "leader": ls[0]["ticker"] if ls else None, "small": meta.get("small", False), "vs": None, "h": None, "ew_vs": None, "composite_pp": None, "z": None, "strength": None}
        if live:
            cap = lambda r: r.get("mcap") or 0                                    # noqa: E731
            row["vs"] = {k: weighted(live, lambda r, k=k: r["vs"].get(k), cap) for k, _ in T.HORIZONS}
            row["h"] = {k: weighted(live, lambda r, k=k: (r.get("h") or {}).get(k), cap) for k, _ in T.HORIZONS}
            row["ew_vs"] = {k: weighted(live, lambda r, k=k: r["vs"].get(k), lambda r: 1.0) for k, _ in T.HORIZONS}
            row["composite_pp"] = T.composite(row["vs"])
        n = len(live)
        ma = [r["ma"] for r in live if r.get("ma")]
        row["breadth"] = {
            "n": n,
            "above_50": sum(1 for m in ma if m.get("d50") is not None and m["d50"] > 0), "above_200": sum(1 for m in ma if m.get("d200") is not None and m["d200"] > 0),
            "pct_above_50": f(100.0 * sum(1 for m in ma if m.get("d50") is not None and m["d50"] > 0) / n, 0) if n else None,
            "pct_above_200": f(100.0 * sum(1 for m in ma if m.get("d200") is not None and m["d200"] > 0) / n, 0) if n else None,
            "beating_m": sum(1 for r in live if r["vs"].get("m") is not None and r["vs"]["m"] > 0),
            "golden": sum(1 for r in live if (r.get("cross") or {}).get("fresh") and r["cross"]["type"] == "GOLDEN"),
            "death": sum(1 for r in live if (r.get("cross") or {}).get("fresh") and r["cross"]["type"] == "DEATH"),
            "regime_up": sum(1 for r in live if (r.get("cross") or {}).get("regime") == "50>200"),
        }
        scores = [r["ad"]["score"] for r in live if (r.get("ad") or {}).get("score") is not None]
        acc = sum(1 for r in live if ((r.get("ad") or {}).get("score") or 0) >= T.AD_LEAN)
        dist = sum(1 for r in live if ((r.get("ad") or {}).get("score") or 0) <= -T.AD_LEAN)
        avg = sum(scores) / len(scores) if scores else None
        row["ad"] = {"score": f(avg, 1), "label": T.ad_label(avg), "acc": acc, "dist": dist}
        row["mf21_usd"] = f(sum(((r.get("mf") or {}).get("m") or {}).get("net") or 0 for r in live), 0) if live else None
        row["mf63_usd"] = f(sum(((r.get("mf") or {}).get("q") or {}).get("net") or 0 for r in live), 0) if live else None
        sm = [(r.get("smart") or {}).get("verdict") for r in ls]
        f13s = [r["f13"] for r in ls if r.get("f13")]
        row["smart"] = {"buying": sum(1 for v in sm if v in ("BUYING", "LEANING BUY")), "selling": sum(1 for v in sm if v in ("SELLING", "LEANING SELL")),
                        "f13_net_usd": f(sum(x.get("net_usd") or 0 for x in f13s), 0) if f13s else None, "f13_in_usd": f(sum(x.get("inflow_usd") or 0 for x in f13s), 0) if f13s else None,
                        "f13_out_usd": f(sum(x.get("outflow_usd") or 0 for x in f13s), 0) if f13s else None, "n_f13": len(f13s),
                        "insider_net_usd": f(sum((r.get("insider") or {}).get("net_usd") or 0 for r in ls), 0) if any(r.get("insider") for r in ls) else None}
        row["f13"] = book_ind.get(ind)
        funds = industry_etf_catalog.etfs_for(ind, meta["sector"])
        row["etfs"] = [x["ticker"] for x in funds]
        row["etf_roles"] = {x["ticker"]: x["role"] for x in funds}
        own = [x for x in funds if x["role"] != "sector"]
        use = [x["ticker"] for x in own if ((etf_by.get(x["ticker"]) or {}).get("flow") or {}).get("current")]
        proxy = None
        if not use:
            sec = [x["ticker"] for x in funds if x["role"] == "sector" and ((etf_by.get(x["ticker"]) or {}).get("flow") or {}).get("current")]
            use, proxy = sec, ("sector" if sec else None)
        flow = {"funds": use, "proxy": proxy, "n_funds": len(use)}
        for wk in ("w1", "w5", "w21"):
            xs = [etf_by[t]["flow"][wk] for t in use if etf_by[t]["flow"].get(wk)]
            aum = sum(etf_by[t]["flow"].get("aum_usd") or 0 for t in use if etf_by[t]["flow"].get(wk))
            usd = sum(x["usd"] for x in xs) if xs else None
            flow[wk] = {"usd": f(usd, 0), "pct_aum": f(100.0 * usd / aum, 3) if (usd is not None and aum) else None} if xs else None
        row["etf_flow"] = flow
        row["signals"] = {k: sum(1 for r in ls if any(s.startswith(k) for s in (r.get("signals") or []))) for k in
                          ("GOLDEN CROSS", "DEATH CROSS", "DOUBLE TOP", "DOUBLE BOTTOM", "BUYING CLIMAX", "SELLING CLIMAX", "VERY TIGHT", "TIGHT")}
        out.append(row)
    T.rank_rows([r for r in out if r.get("vs")])
    ranked = sorted([r for r in out if r.get("strength") is not None], key=lambda r: -r["strength"])
    for i, r in enumerate(ranked):
        r["rank"] = i + 1
    out.sort(key=lambda r: (r.get("rank") is None, r.get("rank") or 0, r["industry"]))
    return out


def board(rows, pred, key):
    """Every ticker that prints the signal, in board order - never truncated, so a count is the length of the list."""
    xs = [r for r in rows if pred(r)]
    xs.sort(key=key)
    return [r["ticker"] for r in xs]


def _live(p):
    return bool(p) and p.get("state") in ("FORMING", "CONFIRMED")


def _pattern_order(p):
    """Fresh confirmations first, then patterns still forming, then confirmations whose target has already printed."""
    done = bool(p.get("target_hit"))
    return (0 if (p["state"] == "CONFIRMED" and not done) else 1 if p["state"] == "FORMING" else 2, (p.get("break") or {}).get("ago", p["ago"]) if p["state"] == "CONFIRMED" else p["ago"])


def boards_of(rows):
    """What is being printed now, as ticker lists ordered by how fresh / how strong. Pure."""
    live = [r for r in rows if not r.get("stale")]
    cr = lambda r: r.get("cross") or {}                                           # noqa: E731
    ad = lambda r: (r.get("ad") or {}).get("score")                               # noqa: E731
    return {
        "golden_cross": board(live, lambda r: cr(r).get("fresh") and cr(r)["type"] == "GOLDEN", lambda r: cr(r)["ago"]),
        "death_cross": board(live, lambda r: cr(r).get("fresh") and cr(r)["type"] == "DEATH", lambda r: cr(r)["ago"]),
        "golden_near": board(live, lambda r: cr(r).get("near") == "GOLDEN NEAR", lambda r: cr(r).get("eta") or 99),
        "death_near": board(live, lambda r: cr(r).get("near") == "DEATH NEAR", lambda r: cr(r).get("eta") or 99),
        "double_top": board(live, lambda r: _live(r.get("dtop")), lambda r: _pattern_order(r["dtop"])),
        "double_bottom": board(live, lambda r: _live(r.get("dbot")), lambda r: _pattern_order(r["dbot"])),
        "buying_climax": board(live, lambda r: (r.get("climax") or {}).get("type") == "BUYING", lambda r: r["climax"]["ago"]),
        "selling_climax": board(live, lambda r: (r.get("climax") or {}).get("type") == "SELLING", lambda r: r["climax"]["ago"]),
        "tight": board(live, lambda r: (r.get("tight") or {}).get("tier") in ("VERY TIGHT", "TIGHT") and not r["tight"].get("flag"), lambda r: -(r["tight"].get("score") or 0)),
        "accumulation": board(live, lambda r: ad(r) is not None and ad(r) >= T.AD_STRONG, lambda r: -ad(r)),
        "distribution": board(live, lambda r: ad(r) is not None and ad(r) <= -T.AD_STRONG, lambda r: ad(r)),
        "smart_buying": board(live, lambda r: (r.get("smart") or {}).get("verdict") == "BUYING", lambda r: -(((r.get("f13") or {}).get("net_usd")) or 0)),
        "smart_selling": board(live, lambda r: (r.get("smart") or {}).get("verdict") == "SELLING", lambda r: (((r.get("f13") or {}).get("net_usd")) or 0)),
        "bull_stack": board(live, lambda r: (r.get("ma") or {}).get("stack") == "BULL", lambda r: -(r.get("strength") or 0)),
        "bear_stack": board(live, lambda r: (r.get("ma") or {}).get("stack") == "BEAR", lambda r: (r.get("strength") or 0)),
        "rs_new_highs": board(live, lambda r: r.get("rs_new_high"), lambda r: -(r.get("strength") or 0)),
        "rs_new_lows": board(live, lambda r: r.get("rs_new_low"), lambda r: (r.get("strength") or 0)),
    }


def summary_of(rows):
    live = [r for r in rows if r.get("vs") and not r.get("stale")]
    n = len(live)
    out = {"n": n}
    for k in ("w", "m", "q", "h", "y"):
        d = [r for r in live if r["vs"].get(k) is not None]
        up = sum(1 for r in d if r["vs"][k] > 0)
        out["beating_" + k] = up
        out["pct_beating_" + k] = f(100.0 * up / len(d), 1) if d else None
    for w in T.MA_WINDOWS:
        d = [r for r in live if (r.get("ma") or {}).get("d%d" % w) is not None]
        out["pct_above_%d" % w] = f(100.0 * sum(1 for r in d if r["ma"]["d%d" % w] > 0) / len(d), 1) if d else None
    lab = [(r.get("ad") or {}).get("label") for r in live]
    out["accumulation"] = sum(1 for x in lab if x in ("ACCUMULATION", "STRONG ACCUMULATION"))
    out["distribution"] = sum(1 for x in lab if x in ("DISTRIBUTION", "STRONG DISTRIBUTION"))
    out["strong_accumulation"] = sum(1 for x in lab if x == "STRONG ACCUMULATION")
    out["strong_distribution"] = sum(1 for x in lab if x == "STRONG DISTRIBUTION")
    sm = [(r.get("smart") or {}).get("verdict") for r in live]
    for k, name in (("BUYING", "smart_buying"), ("SELLING", "smart_selling"), ("LEANING BUY", "smart_leaning_buy"), ("LEANING SELL", "smart_leaning_sell"), ("NO READ", "smart_no_read")):
        out[name] = sum(1 for x in sm if x == k)
    out["regime_up"] = sum(1 for r in live if (r.get("cross") or {}).get("regime") == "50>200")
    out["regime_down"] = sum(1 for r in live if (r.get("cross") or {}).get("regime") == "50<200")
    out["mf21_net_usd"] = f(sum(((r.get("mf") or {}).get("m") or {}).get("net") or 0 for r in live), 0)
    return out


def changes_since(prev, rows, etfs, as_of):
    """Signals that appeared or went away since the previous session's artifact. A re-run on the same session keeps
    the block it already published. Pure."""
    if not isinstance(prev, dict) or prev.get("contract") != CONTRACT:
        return {"prev_as_of": None, "new": [], "gone": [], "note": "first publication: nothing to compare with"}
    if prev.get("as_of") == as_of:
        return prev.get("changes") or {"prev_as_of": None, "new": [], "gone": []}
    before = {}
    for r in (prev.get("rows") or []) + (prev.get("etfs") or []):
        before[r.get("ticker")] = set(r.get("signals") or [])
    new, gone = [], []
    for r in rows + etfs:
        if r["ticker"] not in before:
            continue
        now = set(r.get("signals") or [])
        for s in sorted(now - before[r["ticker"]]):
            new.append({"ticker": r["ticker"], "signal": s, "industry": r.get("industry"), "kind": r.get("kind")})
        for s in sorted(before[r["ticker"]] - now):
            gone.append({"ticker": r["ticker"], "signal": s, "industry": r.get("industry"), "kind": r.get("kind")})
    return {"prev_as_of": prev.get("as_of"), "new": new[:400], "gone": gone[:400], "n_new": len(new), "n_gone": len(gone)}


DEFINITIONS = {
    "leaders": "the five largest companies of each FMP industry by market cap among US-listed common stocks and ADRs (share classes of one CIK folded into the most traded); a leader needs $2B of market cap and $5M of daily dollar volume, an industry with no such company keeps its largest above $300M; rank counts ADRs, rank_us does not",
    "vs": "excess return against SPY in percentage points on 1 / 5 / 21 / 63 / 126 / 252 sessions, date-aligned, bars split-rebased",
    "strength": "percentile (0-100) within the leaders of the weighted cross-sectional z-score of excess return on 1d / 5d / 21d / 63d / 126d (10 / 20 / 30 / 25 / 15) - the strength desk's construction",
    "ma": "the last close against its 20 / 50 / 100 / 200 / 250-session simple averages: d<N> = distance in %, above = how many it is over, stack = BULL when close > 20 > 50 > 100 > 200 > 250, BEAR when the reverse, else MIXED",
    "cross": "50 / 200-session average cross: GOLDEN when the 50 crosses above the 200, DEATH below; fresh = within 20 sessions; near = the gap is at most 1.5% and closing at a pace that meets within 15 sessions; n_120 counts the crosses of the last 120 sessions (2 or more is a whipsaw)",
    "double_top": "two swing highs within 3-5% of each other, 10-150 sessions apart, a swing low between them at least 5% lower, the first high a 90-session high; FORMING while closes hold that low (the neckline) and the second high is at most 40 sessions old; CONFIRMED on a close 0.5% under the neckline (kept 20 sessions; target_hit once a close reaches the neckline minus the pattern's height); FAILED when the close is back over the neckline by more than max(3%, half the swing threshold) before the target printed - a failed pattern stays on the row and off the boards; void on a close 0.5% over the higher high. to_neck_pct is the neckline against the last close",
    "double_bottom": "the mirror of the double top; vol_ratio under 1 means the second low printed on lighter volume than the first (the secondary test of the bottom paper)",
    "climax": "SELLING: a new 120-session low after a decline of at least 6 ATR and 12% (funds 10%), close under the 50-session average and in the bottom 35% of the yearly range, the heaviest volume of 60 sessions and at least 2x the 250-session average, a range of at least 1.5 ATR, and absorption (the close off the low or a higher close within three sessions). BUYING: the mirror at a new 120-session high after an advance of at least 20% (funds 12%). The last 30 sessions are read; day_pct and gap_pct are the climax session's own move and opening gap (a bar that is one piece of news reads as that), held says whether its extreme has stood since. A climax locates stopping action; it is not a trade",
    "tight": "the strength desk's coil: the longest trailing run of closes inside 6% (VERY TIGHT, 40+ sessions), 10% (TIGHT, 40+), 15% (LONG BASE, 120+) or 10% (FORMING, 20+) of the last close, counted only when the base is at most 60% of the yearly close range; score 0-100 = length + tightness + Bollinger squeeze + volume dry-up + accumulation inside the base + RS holding; PINNED = a stock almost motionless inside a sliver of a base (deal-like), kept off the board",
    "ad": "tape accumulation / distribution, -100..+100: Chaikin money flow on 21 and 63 sessions, up-volume over down-volume on 50, net on-balance volume on 63, accumulation minus distribution sessions (a move of 0.2% on volume 1.2x the 50-session average) in the last 25; weights 25 / 20 / 25 / 15 / 15; +60 STRONG ACCUMULATION (A), +20 ACCUMULATION (B), -20 DISTRIBUTION (D), -60 STRONG DISTRIBUTION (E); the benchmark's own score is published beside the leaders'",
    "mf": "signed dollar volume: a session's typical price x shares counts as money in when the typical price rose from the prior session and money out when it fell; in / out / net on 5 / 21 / 63 sessions. A tape estimate of pressure, not a reported flow",
    "f13": "the 13F cohort's read of the ticker across two dated quarterly disclosures: holders now / prior, bull / bear manager votes, dollars in / out / net at reported prices, stance. Newly disclosed is not bought, absent is not sold, and the filings are 45+ days old when they arrive",
    "insider": "open-market purchases (P) and sales (S) on Form 4 over the last 90 days from the banked feed: transactions, distinct insiders, dollars. Awards, option exercises, tax withholding and gifts are not counted. Sales are mostly scheduled plans; purchases are the informative side. Foreign private issuers (most ADRs) do not file Form 4",
    "smart": "13F leg (+2 ACCUMULATING, -2 DISTRIBUTING, +-1 when contested but net dollars are at least 10% of the larger of the two reported values) + insider leg (+1 on purchases of at least $100K exceeding sales, -1 on $10M+ of sales by three or more insiders with no purchase): BUYING at +2 or more, SELLING at -2 or less, LEANING at +-1, QUIET at 0, NO READ without either leg",
    "etf_flow": "reported creations / redemptions of the industry's funds on the fund's own last 1 / 5 / 21 reporting observations, USD and % of reported assets: the native ETF desk, else the canonical 300-fund packet, else the Polygon bank's per-fund ETF Global history (used only when every row names the fund and its last observation is within 7 days). An industry with no fund of its own shows its sector fund, labelled a sector proxy",
    "industry": "an industry's row is its five largest leaders: cap-weighted (vs) and equal-weighted (ew_vs) excess return, share above the 50 / 200-session averages, average A/D score, the leaders' summed signed dollar volume and 13F dollars, and the 13F book's own industry layer (all the cohort's holdings in the industry, not only the leaders)",
    "history": "the bars a row is computed from: sessions and the first date; cut = where the series was shortened (gap: the ticker printed nothing for more than 10 days, so the bars before belong to another security or another stretch; listing: bars older than the listing date; separation: an overnight collapse on the day a same-sector company worth 0.5x-2x the vanished value listed); stitched = the previous symbol's bars put back in front (named by Polygon's ticker events or FMP's symbol-change list, and only when the tape hands over within x0.7-x1.4); jumps = one-session moves beyond x1.8 / x0.56 that are real prints (a horizon spanning one says so). A horizon longer than the history is null",
    "changes": "signals that appeared or went away against the previous session's publication",
}
LIMITATIONS = [
    "13F is quarterly and arrives up to 45 days after the quarter: the cohort read is the last two disclosures, not this week's trading.",
    "FMP's all-institution ownership summaries are not banked (the provider's bandwidth hold), so the institutional read is the 13F desk's manager cohort only (f13.n_cohort managers).",
    "Signed dollar volume and the A/D score are tape estimates from daily bars; every share bought was also sold.",
    "Insider data covers Form 4 filers; foreign private issuers (most ADRs) are exempt and carry no insider leg.",
    "Fund flows for funds outside the ETF desk and the canonical 300 come from the Polygon bank's rotation and can be several days behind; a history older than 7 days is shown as not current.",
    "The bars adjust for splits only. A spin-off worth less than 44% of its parent prints as a one-session drop in the parent that no holder lost; dividends are not added back.",
    "A ticker change is bridged only when a record names the previous symbol; without one the history starts at the change (history.cut) and longer horizons are null.",
    "Double tops / bottoms and climaxes are mechanical readings of daily bars with no out-of-sample validation; nothing here is a call or grants sizing authority.",
]


# ---------------------------------------------------------------------------------------------------------------- build
def build(s3, now, log=print, remaining_s=300.0):
    started = time.time()
    now_iso = now.isoformat()
    deadline = started + max(0.0, remaining_s - RESERVE_S)
    errors = []
    # 1. the universe
    book = bank_enrich.rows(fields=BOOK_FIELDS)
    if not book:
        raise RuntimeError("the factor book is not readable: the leaders universe cannot be drawn")
    leaders, industries, ustats = select_leaders(book)
    if len(leaders) < MIN_LEADERS:
        raise RuntimeError("only %d leaders from %d factor-book rows: the book is short; the previous publication stands" % (len(leaders), len(book)))
    book_as_of = bank_enrich.as_of()
    man, _, man_err = optional(s3, polygon_bankfirst.MANIFEST)
    bank_last = (man or {}).get("last_session")
    if man_err:
        errors.append({"input": "polygon-eod-manifest", "error": man_err})
    meta_by = {x["ticker"]: x for x in leaders}
    etf_meta = {}
    for ind, m in industries.items():
        if not any(x["industry"] == ind for x in leaders):
            continue
        for e in industry_etf_catalog.etfs_for(ind, m["sector"]):
            d = etf_meta.setdefault(e["ticker"], {"ticker": e["ticker"], "industries": [], "roles": {}, "thin": e["thin"]})
            d["industries"].append(ind)
            d["roles"][ind] = e["role"]
    log("leaders: %d leaders in %d industries (%d factor-book rows, %d common, %d classes folded), %d funds, bank last session %s" % (
        len(leaders), ustats["industries_with_leader"], ustats["rows"], ustats["common"], ustats["folded"], len(etf_meta), bank_last))
    # 2. bars (bank only)
    bench = read_bars(s3, BENCH)
    if not bench:
        raise RuntimeError("the Polygon EOD bank has no %s bars" % BENCH)
    tickers = [t for t in list(meta_by) + list(etf_meta) if t != BENCH]
    bars_by = {}

    def fetch(t):
        if time.time() > deadline:
            return t, None
        try:
            return t, read_bars(s3, t)
        except Exception as exc:  # noqa: BLE001
            errors.append({"ticker": t, "error": "%s: %s" % (type(exc).__name__, str(exc)[:80])})
            return t, []
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        for t, b in pool.map(fetch, dict.fromkeys(tickers)):
            bars_by[t] = b
    unbanked = sorted(t for t, b in bars_by.items() if b is not None and not b)
    skipped = sorted(t for t, b in bars_by.items() if b is None)
    # 2a. continuity: a ticker is not a security. Each series is cut to its own continuous history, and the symbol
    #     it traded under before is put back in front where the record and the tape agree
    ipo_by = {canon(r.get("symbol")): str(r["ipo_date"])[:10] for r in book if r.get("ipo_date")}
    changes, changes_mod, changes_err = optional(s3, SYMBOL_CHANGE_KEY)
    changes = changes if isinstance(changes, list) else []
    if changes_err:
        errors.append({"input": "fmp-symbol-change", "error": changes_err})
    cuts, stitched_by, refused_by, alias = {}, {}, {}, {}
    bench, _ = T.own_history(bench)
    for t, b in list(bars_by.items()):
        if not b:
            continue
        try:
            bars_by[t], cut, st, why, olds = continuous(s3, t, b, ipo_by.get(t), changes, deadline)
        except Exception as exc:  # noqa: BLE001
            errors.append({"ticker": t, "error": "continuity %s: %s" % (type(exc).__name__, str(exc)[:80])})
            bars_by[t], cut = T.own_history(b, ipo_by.get(t))
            st, why, olds = [], None, []
        if cut:
            cuts[t] = cut
        if st:
            stitched_by[t], alias[t] = st, olds
        if why:
            refused_by[t] = why
    # 2b. splits: recorded ones the bars still print (the predecessor symbol's included), then inferred ones
    first_day = min([b[0]["date"] for b in bars_by.values() if b] + [bench[0]["date"]])
    rec, n_windows = recorded_splits(s3, first_day, (bank_last or bench[-1]["date"]), set(bars_by) | {BENCH} | {o for olds in alias.values() for o in olds})
    splits_by, jumps_by = {}, {}
    bench, ev = T.rebase(bench, rec.get(BENCH))
    if ev:
        splits_by[BENCH] = ev
    listings = listings_of(book)
    for t, b in list(bars_by.items()):
        if b:
            b, ev = T.rebase(b, (rec.get(t) or []) + [e for o in alias.get(t, []) for e in rec.get(o) or []])
            if ev:
                splits_by[t] = ev
            # 2c. a collapse on the day a same-sector company of matching value lists is a separation, not a loss
            m = meta_by.get(t)
            if m:
                b, sep = T.separation_cut(b, ev, t, m["mcap"], m["sector"], listings)
                if sep:
                    cuts[t] = sep
            jumps = T.jumps_of(b, ev)
            if jumps:
                jumps_by[t] = jumps
            bars_by[t] = b

    def history_of(t, b):
        return {"sessions": len(b), "from": b[0]["date"], "cut": cuts.get(t), "stitched": stitched_by.get(t), "jumps": jumps_by.get(t)}
    # 3. the joins
    tbl, tbl_mod, tbl_err = optional(s3, TICKERS_KEY)
    tbl_rows, tbl_cols = (tbl or {}).get("by_ticker") or {}, (tbl or {}).get("columns") or []
    bookdoc, book_mod, book_err = optional(s3, BOOK_KEY)
    cohort_ind = ((((bookdoc or {}).get("book") or {}).get("sectors") or {}).get("cohort") or {}).get("industries") or []
    conc_ind = ((((bookdoc or {}).get("book") or {}).get("sectors") or {}).get("concentrated") or {}).get("industries") or []
    book_ind = {}
    for x in cohort_ind:
        if isinstance(x, dict) and x.get("name"):
            book_ind[x["name"]] = {k: x.get(k) for k in F13_IND_KEEP}
    for x in conc_ind:
        if isinstance(x, dict) and x.get("name") in book_ind:
            book_ind[x["name"]]["conc"] = {k: x.get(k) for k in ("value_usd", "net_usd", "stance", "money_stance", "n_managers")}
    desk, desk_mod, desk_err = optional(s3, DESK_KEY)
    canon_doc, canon_mod, canon_err = optional(s3, CANON_KEY)
    phase, _, phase_err = optional(s3, PHASE_KEY)
    bottom, _, bottom_err = optional(s3, BOTTOM_KEY)
    for k, e in (("13f-tickers", tbl_err), ("13f-book", book_err), ("etf-desk", desk_err), ("canonical-flows", canon_err), ("phase-detector", phase_err), ("bottom", bottom_err)):
        if e:
            errors.append({"input": k, "error": e})
    flows = {}
    for t, fd in (((desk or {}).get("funds")) or {}).items():
        if t in etf_meta:
            flows[t] = packet_flow(fd.get("flows") or {}, (fd.get("quality") or {}).get("flow_current_eligible"), now_iso, "etf-desk")
    for t, fl in (((canon_doc or {}).get("funds")) or {}).items():
        if t in etf_meta and t not in flows:
            flows[t] = packet_flow(fl, (fl.get("quality") or {}).get("status") == "complete_acquired_history", now_iso, "provider-flow-research")
    bank_reads = {"asked": 0, "this_fund": 0, "other_funds": 0, "absent": 0, "not_current": 0}

    def bank_one(t):
        try:
            rows, _ = get_json(s3, BANK_FLOW_ROOT + t + ".json.gz")
        except Exception as exc:  # noqa: BLE001
            return t, None, "absent" if missing(exc) else type(exc).__name__
        rows = rows if isinstance(rows, list) else (rows.get("results") if isinstance(rows, dict) else None)
        fl = bank_flow(rows, t, bank_last)
        return t, fl, None if fl else ("other_funds" if rows else "absent")
    need = [t for t in etf_meta if t not in flows or not flows[t]["current"]]
    with ThreadPoolExecutor(max_workers=8) as pool:
        for t, fl, why in pool.map(bank_one, need):
            bank_reads["asked"] += 1
            if fl:
                bank_reads["this_fund"] += 1
                if not fl["current"]:
                    bank_reads["not_current"] += 1
                if t not in flows or fl["current"]:
                    flows[t] = fl
            elif why in bank_reads:
                bank_reads[why] += 1
            elif why:
                errors.append({"input": "polygon-bank-flow", "ticker": t, "error": why})
    insiders, insider_meta = read_insiders(s3, set(meta_by), now.date(), INSIDER_DAYS, deadline)
    phases = (phase or {}).get("phases_all") or {}
    bottom_by = {}
    for x in (bottom or {}).get("board_all") or []:
        if isinstance(x, dict) and x.get("ticker") and (x["ticker"] not in bottom_by or x.get("frame") == "D"):
            bottom_by[x["ticker"]] = {k: x.get(k) for k in ("frame", "state", "grade", "score", "sc_date", "st_date")}
    # 4. rows
    rows = []
    for t, m in meta_by.items():
        b = bars_by.get(t)
        if not b:
            continue
        r = {"ticker": t, "name": m["name"], "kind": "stock", "sector": m["sector"], "industry": m["industry"], "country": m["country"], "adr": m["adr"], "rank": m["rank"], "rank_us": m["rank_us"],
             "mcap": f(m["mcap"], 0), "mcap_share_pct": m.get("mcap_share_pct"), "dv20": f(m["dv20"], 0), "classes": m.get("classes") or [], "small": bool(m.get("small")),
             "short_pct_float": f(m.get("short_pct_float")), "days_to_cover": f(m.get("days_to_cover")), "short_volume_ratio": f(m.get("short_volume_ratio"), 3), "beta": f(m.get("beta"))}
        r.update(tape_row(b, bench, bank_last, "stock"))
        r["history"] = history_of(t, b)
        r["splits"] = splits_by.get(t)
        r["f13"] = f13_of(tbl_rows, tbl_cols, t)
        r["insider"] = insiders.get(t)
        r["smart"] = smart_verdict(r["f13"], r["insider"])
        ph = next((phases[s] for s in spellings(t) if s in phases), None)
        bt = next((bottom_by[s] for s in spellings(t) if s in bottom_by), None)
        r["desks"] = {"phase": (ph or {}).get("p"), "phase_begin": (ph or {}).get("b"), "bottom": bt} if (ph or bt) else None
        funds = industry_etf_catalog.etfs_for(m["industry"], m["sector"])
        r["etf"] = funds[0]["ticker"] if funds else None
        rows.append(r)
    zstats = T.rank_rows(rows)
    for r in rows:
        r["signals"] = signals_of(r)
    etfs = []
    for t, m in etf_meta.items():
        b = bars_by.get(t)
        if not b:
            continue
        r = {"ticker": t, "kind": "fund", "industries": m["industries"], "roles": m["roles"], "role": "industry" if "industry" in m["roles"].values() else "theme" if "theme" in m["roles"].values() else "sector",
             "thin": m["thin"], "n_industries": len(m["industries"])}
        r.update(tape_row(b, bench, bank_last, "fund"))
        r["history"] = history_of(t, b)
        r["splits"] = splits_by.get(t)
        r["flow"] = flows.get(t)
        r["f13"] = f13_of(tbl_rows, tbl_cols, t)
        etfs.append(r)
    T.rank_rows(etfs)
    for r in etfs:
        r["signals"] = signals_of(r)
    etf_by = {r["ticker"]: r for r in etfs}
    rows.sort(key=lambda r: (-(r["strength"] if r["strength"] is not None else -1), r["ticker"]))
    etfs.sort(key=lambda r: (-(r["strength"] if r["strength"] is not None else -1), r["ticker"]))
    inds = industry_rows(rows, {k: v for k, v in industries.items() if any(r["industry"] == k for r in rows)}, book_ind, etf_by)
    ind_rank = {x["industry"]: x.get("rank") for x in inds}
    ind_strength = {x["industry"]: x.get("strength") for x in inds}
    for r in rows:
        r["industry_rank"] = ind_rank.get(r["industry"])
        r["industry_strength"] = ind_strength.get(r["industry"])
    prev, _, _ = optional(s3, OUT_KEY)
    as_of = bank_last or bench[-1]["date"]
    bench_c = [float(b["c"]) for b in bench]
    sources = [{**s, **head(s3, s["key"], now, s["expected_h"])} for s in SOURCES]
    n_flow = sum(1 for r in etfs if (r.get("flow") or {}).get("current"))
    out = {
        "engine": ENGINE, "version": VERSION, "contract": CONTRACT, "generated_at": now.isoformat(timespec="seconds"), "as_of": as_of,
        "bank": {"last_session": bank_last, "updated_at": (man or {}).get("updated_at"), "tickers": (man or {}).get("tickers"), "lookback_sessions": LOOKBACK},
        "benchmark": {"ticker": BENCH, "px": f(bench_c[-1]), "as_of": bench[-1]["date"], "h": T.horizons(bench_c), "sessions": len(bench), "ma": T.ma_block(bench_c),
                      "cross": T.cross_50_200(bench_c, [b["date"] for b in bench]), "ad": T.ad_block(bench), **T.context_52w(bench)},
        "universe": {"top_n": TOP_N, "min_cap_usd": MIN_CAP, "min_dollar_vol_usd": MIN_DV, "floor_cap_usd": FLOOR_CAP, "floor_dollar_vol_usd": FLOOR_DV, "book_as_of": book_as_of, "stats": ustats,
                     "n_leaders": len(rows), "n_top": sum(1 for r in rows if r["rank"] <= TOP_N), "n_adr": sum(1 for r in rows if r["adr"]), "n_industries": len(inds), "n_etfs": len(etfs),
                     "n_sectors": len({r["sector"] for r in rows}), "unbanked": unbanked, "skipped_for_time": skipped,
                     "industries_without_leader": sorted(k for k in industries if not any(r["industry"] == k for r in rows))},
        "summary": summary_of(rows), "z_stats": zstats,
        "rows": rows, "industries": inds, "etfs": etfs,
        "boards": {"leaders": boards_of(rows), "etfs": boards_of(etfs)},
        "changes": changes_since(prev, rows, etfs, as_of),
        "splits": {"windows_read": n_windows, "n_rebased": len(splits_by), "recorded": sum(1 for ev in splits_by.values() for e in ev if e.get("source") == "polygon-splits"),
                   "inferred": sum(1 for ev in splits_by.values() for e in ev if e.get("source") == "inferred"), "tickers": {t: ev for t, ev in sorted(splits_by.items())},
                   "note": "the bank's history was pulled adjusted; sessions appended since are raw. A recorded split is applied only when the bars still print it; an unrecorded jump is rebased when volume confirms it"},
        "history": {"gap_days": T.GAP_DAYS, "n_cut": len(cuts), "n_stitched": len(stitched_by), "n_with_jumps": len(jumps_by),
                    "cut": {t: c for t, c in sorted(cuts.items())}, "stitched": {t: x for t, x in sorted(stitched_by.items())}, "refused": {t: x for t, x in sorted(refused_by.items())},
                    "short": sorted(r["ticker"] for r in rows + etfs if r["sessions"] < 253),
                    "symbol_changes": {"rows": len(changes), "newest": max([str(x.get("date") or "") for x in changes if isinstance(x, dict)] or [None]), "last_modified": changes_mod},
                    "note": "the bank keys bars by ticker and a ticker can change hands: a series is cut at a calendar gap of more than %d days, at its listing date and at a separation, and the previous symbol's bars are put back in front only when Polygon's ticker events or FMP's symbol-change list name it and the tape hands over within x%.1f..x%.1f" % (T.GAP_DAYS, T.HANDOVER_MIN, T.HANDOVER_MAX)},
        "f13": {"period": (tbl or {}).get("period"), "generated_at": (tbl or {}).get("generated_at"), "n_cohort": (tbl or {}).get("n_cohort"), "last_modified": tbl_mod,
                "n_leaders_held": sum(1 for r in rows if r.get("f13")), "n_industries": len(book_ind), "book_last_modified": book_mod},
        "flows": {"desk_generated_at": (desk or {}).get("generated_at"), "desk_last_modified": desk_mod, "canonical_generated_at": (canon_doc or {}).get("generated_at"), "canonical_last_modified": canon_mod,
                  "n_funds": len(etfs), "n_current": n_flow, "by_source": {s: sum(1 for r in etfs if (r.get("flow") or {}).get("source") == s and r["flow"].get("current")) for s in ("etf-desk", "provider-flow-research", "polygon-bank")},
                  "no_flow": sorted(r["ticker"] for r in etfs if not (r.get("flow") or {}).get("current")), "polygon_bank": bank_reads},
        "insiders": {**insider_meta, "n_leaders": len(insiders), "n_buying": sum(1 for v in insiders.values() if v["buys"]), "n_selling": sum(1 for v in insiders.values() if v["sells"])},
        "desks": {"phase_generated_at": (phase or {}).get("generated_at"), "phase_n": len(phases), "bottom_generated_at": (bottom or {}).get("generated_at"), "bottom_n": len(bottom_by),
                  "n_leaders_with_phase": sum(1 for r in rows if (r.get("desks") or {}).get("phase")), "n_leaders_on_bottom_desk": sum(1 for r in rows if (r.get("desks") or {}).get("bottom"))},
        "sources": sources, "errors": errors[:60], "n_errors": len(errors), "definitions": DEFINITIONS, "limitations": LIMITATIONS,
        "note": "Leaders are the largest companies of each industry, not a recommendation. Relative strength and the averages are price facts; accumulation / distribution and signed dollar volume are tape estimates; fund flows are reported creations / redemptions; 13F stances count managers across two dated disclosures. No trade, call or sizing is implied.",
        "run_seconds": round(time.time() - started, 1),
    }
    return out


def put_json(s3, key, doc, cache="max-age=300"):
    body = json.dumps(doc, separators=(",", ":"), default=str).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json", CacheControl=cache)
    return len(body)


def lambda_handler(event, context):
    chain_guard.begin(event, context)   # trigger cap: a root run (the schedule) or whatever chain started it
    now = datetime.now(timezone.utc)
    s3 = _s3()
    try:
        remaining = context.get_remaining_time_in_millis() / 1000.0
    except Exception:  # noqa: BLE001
        remaining = 300.0
    out = build(s3, now, remaining_s=remaining)
    trig = event.get("trigger") if isinstance(event, dict) else None
    out["trigger"] = {"kind": trig or "scheduled"}
    n = put_json(s3, OUT_KEY, out)
    b, s = out["boards"]["leaders"], out["summary"]
    print("leaders: %d leaders / %d industries / %d funds as of %s; beating SPY 1M %s%%; A/D %d/%d; smart %d/%d; golden %d death %d; dtop %d dbot %d; climax %d/%d; tight %d; flows %d/%d; %d bytes, %.1fs, errors %d" % (
        len(out["rows"]), len(out["industries"]), len(out["etfs"]), out["as_of"], s.get("pct_beating_m"), s["accumulation"], s["distribution"], s["smart_buying"], s["smart_selling"],
        len(b["golden_cross"]), len(b["death_cross"]), len(b["double_top"]), len(b["double_bottom"]), len(b["buying_climax"]), len(b["selling_climax"]), len(b["tight"]),
        out["flows"]["n_current"], out["flows"]["n_funds"], n, out["run_seconds"], out["n_errors"]))
    return {"statusCode": 200, "key": OUT_KEY, "bytes": n, "leaders": len(out["rows"]), "industries": len(out["industries"]), "etfs": len(out["etfs"]), "as_of": out["as_of"],
            "run_seconds": out["run_seconds"], "errors": out["n_errors"]}
