"""justhodl-campaign-scan - every stock the chart's Accumulation, Bottom and Distribution icons would mark.

Khalid 2026-11-19: "accumulation, bottom, distribution engines should be built for the icons there it should show all
the corresponding stocks".  On chart.html those three icons mark ONE symbol - the one on the chart: CLIMAX and BOTTOM
(the Bot icon), ACCUM (the Acc icon), DIST and D-TOP (the Dist icon).  This engine runs the very same scans over every
listed common stock and every plain ETF in the warehouse and writes the names that carry a mark now:

    boards.bottom         a live selling climax (CLIMAX, inside the last 40 sessions, no low confirmed after it) or a
                          confirmed cycle low (BOTTOM, the low inside the last 126 sessions)
    boards.accumulation   ACCUM - the bar that ends the base that follows a confirmed low - inside the last 63 sessions
    boards.distribution   DIST - a rally that failed its holding tests - inside the last 63 sessions

The scans are not the engine's own: chart_scans.py is a line-for-line port of jh-chart-vol-events.js
(sellingClimaxScan, structureScan), jh-chart-campaign-marks.js (fromScan) and jh-chart-distribution.js
(distributionScan), and tests/campaign-scan-parity.test.js fails the suite when the browser and the port disagree on a
single mark over real bars.  Like the chart, a symbol on the bottom desk's board (data/bottom.json) also takes that
desk's own dates: CLIMAX at its selling climax, BOTTOM at its secondary test, ACCUM at its trigger.

Reads S3 only - the Polygon EOD bank (data/warm/polygon-full/eod/<T>.json.gz), the factor book's identity and size
columns, Polygon's split reference and the bottom desk's board.  No provider call, no invoke, no event.  Daily.

What a row is not: a mark is where the tape printed a pattern, not a forecast and not an instruction.  Every row
carries where price stands against the mark's own levels today, so a mark that has already failed reads as failed.
"""
from __future__ import annotations

import gzip
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone

import boto3
from botocore.config import Config

import bank_enrich          # aws/shared: the whole-market factor book (identity, market cap, dollar volume) - S3 only
import chain_guard          # the fleet-wide trigger cap: every engine start is admitted, counted and stamped
import polygon_bankfirst    # aws/shared: the Polygon EOD bank's layout (ROOT, MANIFEST, FIELDS)

import chart_scans as S     # the chart's own scans, ported; parity is a test
import leaders_tape as T    # the leaders engine's pure tape module, byte-identical copy: own_history, rebase, separation_cut

VERSION = "1.0.1"
CONTRACT = "campaign-scan.v1"
ENGINE = "justhodl-campaign-scan"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("OUT_KEY", "data/campaign-scan.json")
BOTTOM_KEY = "data/bottom.json"
BANK_SPLITS_ROOT = "data/warm/polygon-bank/splits/"
MIN_DV = float(os.environ.get("MIN_DOLLAR_VOL", "250000"))       # 20-session average dollar volume a name must trade to be scanned
MIN_SESSIONS = int(os.environ.get("MIN_SESSIONS", "280"))        # the distribution scan's own floor; the bottom scan wants 300
MIN_SCANNED = int(os.environ.get("MIN_SCANNED", "2500"))         # fewer than this is a broken read, not a market
CLIMAX_SESSIONS = 40           # the chart's own rule for a live climax (jh-chart-campaign-marks.js)
BOTTOM_SESSIONS = int(os.environ.get("BOTTOM_SESSIONS", "126"))  # a confirmed low is first readable 40 sessions after it; this keeps it about a quarter
ACCUM_SESSIONS = int(os.environ.get("ACCUM_SESSIONS", "63"))
DIST_SESSIONS = int(os.environ.get("DIST_SESSIONS", "63"))
STALE_SESSIONS = 3             # a name whose last bar is older than this many sessions is not trading; it is not scanned
SNAP_DAYS = 8                  # the chart snaps a bottom-desk date to the nearest bar inside this many days
JUMP_FLAG = 3.0                # a close-to-close move beyond 3-to-1 either way that no split explains is flagged on the row, never hidden
JUMP_SESSIONS = 300            # ... when it sits inside the sessions the scans look back over
THREADS = int(os.environ.get("THREADS", "16"))
RESERVE_S = 45.0
MAX_ERROR_SHARE = 0.05
BOOK_FIELDS = ["mcap", "dollar_vol_20d", "ipo_date"]
NOT_COMMON_SUFFIX = re.compile(r"-(R|RI|RT|RW|WS|WT|W|U|UN|CVR)$")
# A geared or inverse fund's tape is a derivative of another tape. "Short" names an inverse fund (Short S&P500, Short VIX) unless it
# names a maturity (Short-Term, Short Duration, Ultra-Short Income) or one leg of a long/short book.
GEARED_RX = re.compile(r"\b(\d(\.\d+)?x|ultrapro|ultrashort|inverse|leveraged|bull|bear)\b|\bultra\b(?![\s-]*short)"
                       r"|(?<!long/)(?<!long-)(?<!long )\bshort\b(?![\s-]*(term|duration|maturity|dated|income|bond|treasury|treasuries|muni|municipal|high yield|corporate|credit))", re.I)
BOARDS = ("accumulation", "bottom", "distribution")
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=5, read_timeout=30, max_pool_connections=THREADS * 2)

SOURCES = [
    {"engine": "justhodl-polygon-eod", "key": polygon_bankfirst.MANIFEST, "role": "daily bars of every US stock and ETF since 2021-08-26, one file a ticker", "schedule": "daily 03:10 UTC"},
    {"engine": "justhodl-bank-factors", "key": "data/warm/factor-book/matrix/index.json.gz", "role": "what each symbol is (common stock, ETF, OTC ...), its name, sector, industry, market cap and 20-session dollar volume", "schedule": "daily 02:50 UTC"},
    {"engine": "justhodl-polygon-bank", "key": BANK_SPLITS_ROOT, "role": "Polygon's split reference, to rebase the sessions the bank still holds unadjusted", "schedule": "daily 22:41 UTC"},
    {"engine": "justhodl-bottom", "key": BOTTOM_KEY, "role": "the bottom desk's board: its climax, secondary-test and trigger dates are marked on the chart too", "schedule": "daily 03:45 UTC, Tue-Sat"},
]

DEFINITIONS = {
    "CLIMAX": "Selling climax (sellingClimaxScan): after a markdown (the low sits 10% under the 60-session high, 8.5% under the 20-session high or 14% under the 126-session high), a bar at the 20 / 60 / 126-session low that sold off or undercut, on climactic effort (volume and range well above the 60 sessions before the last 20) and with demand showing (a close in the upper part of the bar, or a rally of 2.5-3.5% inside 8 sessions). Listed while it is live: inside the last 40 sessions, score 4 or more, no confirmed low after it.",
    "BOTTOM": "Confirmed cycle low (structureScan): a swing low (15 sessions to the left, 10 to the right) at the 126- or 252-session low, at least 12% under the 252-session high, followed by a bounce of 4% or more inside 12 sessions; one low per 100-session cluster. It can only be read 40 sessions after the low. Listed while the low is inside the last 126 sessions.",
    "ACCUM": "End of accumulation (structureScan EOA): 8 to 50 sessions after a confirmed low, the first up-close bar that makes the highest high since the low and closes within 0.2% of that high and at least 6% above the low. Listed while the bar is inside the last 63 sessions.",
    "DIST": "Distribution (distributionScan): a swing high within 3% of the 189-session high after an advance of 15% off the 252-session low (or 30% off the 504-session low), where the latest push bought less than the push before it (more volume for less progress, or a new high by under 2.5%), price then gave back half of the push and 3.5% from the high inside 18 sessions, did not close back at the high in the next 6, and the reaction was worse than the one before. The mark prints on the bar that completes those tests. Listed while that bar is inside the last 63 sessions.",
    "D-TOP": "The structure top (a failed swing high at the 252-session high) within 40 sessions of the distribution's rally high, when there is one.",
    "status": "Where the last close stands against the mark's own levels. Bottom: HELD (no low under the marked low since), UNDERCUT (a later low went under it and the last close is back at or above it) or BROKEN (the last close is under it). Accumulation: ABOVE (at or above the ACCUM bar's close), BASE (under it, above the low the base stands on) or LOST (under that low). Distribution: BELOW (under the DIST bar's close), RANGE (between it and the rally high) or RECLAIMED (at or above the rally high).",
    "jump": "A row carries jump {date, ratio} when the symbol's bars hold a one-session close-to-close move beyond 3-to-1 either way, inside the last 300 sessions, that no recorded or inferred split explains. It can be a real collapse or a corporate action the record does not carry; percentages on that row should be read with it in mind.",
    "source": "scan = the chart's scan on this engine's bars; harvest = the bottom desk's own date for the symbol (the chart marks it the same way); both = the two agree on the mark inside 5 sessions.",
    "universe": "Listed common stocks (no OTC, preferreds, warrants, units, rights, shells) and ETFs that are not geared or inverse, with a 20-session average dollar volume of at least the floor, a bar in the last 3 sessions and at least 280 sessions of their own continuous history.",
}

LIMITATIONS = [
    "The bank begins 2021-08-26. The chart may hold a longer history; a mark that depends on bars older than the bank can differ.",
    "Bars are rebased for splits here (recorded, then inferred). The chart's bars come from its own provider; around a fresh split the two can disagree for a few sessions.",
    "A confirmed low is 40 sessions old before any scan can read it. A newer low shows first as a climax, or not at all.",
    "A mark is a pattern on the tape. It carries no fundamental, no position and no forecast.",
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
        d = {k: v for k, v in zip(("o", "h", "l", "c", "v"), r[1:6]) if v is not None}     # a field the bank holds as null is absent, as in the leaders engine
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


def harvest_index(doc):
    """The bottom desk's board the way the chart indexes it: the first row of each ticker. Pure."""
    out = {}
    for r in (doc or {}).get("board") or []:
        if isinstance(r, dict) and r.get("ticker"):
            out.setdefault(canon(r["ticker"]), r)
    return out


# --------------------------------------------------------------------------------------------------------- one ticker
def _r(v, d=2):
    return None if v is None or v != v or v in (float("inf"), float("-inf")) else round(float(v), d)


def clean(bars):
    """Bars a scan can read: every price finite and above zero, the high not under the low. Pure."""
    out = []
    for b in bars:
        try:
            o, h, l, c = float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"])
        except (TypeError, ValueError, KeyError):
            continue
        if not (o > 0 and h > 0 and l > 0 and c > 0 and h >= l) or o != o or h != h or l != l or c != c or h == float("inf"):
            continue
        v = b.get("v")
        out.append({"date": b["date"], "o": o, "h": h, "l": l, "c": c, "v": float(v) if isinstance(v, (int, float)) and v == v and v >= 0 else 0.0})
    return out


def snap(dates, iso):
    """The chart's snapTime(): the bar nearest a date, inside SNAP_DAYS; None otherwise. Pure."""
    if not iso or not dates:
        return None
    try:
        target = date.fromisoformat(str(iso)[:10])
    except ValueError:
        return None
    best, gap = None, 10 ** 9
    lo, hi = 0, len(dates)
    key = target.isoformat()
    while lo < hi:
        mid = (lo + hi) // 2
        if dates[mid] < key:
            lo = mid + 1
        else:
            hi = mid
    for i in (lo - 1, lo):
        if 0 <= i < len(dates):
            g = abs((date.fromisoformat(dates[i]) - target).days)
            if g < gap:
                best, gap = i, g
    return best if best is not None and gap <= SNAP_DAYS else None


def _base(meta, mark, i, dates, C, source):
    n = len(C)
    return {"ticker": meta["ticker"], "name": meta["name"], "kind": meta["kind"], "sector": meta["sector"], "industry": meta["industry"], "mcap": _r(meta["mcap"], 0),
            "adv_usd": _r(meta["adv_usd"], 0), "last": _r(C[-1], 4), "mark": mark, "date": dates[i], "ago": n - 1 - i, "source": source, "px": _r(C[i], 4),
            "since_pct": _r((C[-1] / C[i] - 1) * 100)}


def _merge(scan_i, harv_i):
    """Which bar a mark sits on when the scan and the bottom desk both have one: the later; 'both' inside 5 sessions."""
    if scan_i is None and harv_i is None:
        return None, None
    if harv_i is None:
        return scan_i, "scan"
    if scan_i is None:
        return harv_i, "harvest"
    if abs(scan_i - harv_i) <= 5:
        return max(scan_i, harv_i), "both"
    return (scan_i, "scan") if scan_i > harv_i else (harv_i, "harvest")


def flagged_jump(jumps):
    """The most recent unexplained one-session move beyond JUMP_FLAG-to-1 inside the scans' reach, or None. A real
    collapse and an unrecorded reverse split print the same bar; the row says so instead of guessing. Pure."""
    hits = [j for j in jumps or [] if j.get("ratio") and j.get("ago", 10 ** 6) <= JUMP_SESSIONS and (j["ratio"] >= JUMP_FLAG or j["ratio"] <= 1.0 / JUMP_FLAG)]
    return {"date": hits[-1]["date"], "ratio": hits[-1]["ratio"]} if hits else None


def rows_of(meta, bars, harvest=None, jump=None):
    """The board rows of one symbol from its repaired bars: {board: row} for the boards it is on. Pure."""
    dates = [b["date"] for b in bars]
    O, H, L, C, V = [b["o"] for b in bars], [b["h"] for b in bars], [b["l"] for b in bars], [b["c"] for b in bars], [b["v"] for b in bars]
    n = len(C)
    st, sc, ds, camp, dmarks = S.scan(dates, O, H, L, C, V)
    out = {}
    hv = harvest or {}
    h_sc, h_st, h_tr = snap(dates, hv.get("sc_date")), snap(dates, hv.get("st_date")), snap(dates, hv.get("trigger_date"))
    hstate = {"harvest_state": hv.get("state"), "harvest_grade": hv.get("grade"), "harvest_frame": hv.get("frame")} if hv else {}

    # ---- bottom: the latest of a live climax and a confirmed low
    bottoms = [m for m in camp if m[1] == "BOTTOM"]
    live = [m for m in camp if m[1] == "CLIMAX" and n - 1 - m[2] <= CLIMAX_SESSIONS and not any(b[2] > m[2] for b in bottoms)]
    s_cl = max((m[2] for m in live), default=None)
    s_bt = max((m[2] for m in bottoms), default=None)
    cl_i, cl_src = _merge(s_cl, h_sc if h_sc is not None and n - 1 - h_sc <= CLIMAX_SESSIONS else None)
    bt_i, bt_src = _merge(s_bt if s_bt is not None and n - 1 - s_bt <= BOTTOM_SESSIONS else None, h_st if h_st is not None and n - 1 - h_st <= BOTTOM_SESSIONS else None)
    if cl_i is not None and n - 1 - cl_i > CLIMAX_SESSIONS:
        cl_i = None
    pick = None
    if cl_i is not None and (bt_i is None or cl_i > bt_i):
        pick = ("CLIMAX", cl_i, cl_src)
    elif bt_i is not None:
        pick = ("BOTTOM", bt_i, bt_src)
    if pick:
        mark, i, src = pick
        row = _base(meta, mark, i, dates, C, src)
        low = L[i]
        held = min(L[i + 1:], default=low) >= low
        row.update({"low": _r(low, 4), "off_low_pct": _r((C[-1] / low - 1) * 100), "status": "HELD" if held else "UNDERCUT" if C[-1] >= low else "BROKEN"})
        if mark == "CLIMAX":
            ev = next((m[3] for m in live if m[2] == i), None)
            if ev:
                row.update({"decline_pct": _r(min(ev["drop60"], ev["drop126"]) * 100, 1), "vol_x": _r(ev["rvol"]), "range_x": _r(ev["wide"]), "score": _r(ev["score"]), "bar_pct": _r(ev["ret"] * 100)})
            elif hv:
                row.update({"decline_pct": _r(hv.get("sc_decline_pct"), 1), "vol_x": _r(hv.get("sc_vol_x")), "range_x": _r(hv.get("sc_range_x"))})
            if bt_i is not None:
                row["prior_low_date"] = dates[bt_i]
        else:
            ev = next((m[3] for m in bottoms if m[2] == i), None)
            if ev:
                row["decline_pct"] = _r(ev["ret"] * 100, 1)
                paired = [m for m in camp if m[1] == "CLIMAX" and m[2] < i and i - m[2] <= 80]
                if paired:
                    row["climax_date"] = dates[max(m[2] for m in paired)]
                rev = next((e for e in st if e["kind"] == "revup" and e["i"] > i and e["i"] - i <= 15), None)
                if rev:
                    row["rev_up_date"] = dates[rev["i"]]
            elif hv:
                row.update({"decline_pct": _r(hv.get("sc_decline_pct"), 1), "climax_date": dates[h_sc] if h_sc is not None else None})
            eoa = max((m[2] for m in camp if m[1] == "ACCUM" and m[2] > i), default=None)
            if eoa is not None:
                row["accum_date"] = dates[eoa]
        row.update(hstate)
        out["bottom"] = row

    # ---- accumulation: the latest ACCUM
    s_ac = max(((m[2], m[3]) for m in camp if m[1] == "ACCUM"), key=lambda x: x[0], default=None)
    ac_i, ac_src = _merge(s_ac[0] if s_ac and n - 1 - s_ac[0] <= ACCUM_SESSIONS else None, h_tr if h_tr is not None and n - 1 - h_tr <= ACCUM_SESSIONS else None)
    if ac_i is not None:
        row = _base(meta, "ACCUM", ac_i, dates, C, ac_src)
        if s_ac and s_ac[0] == ac_i or (ac_src == "both" and s_ac):
            low_i = s_ac[1]["from"]
        else:
            under = [k for k in (h_sc, h_st) if k is not None and k < ac_i]          # the bottom desk's base stands on the lower of its climax and its test
            low_i = min(under, key=lambda k: L[k]) if under else None
        if low_i is not None:
            low = L[low_i]
            row.update({"low": _r(low, 4), "low_date": dates[low_i], "base_sessions": ac_i - low_i, "gain_from_low_pct": _r((C[ac_i] / low - 1) * 100, 1),
                        "off_low_pct": _r((C[-1] / low - 1) * 100), "status": "ABOVE" if C[-1] >= C[ac_i] else "BASE" if C[-1] >= low else "LOST"})
        else:
            row["status"] = "ABOVE" if C[-1] >= C[ac_i] else "BASE"
        later = [m for m in live if m[2] > ac_i]
        if later:
            row["climax_after"] = dates[max(m[2] for m in later)]
        row.update(hstate)
        out["accumulation"] = row

    # ---- distribution: the latest DIST
    last_d = max((m for m in dmarks if m[1] == "DIST"), key=lambda m: m[2], default=None)
    if last_d is not None and n - 1 - last_d[2] <= DIST_SESSIONS:
        i, ev = last_d[2], last_d[3]
        row = _base(meta, "DIST", i, dates, C, "scan")
        high = ev["high"]
        row.update({"high": _r(high, 4), "high_date": dates[ev["highI"]], "below_high_pct": _r((C[-1] / high - 1) * 100), "push_pct": _r(ev["push_pct"], 1),
                    "prev_push_pct": _r(ev["prev_push_pct"], 1), "reaction_pct": _r(ev["reaction_pct"], 1), "effort_x": _r(ev["effort_x"]), "why": ev["why"],
                    "status": "RECLAIMED" if C[-1] >= high else "BELOW" if C[-1] < C[i] else "RANGE"})
        tops = [m for m in dmarks if m[1] == "D-TOP" and abs(m[2] - ev["highI"]) <= 40]
        if tops:
            row["d_top_date"] = dates[tops[-1][2]]
        out["distribution"] = row
    if jump:
        for row in out.values():
            row["jump"] = jump
    return out


def one_ticker(s3, meta, recorded, harvest, bank_last_ord, listings, first_bar):
    """(status, rows, note): read, repair and scan one symbol."""
    raw = read_bars(s3, meta["ticker"])
    if not raw:
        return "unbanked", {}, None
    own, _ = T.own_history(raw, meta.get("ipo_date"))
    bars, ev = T.rebase(own, recorded or [])
    if meta["kind"] == "stock" and meta.get("mcap") and meta.get("sector"):
        bars, _ = T.separation_cut(bars, ev, meta["ticker"], meta["mcap"], meta["sector"], listings, first_bar)
    bars = clean(bars)
    if not bars:
        return "unreadable", {}, None
    if bank_last_ord is not None and bank_last_ord.get(bars[-1]["date"], -10 ** 6) < bank_last_ord["__last__"] - STALE_SESSIONS:
        return "stale", {}, bars[-1]["date"]
    if len(bars) < MIN_SESSIONS:
        return "short", {}, len(bars)
    return "scanned", rows_of(meta, bars, harvest, flagged_jump(T.jumps_of(bars, ev))), len(bars)


# --------------------------------------------------------------------------------------------------------------- build
def summarize(rows, key):
    out = {}
    for r in rows:
        k = r.get(key)
        out[k] = out.get(k, 0) + 1
    return out


def board_counts(rows):
    out = {"n": len(rows), "new_5": len([r for r in rows if r["ago"] <= 4])}
    for kind in ("stock", "etf"):
        sub = [r for r in rows if r["kind"] == kind]
        out[kind] = {"n": len(sub), "new_5": len([r for r in sub if r["ago"] <= 4]), "by_status": summarize(sub, "status"), "by_mark": summarize(sub, "mark"),
                     "mcap": _r(sum(r["mcap"] or 0 for r in sub), 0), "flagged_jump": len([r for r in sub if r.get("jump")])}
    return out


def changes_since(prev, boards, as_of):
    """Tickers that joined or left each board since the previous session's publication; the same session keeps the record."""
    if not isinstance(prev, dict) or not prev.get("boards"):
        return {"prev_as_of": None}
    if prev.get("as_of") == as_of:
        return prev.get("changes") or {"prev_as_of": None}
    out = {"prev_as_of": prev.get("as_of")}
    for b in BOARDS:
        before = {r.get("ticker"): r for r in (prev["boards"].get(b) or [])}
        now = {r["ticker"]: r for r in boards[b]}
        out[b] = {"new": sorted(t for t in now if t not in before), "gone": sorted(t for t in before if t not in now),
                  "moved": sorted(t for t in now if t in before and (before[t].get("mark"), before[t].get("status")) != (now[t]["mark"], now[t]["status"]))}
    return out


def build(s3, now, log=print, remaining_s=600.0):
    started = time.time()
    deadline = started + max(30.0, remaining_s - RESERVE_S)
    errors = []
    book = bank_enrich.rows(fields=BOOK_FIELDS)
    if not book:
        raise RuntimeError("the factor book is not readable: the universe cannot be drawn")
    universe, ustats = select_universe(book)
    if len(universe) < MIN_SCANNED:
        raise RuntimeError("only %d symbols from %d factor-book rows: the book is short; the previous publication stands" % (len(universe), len(book)))
    man, man_err = optional(s3, polygon_bankfirst.MANIFEST)
    if man_err or not (man or {}).get("last_session"):
        raise RuntimeError("the Polygon EOD bank's manifest is not readable (%s): the last session is unknown" % man_err)
    bank_last, bank_first = man["last_session"], man.get("first_session") or polygon_bankfirst.BANK_FIRST_SESSION
    bench = clean(read_bars(s3, "SPY"))
    if not bench or bench[-1]["date"] != bank_last:
        raise RuntimeError("SPY's last banked bar is %s, the manifest says %s: the bank is mid-fold" % (bench[-1]["date"] if bench else None, bank_last))
    order = {b["date"]: i for i, b in enumerate(bench)}
    order["__last__"] = len(bench) - 1
    recorded, n_windows = recorded_splits(s3, bank_first, bank_last, set(universe))
    hdoc, herr = optional(s3, BOTTOM_KEY)
    if herr:
        errors.append({"input": "bottom-desk", "error": herr})
    harvest = harvest_index(hdoc)
    listings = listings_of(universe)
    first_bars = {}

    def first_bar(t):
        if t not in first_bars:
            try:
                own, _ = T.own_history(read_bars(s3, t), (universe.get(t) or {}).get("ipo_date"))
                first_bars[t] = own[0]["date"] if own else None
            except Exception:  # noqa: BLE001
                first_bars[t] = None
        return first_bars[t]

    status, boards, notes = {}, {b: [] for b in BOARDS}, {"stale": [], "short": 0, "unbanked": 0, "unreadable": 0}
    scanned_by_kind, scanned_by_sector, sessions_read = {"stock": 0, "etf": 0}, {}, []

    def work(t):
        if time.time() > deadline:
            return t, "timeout", {}, None
        try:
            st, rows, note = one_ticker(s3, universe[t], recorded.get(t), harvest.get(t), order, listings, first_bar)
            return t, st, rows, note
        except Exception as exc:  # noqa: BLE001
            return t, "error", {}, "%s: %s" % (type(exc).__name__, str(exc)[:100])

    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        for t, st, rows, note in pool.map(work, sorted(universe)):
            status[st] = status.get(st, 0) + 1
            if st == "scanned":
                m = universe[t]
                scanned_by_kind[m["kind"]] += 1
                sessions_read.append(note)
                if m["kind"] == "stock":
                    sec = scanned_by_sector.setdefault(m["sector"] or "Unclassified", {"scanned": 0, "accumulation": 0, "bottom": 0, "distribution": 0})
                    sec["scanned"] += 1
                    for b in rows:
                        sec[b] += 1
                for b, row in rows.items():
                    boards[b].append(row)
            elif st == "error":
                errors.append({"ticker": t, "error": note})
            elif st == "stale":
                notes["stale"].append(t)
            elif st in notes:
                notes[st] += 1
    n_scanned = status.get("scanned", 0)
    if status.get("timeout"):
        raise RuntimeError("the scan ran out of time with %d of %d symbols unread: a partial board is not published; the previous one stands" % (status["timeout"], len(universe)))
    if n_scanned < MIN_SCANNED:
        raise RuntimeError("only %d of %d symbols scanned (%s): the previous publication stands" % (n_scanned, len(universe), json.dumps(status)))
    if status.get("error", 0) > MAX_ERROR_SHARE * len(universe):
        raise RuntimeError("%d symbols failed to scan: %s" % (status["error"], json.dumps(errors[:5])))
    for b in BOARDS:
        boards[b].sort(key=lambda r: (r["ago"], -(r["mcap"] or 0), r["ticker"]))
    window = max(BOTTOM_SESSIONS, ACCUM_SESSIONS, DIST_SESSIONS)
    recent = [x["date"] for x in bench[-window - 1:]]
    prints = []
    by_date = {b: summarize(boards[b], "date") for b in BOARDS}
    cl_date = summarize([r for r in boards["bottom"] if r["mark"] == "CLIMAX"], "date")
    for d in recent:
        prints.append({"date": d, "accumulation": by_date["accumulation"].get(d, 0), "climax": cl_date.get(d, 0), "bottom": by_date["bottom"].get(d, 0) - cl_date.get(d, 0),
                       "distribution": by_date["distribution"].get(d, 0)})
    prev, _ = optional(s3, OUT_KEY)
    out = {
        "engine": ENGINE, "version": VERSION, "contract": CONTRACT, "generated_at": now.isoformat(timespec="seconds"), "as_of": bank_last,
        "universe": {"book_rows": ustats["book_rows"], "eligible": len(universe), "eligible_stock": ustats["stock"], "eligible_etf": ustats["etf"], "left_out": ustats["left_out"],
                     "scanned": n_scanned, "scanned_stock": scanned_by_kind["stock"], "scanned_etf": scanned_by_kind["etf"], "short_history": notes["short"],
                     "not_trading": len(notes["stale"]), "unbanked": notes["unbanked"], "unreadable": notes["unreadable"], "min_dollar_vol": MIN_DV, "min_sessions": MIN_SESSIONS,
                     "bank_first_session": bank_first, "bank_sessions": len(bench), "median_sessions_read": sorted(sessions_read)[len(sessions_read) // 2] if sessions_read else None,
                     "split_windows_read": n_windows, "harvest_rows": len(harvest)},
        "windows": {"climax_sessions": CLIMAX_SESSIONS, "bottom_sessions": BOTTOM_SESSIONS, "accumulation_sessions": ACCUM_SESSIONS, "distribution_sessions": DIST_SESSIONS},
        "counts": {b: board_counts(boards[b]) for b in BOARDS},
        "by_sector": dict(sorted(scanned_by_sector.items(), key=lambda kv: -kv[1]["scanned"])),
        "prints": prints,
        "boards": boards,
        "definitions": DEFINITIONS, "limitations": LIMITATIONS, "sources": SOURCES,
        "n_errors": len(errors), "errors": errors[:40],
    }
    out["changes"] = changes_since(prev, boards, bank_last)
    out["run_seconds"] = round(time.time() - started, 1)
    log("campaign-scan: %d scanned of %d eligible (%s) in %.1fs" % (n_scanned, len(universe), json.dumps(status), out["run_seconds"]))
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
    c = out["counts"]
    print("campaign-scan %s: accumulation %d, bottom %d, distribution %d of %d scanned; %d bytes, %.1fs, errors %d" % (
        out["as_of"], c["accumulation"]["n"], c["bottom"]["n"], c["distribution"]["n"], out["universe"]["scanned"], n, out["run_seconds"], out["n_errors"]))
    return {"statusCode": 200, "key": OUT_KEY, "bytes": n, "as_of": out["as_of"], "scanned": out["universe"]["scanned"], "accumulation": c["accumulation"]["n"],
            "bottom": c["bottom"]["n"], "distribution": c["distribution"]["n"], "run_seconds": out["run_seconds"], "errors": out["n_errors"]}
