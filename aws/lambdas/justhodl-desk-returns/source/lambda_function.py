"""
justhodl-desk-returns -- realized daily mark-to-market return feed for the
seven strategy desks.
=====================================================================
WHY THIS EXISTS
---------------
The Desk Allocator sizes the seven strategy desks by inverse-volatility
risk parity. Volatility is the input that decides the split -- yet the
allocator has been running on archetype PRIORS alone, because no desk has
a realized return history. Its shrink(prior, realized, N) machinery and
its realized_desk_vol() reader were shipped dormant: a documented Phase-2
hook waiting for a return feed.

This engine is that feed. It is the performance-measurement function a
real multi-manager shop runs next to, but separate from, the capital
allocator: the allocator decides the split, this proves how each desk
actually did.

THE METHOD -- honest daily mark-to-market
-----------------------------------------
Every trading day, after the close:

  1. Build each desk's book from its own sidecar JSON, as a signed-weight
     map {symbol: weight}. Long-only desks carry positive weights summing
     to gross 1; the defensive short desk carries negatives; the trend and
     index-reconstitution desks carry both; the pairs desk carries each
     leg of every pair (+long, -short). Each book is capped to its
     headline names so the return reflects the desk's real thesis, not a
     long tail.
  2. Quote every symbol in the union of today's books and yesterday's
     books on FMP /stable/quote.
  3. For each desk, mark YESTERDAY's book to today's close:
        desk_return = sum( w_i * (px_today_i / px_prev_i - 1) )
     renormalised over the names that resolved, so a few missing quotes
     do not bias the number. That is the return of the book held overnight
     into today -- a true one-trading-day return.
  4. Append it to the desk's return series, then refresh the stored book
     and prices for tomorrow.

GUARDS
  * A single-name one-day move beyond +/-60% is dropped (stock split or
    data error, not a real return).
  * If the run cadence slipped (gap > 4 calendar days) the observation is
    skipped rather than logged as a polluted multi-day return.
  * If no name in any book moved, the day is a market holiday and nothing
    is appended.
  * A stale desk sidecar does not stop the mark -- the last known book is
    assumed held and still marked to market.

The series is daily, so annualising realized vol with sqrt(252) -- which
is exactly what the allocator's realized_desk_vol() does -- is correct.
Once a desk clears 20 observations the allocator stops being prior-only
and the Bayesian shrinkage warms in the real number.

OUTPUT    data/desk-returns.json        SCHEDULE  weekdays 23:30 UTC
HONESTY   Hypothetical equal-/signal-weight marks. No transaction costs,
          no slippage, no borrow. Research and education only.
"""
import concurrent.futures as cf
import json
import math
import os
import ssl
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/desk-returns.json"
SCHEMA = "1.0"

FMP_KEY = os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP = "https://financialmodelingprep.com/stable"

RETURNS_CAP = 400          # daily observations retained per desk
MAX_PER_DESK = 40          # cap a desk book to its top-N headline names
MAX_PAIRS = 30             # cap the pairs desk to its top-N pairs
DAY_MOVE_GUARD = 0.60      # drop any single-name 1-day move beyond +/-60%
MAX_GAP_DAYS = 4           # skip the observation if the run cadence slipped
MIN_N_FOR_VOL = 20         # observations before realized vol is reported

s3 = boto3.client("s3", region_name="us-east-1")
_SSL = ssl.create_default_context()


# ---- s3 helpers ------------------------------------------------------------
def get_json(key):
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(raw)
    except Exception:
        return None


def norm(sym):
    return (sym or "").upper().strip()


# ---- per-desk book builders ------------------------------------------------
# Each builder turns a desk's sidecar JSON into a signed-weight book
# {symbol: weight}; absolute weights sum to gross 1.0.
def book_long_only(doc, arrays, sym_field, cap):
    syms = []
    for ak in arrays:
        for it in (doc.get(ak) or []):
            if isinstance(it, dict):
                s = norm(it.get(sym_field))
                if s and s not in syms:
                    syms.append(s)
    syms = syms[:cap]
    if not syms:
        return {}
    w = 1.0 / len(syms)
    return {s: w for s in syms}


def book_short_only(doc, arrays, sym_field, cap):
    return {s: -w for s, w in
            book_long_only(doc, arrays, sym_field, cap).items()}


def book_trend(doc, cap):
    longs, shorts = [], []
    for p in (doc.get("positions") or []):
        if not isinstance(p, dict):
            continue
        s = norm(p.get("symbol"))
        d = p.get("direction")
        if not s:
            continue
        if d == "LONG" and s not in longs:
            longs.append(s)
        elif d == "SHORT" and s not in shorts:
            shorts.append(s)
    longs, shorts = longs[:cap], shorts[:cap]
    n = len(longs) + len(shorts)
    if n == 0:
        return {}
    w = 1.0 / n
    book = {s: w for s in longs}
    for s in shorts:
        book[s] = book.get(s, 0.0) - w
    return book


def book_index_recon(doc, cap):
    longs = [norm(x.get("symbol")) for x in
             (doc.get("russell_2000_additions") or [])
             if isinstance(x, dict)]
    shorts = [norm(x.get("symbol")) for x in
              (doc.get("russell_2000_deletions") or [])
              if isinstance(x, dict)]
    longs = [s for s in dict.fromkeys(longs) if s][:cap]
    shorts = [s for s in dict.fromkeys(shorts) if s][:cap]
    n = len(longs) + len(shorts)
    if n == 0:
        return {}
    w = 1.0 / n
    book = {s: w for s in longs}
    for s in shorts:
        book[s] = book.get(s, 0.0) - w
    return book


def book_pairs(doc, cap_pairs):
    pairs = [p for p in (doc.get("pairs") or []) if isinstance(p, dict)]
    pairs = pairs[:cap_pairs]
    if not pairs:
        return {}
    per = 1.0 / (2 * len(pairs))      # gross 1 across every leg
    book = {}
    for p in pairs:
        a = norm((p.get("long_leg") or {}).get("symbol"))
        b = norm((p.get("short_leg") or {}).get("symbol"))
        if a:
            book[a] = book.get(a, 0.0) + per
        if b:
            book[b] = book.get(b, 0.0) - per
    return book


# desk registry: key -> (sidecar json key, book builder)
DESKS = {
    "best-ideas":   ("data/best-ideas.json",
                     lambda d: book_long_only(d, ["stack"], "symbol",
                                              MAX_PER_DESK)),
    "pairs-arb":    ("data/pairs-arb.json",
                     lambda d: book_pairs(d, MAX_PAIRS)),
    "trend-engine": ("data/trend-engine.json",
                     lambda d: book_trend(d, MAX_PER_DESK)),
    "merger-arb":   ("data/merger-arb.json",
                     lambda d: book_long_only(d, ["all_priced"], "target",
                                              MAX_PER_DESK)),
    "spinoff-desk": ("data/spinoff-desk.json",
                     lambda d: book_long_only(d, ["top_setups"], "symbol",
                                              MAX_PER_DESK)),
    "index-recon":  ("data/index-recon.json",
                     lambda d: book_index_recon(d, MAX_PER_DESK)),
    "risk-radar":   ("data/risk-radar.json",
                     lambda d: book_short_only(d, ["stack"], "symbol",
                                               MAX_PER_DESK)),
}
DESK_ORDER = ["best-ideas", "pairs-arb", "trend-engine", "merger-arb",
              "spinoff-desk", "index-recon", "risk-radar"]


# ---- price fetch -----------------------------------------------------------
def fetch_price(sym):
    """Current price for one symbol off FMP /stable/quote."""
    url = (f"{FMP}/quote?symbol={urllib.parse.quote(sym)}"
           f"&apikey={FMP_KEY}")
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "justhodl-desk-returns"})
        with urllib.request.urlopen(req, timeout=20, context=_SSL) as r:
            data = json.loads(r.read().decode("utf-8"))
        if isinstance(data, list) and data:
            px = data[0].get("price")
            if isinstance(px, (int, float)) and px > 0:
                return sym, float(px)
    except Exception:
        pass
    return sym, None


# ---- return + vol math -----------------------------------------------------
def desk_return(prev_book, prev_px, today_px):
    """One-trading-day MTM return of a book, renormalised over resolved
    names. Returns (ret, n_resolved) or (None, 0)."""
    num = 0.0
    gross = 0.0
    n = 0
    for sym, w in prev_book.items():
        p0 = prev_px.get(sym)
        p1 = today_px.get(sym)
        if not p0 or not p1 or p0 <= 0:
            continue
        r = p1 / p0 - 1.0
        if abs(r) > DAY_MOVE_GUARD:        # split / data-error guard
            continue
        num += w * r
        gross += abs(w)
        n += 1
    if gross <= 0:
        return None, 0
    return num / gross, n


def ann_vol(returns):
    rets = [r["ret"] for r in returns
            if isinstance(r.get("ret"), (int, float))]
    if len(rets) < MIN_N_FOR_VOL:
        return None
    mean = sum(rets) / len(rets)
    var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)
    return math.sqrt(var) * math.sqrt(252.0)


def mean_ret(returns):
    rets = [r["ret"] for r in returns
            if isinstance(r.get("ret"), (int, float))]
    return sum(rets) / len(rets) if rets else None


# ---- handler ---------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()

    state = get_json(OUT_KEY) or {"schema": SCHEMA, "desks": {}}
    desks_state = state.setdefault("desks", {})

    # 1) build today's book per desk
    today_books = {}
    book_fresh = {}
    for key in DESK_ORDER:
        jk, builder = DESKS[key]
        doc = get_json(jk)
        b = {}
        if isinstance(doc, dict):
            try:
                b = builder(doc) or {}
            except Exception:
                b = {}
        today_books[key] = b
        book_fresh[key] = bool(b)

    # 2) union of every symbol in today's books + every stored prev book
    universe = set()
    for b in today_books.values():
        universe |= set(b)
    for st in desks_state.values():
        universe |= set((st.get("prev_book") or {}).keys())
    universe = sorted(universe)

    # 3) quote the universe
    today_px = {}
    if universe:
        with cf.ThreadPoolExecutor(max_workers=12) as ex:
            for sym, px in ex.map(fetch_price, universe):
                if px is not None:
                    today_px[sym] = px

    # 4) per-desk: mark yesterday's book, then refresh
    desk_out = []
    appended_any = False
    for key in DESK_ORDER:
        st = desks_state.setdefault(
            key, {"prev_book": {}, "prev_px": {}, "prev_date": "",
                  "returns": []})
        if not isinstance(st.get("returns"), list):
            st["returns"] = []
        prev_book = st.get("prev_book") or {}
        prev_px = st.get("prev_px") or {}
        prev_date = st.get("prev_date") or ""

        appended = False
        skip_reason = ""
        ret = None
        n_res = 0
        gap = None
        if prev_book and prev_date and prev_date != today:
            try:
                gap = (date.fromisoformat(today)
                       - date.fromisoformat(prev_date)).days
            except Exception:
                gap = None
            if gap is not None and 1 <= gap <= MAX_GAP_DAYS:
                ret, n_res = desk_return(prev_book, prev_px, today_px)
                moved = any(
                    abs(today_px.get(s, 0.0) - prev_px.get(s, 0.0)) > 1e-9
                    for s in prev_book
                    if s in today_px and s in prev_px)
                if ret is not None and n_res > 0 and moved:
                    st["returns"].append({
                        "date": today, "ret": round(ret, 6),
                        "n": n_res, "gap_days": gap})
                    st["returns"] = st["returns"][-RETURNS_CAP:]
                    appended = True
                    appended_any = True
                elif not moved:
                    skip_reason = "no price movement (market holiday)"
                else:
                    skip_reason = "no resolvable positions"
            elif gap is not None:
                skip_reason = "run-cadence gap %dd > %dd" % (
                    gap, MAX_GAP_DAYS)
        elif not prev_book:
            skip_reason = "first observation - seeding book"

        # refresh the stored book (keep last known if the desk is stale)
        tb = today_books.get(key) or {}
        if tb:
            st["prev_book"] = tb
        book_for_px = st.get("prev_book") or {}
        st["prev_px"] = {s: today_px[s] for s in book_for_px
                         if s in today_px}
        st["prev_date"] = today

        rv = ann_vol(st["returns"])
        desk_out.append({
            "key": key,
            "book_names": len(st.get("prev_book") or {}),
            "book_fresh": book_fresh[key],
            "appended_today": appended,
            "skip_reason": skip_reason,
            "today_return": round(ret, 6) if (appended and ret is not None)
            else None,
            "names_marked": n_res if appended else 0,
            "n_returns": len(st["returns"]),
            "realized_vol_annualized": round(rv, 4) if rv is not None
            else None,
            "mean_daily_return": (round(mean_ret(st["returns"]), 6)
                                  if st["returns"] else None),
            "vol_ready": len(st["returns"]) >= MIN_N_FOR_VOL,
            "last_return": (st["returns"][-1] if st["returns"] else None),
        })

    ready = sum(1 for d in desk_out if d["vol_ready"])
    fresh = sum(1 for d in desk_out if d["book_fresh"])
    if appended_any:
        headline = (
            "Marked %d/7 desks to today's close. %d desk(s) have cleared "
            "the 20-observation threshold and now feed a realized "
            "volatility into the Desk Allocator; the rest are still warming "
            "up on archetype priors." % (fresh, ready))
    else:
        headline = (
            "No desk return logged this run (%s). %d/7 desk books are "
            "fresh; %d desk(s) vol-ready."
            % (desk_out[0]["skip_reason"] or "seeding", fresh, ready))

    state["schema"] = SCHEMA
    state["engine"] = "justhodl-desk-returns"
    state["generated_at"] = now.isoformat()
    state["trading_date"] = today
    state["build_seconds"] = round(time.time() - t0, 2)
    state["universe_size"] = len(universe)
    state["prices_resolved"] = len(today_px)
    state["headline"] = headline
    state["desk_summary"] = desk_out
    state["parameters"] = {
        "returns_cap": RETURNS_CAP,
        "max_names_per_desk": MAX_PER_DESK,
        "max_pairs": MAX_PAIRS,
        "day_move_guard_pct": DAY_MOVE_GUARD * 100,
        "max_gap_days": MAX_GAP_DAYS,
        "min_observations_for_vol": MIN_N_FOR_VOL,
    }
    state["methodology"] = (
        "Each desk book is read from its sidecar as a signed-weight map "
        "(gross 1.0), capped to its headline names. Every trading day the "
        "prior day's book is marked to today's FMP /stable/quote close: "
        "desk return = sum(weight * one-day symbol return), renormalised "
        "over resolved names. Single-name moves beyond +/-60% are dropped "
        "as splits/errors; cadence gaps over 4 days and zero-movement "
        "holidays are skipped. The series is daily, so realized vol "
        "annualises with sqrt(252) - the exact input the Desk Allocator's "
        "Bayesian shrinkage blends in once a desk clears 20 observations.")
    state["disclaimer"] = (
        "Hypothetical equal-/signal-weight marks - no transaction costs, "
        "no slippage, no borrow cost. Research and education only, not "
        "investment advice or a record of actual trading.")

    body = json.dumps(state, default=str).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="max-age=300")

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "trading_date": today,
        "universe": len(universe), "prices_resolved": len(today_px),
        "appended_any": appended_any, "desks_fresh": fresh,
        "vol_ready": ready})}
