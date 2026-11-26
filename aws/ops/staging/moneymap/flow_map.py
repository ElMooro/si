"""The money map: where money went in and out, for every ETF the warehouse holds (Khalid, 2026-11-25).

"Enhance that engine and tracks what etfs and sectors is the money flowing out and flowing into and covers as many asset
classes and etfs as possible."

The research view beside this module (flow_desk_research, document data/global-flow-desk.json) reads 96 funds from the
issuers' own files and is left exactly as it was. This module adds a second document, built in the same run from the
warehouse only (no provider is asked here):

  data/warm/polygon-bank/etf-global-fund-flows-by-date/<YYYY-MM>.json.gz   every fund's row for every day of a month
  data/warm/polygon-bank/etf-global-fund-flows/<FUND>.json.gz             each fund's long history (older days)
  data/warm/polygon-bank/etf-global-profiles/<FUND>.json.gz               the provider's class, category, focus, weights
  data/etf-creations/<FUND>.json                                          the issuers' own share counts (a check only)

What a figure is.  A fund's flow on a day is its share count's own path: (shares outstanding minus shares on the row
before) times the NAV of the row before.  On rows that follow each other day by day this is the provider's creation /
redemption figure to the dollar (351,572 of 351,576 rows over 63 days, ops 6965); the provider's figure itself is not
used because after a missing day it refers to rows the warehouse does not have (502 of 1,123 such rows were wrong, by
$14.3 billion: a November buffer series printed -85% with its share count unchanged).  Four things are done to the
counts, each from evidence and each counted in the document:

  a share split      the warehouse's record of splits (polygon-bank/splits), found in the counts within six days, or a
                     whole-ratio move of count and NAV in opposite directions on the same or the next row; the count and
                     the NAV before it are restated on the new basis (Vanguard's five April 2026 splits alone printed as
                     +$401 billion of inflow, the NAV having been restated a day before the count);
  a misprint         a row whose count and NAV both leave and both come back on the next row is dropped;
  a round trip       a count that jumps by more than 40% and is back within two rows (a heartbeat trade, a reset day, a
                     doubled print) counts as the difference it left behind, not as money in and then money out;
  a late fund        a fund whose newest row is more than three reporting days old is left out of every total and listed.

A jump that stays is money and is counted, however large: the Alger Concentrated Equity ETF really did take in about
$7 billion from August to October 2026 and lose most of it in November.  Nothing is estimated or carried forward.

Groups never overlap inside one table: each fund has one segment, one provider class, one category.  Funds that use
leverage or run inverse are kept in their own segment so a bet against the market is never netted against a purchase.
The look-through sector table is the one exception by design: a fund's flow is split by the provider's sector weights.
"""
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gzip
import json
import math
import time

VERSION = "3.0.0"
CONTRACT = "global-flow-map.v1"
OUT = "data/global-flow-map.json"
OUT_FUNDS = "data/global-flow-map-funds.json"
OUT_HISTORY = "data/global-flow-map-history.json"
OUT_SHARDS = "data/global-flow-map/funds/"
BANK = "data/warm/polygon-bank/"
BY_DATE = BANK + "etf-global-fund-flows-by-date/"
PER_FUND = BANK + "etf-global-fund-flows/"
PROFILES = BANK + "etf-global-profiles/"
ISSUER_INDEX = "data/etf-creations/index.json"
ISSUER_FEED = "data/etf-creations/%s.json"

WINDOWS = (("d1", "1D", 1), ("w1", "1W", 5), ("m1", "1M", 21), ("m3", "3M", 63), ("ytd", "YTD", None), ("y1", "1Y", 252))
KEEP_DAYS = 300                 # reporting days kept per fund (a year of windows plus the comparison history)
LATE_DAYS = 3                   # a fund whose newest row is older than this many reporting days is left out of totals
SHARD_DAYS = 130                # days of each fund's own history published for its sheet
MONTHS_READ = 4                 # by-date months read each run: they bring every fund's long file up to the newest day
HISTORY_DAYS = 260
MIN_FUNDS = 2500                # fewer current funds than this and nothing is published
MAX_AGE_DAYS = 6                # the newest reporting day may be at most this many calendar days old
BOARD = 40
NO_CLASS = "No class given"                # funds the provider has not classed: kept in the class table so it adds up to the total
JUMP = math.log(1.4)            # a share count that moves by more than 40% between two rows
TRIP_ROWS = 2                   # a jump undone within this many rows is a round trip ...
TRIP_LEFT = 0.2                 # ... when no more than a fifth of it is left
SPLIT_DAYS = 6                  # a recorded split is looked for in the counts within this many calendar days of its date
SPLITS = BANK + "splits/"       # the warehouse's record of splits: <YYYY-MM>.json.gz, rows ticker / execution_date / split_from / split_to
_WHOLE = (2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30, 40, 50, 100)
RATIO_NAMES = dict([(float(n), "%d-for-1" % n) for n in _WHOLE] + [(1.0 / n, "1-for-%d" % n) for n in _WHOLE]
                   + [(1.5, "3-for-2"), (2.5, "5-for-2"), (1.25, "5-for-4"), (2.0 / 3, "2-for-3"), (0.4, "2-for-5"), (0.8, "4-for-5")])
RATIOS = sorted(RATIO_NAMES)
SECTOR_NAMES = {"technology": "Technology", "financials": "Financials", "health_care": "Health care", "consumer_discretionary": "Consumer discretionary",
                "communications": "Communications", "industrials": "Industrials", "consumer_staples": "Consumer staples", "energy": "Energy",
                "utilities": "Utilities", "real_estate": "Real estate", "materials": "Materials"}
FOCUS_SECTOR = {"natural resources": "Natural resources", "information technology": "Technology", "technology": "Technology", "financials": "Financials", "health care": "Health care",
                "consumer discretionary": "Consumer discretionary", "communication services": "Communications", "communications": "Communications",
                "telecommunication services": "Communications", "industrials": "Industrials", "consumer staples": "Consumer staples", "energy": "Energy",
                "utilities": "Utilities", "real estate": "Real estate", "materials": "Materials"}
COUNTRIES = {"us": "United States", "jp": "Japan", "cn": "China", "hk": "Hong Kong", "tw": "Taiwan", "kr": "South Korea", "in": "India", "br": "Brazil", "mx": "Mexico",
             "ca": "Canada", "gb": "United Kingdom", "de": "Germany", "fr": "France", "it": "Italy", "es": "Spain", "ch": "Switzerland", "nl": "Netherlands", "se": "Sweden",
             "au": "Australia", "sg": "Singapore", "za": "South Africa", "tr": "Turkey", "pl": "Poland", "id": "Indonesia", "th": "Thailand", "vn": "Vietnam",
             "my": "Malaysia", "ph": "Philippines", "sa": "Saudi Arabia", "ae": "United Arab Emirates", "il": "Israel", "ar": "Argentina", "cl": "Chile", "co": "Colombia",
             "pe": "Peru", "gr": "Greece", "ie": "Ireland", "dk": "Denmark", "no": "Norway", "fi": "Finland", "be": "Belgium", "at": "Austria", "pt": "Portugal",
             "nz": "New Zealand", "eg": "Egypt", "qa": "Qatar", "kw": "Kuwait", "ng": "Nigeria", "pk": "Pakistan"}
CHINA_PLACES = ("cn", "hk", "ky", "bm")      # Chinese companies are registered in the Cayman Islands and Bermuda and listed in Hong Kong
SHORT_BUCKETS, LONG_BUCKETS = ("0-1", "1-3"), ("10-15", "15-20", "20-30", "30+")


def num(v):
    try:
        f = float(v)
        return f if f == f and abs(f) != float("inf") else None
    except (TypeError, ValueError):
        return None


def day(v):
    s = str(v or "")[:10]
    return s if len(s) == 10 and s[4] == "-" and s[7] == "-" else None


# --------------------------------------------------------------------------------------------- classes
def classify(profile):
    """One segment, the provider's own three levels, and the cuts read from its weights.  Rules only; nothing guessed."""
    p = profile or {}
    ac, cat, foc = p.get("asset_class") or "", p.get("category") or "", p.get("focus") or ""
    region, dev = p.get("region") or "", p.get("development_class") or ""
    lev = num(p.get("levered_amount")) or 0.0
    leveraged = (p.get("leverage_style") == "leveraged") or lev != 0.0
    geo = p.get("geographic_exposure") if isinstance(p.get("geographic_exposure"), dict) else {}
    top = max(geo.items(), key=lambda kv: num(kv[1]) or 0) if geo else (None, 0)
    us = num(geo.get("us")) or 0.0
    out = {"ac": ac or None, "cat": cat or None, "foc": foc or None, "reg": region or None, "lev": lev if leveraged else 0.0,
           "sec": None, "cty": None, "mat": None, "sub": None}
    if not p:
        out["seg"] = "Unclassified"
        return out
    if leveraged:
        out["seg"] = "Leveraged & inverse"
        out["sub"] = ("Inverse" if lev < 0 else "Leveraged long") + " · " + (ac or "other")
        return out
    if ac in ("Equity", "Real Estate") and cat == "Sector":
        out["sec"] = FOCUS_SECTOR.get(foc.lower())               # a "sector" fund whose focus is not a sector is in no sector row
    if ac == "Real Estate":
        out["seg"] = "Real estate"
    elif ac == "Equity":
        lead, share = str(top[0]).lower(), num(top[1]) or 0.0
        china = sum(num(geo.get(k)) or 0.0 for k in CHINA_PLACES)
        if share >= 0.6 and lead not in ("us", "ky", "bm"):
            out["cty"] = COUNTRIES.get(lead, lead.upper())
        elif china >= 0.6 and region == "Asia-Pacific":
            out["cty"] = "China"                                    # mainland, Hong Kong and offshore registrations together
        if cat == "Sector":
            out["seg"] = "Equity · sector funds"
        elif foc == "Single Stock":
            out["seg"] = "Equity · single-stock funds"
        elif us >= 0.7 or (region == "North America" and us >= 0.5) or (not geo and region == "North America"):
            out["seg"] = "Equity · United States"
        elif dev == "Emerging Markets" or region in ("Emerging Markets", "Latin America", "South America", "Middle East & Africa"):
            out["seg"] = "Equity · emerging markets"
        elif region == "Global" and us >= 0.3:
            out["seg"] = "Equity · global"
        else:
            out["seg"] = "Equity · international developed"
        out["sub"] = foc or cat or None
    elif ac == "Fixed Income":
        mat = p.get("maturity_exposure") if isinstance(p.get("maturity_exposure"), dict) else {}
        tot = sum(max(num(v) or 0, 0) for v in mat.values())
        if tot > 0:
            short = sum(max(num(mat.get(k)) or 0, 0) for k in SHORT_BUCKETS) / tot
            long_ = sum(max(num(mat.get(k)) or 0, 0) for k in LONG_BUCKETS) / tot
            out["mat"] = "Short (under 3 years)" if short >= 0.5 else "Long (10 years and over)" if long_ >= 0.5 else "Intermediate (3 to 10 years)" if short + long_ <= 0.5 else "Mixed"
        if cat == "U.S. Government":
            out["seg"] = "Bonds · TIPS" if foc == "TIPS" else "Bonds · Treasuries & agencies"
        elif cat == "Corporate":
            out["seg"] = "Bonds · high yield" if foc == "High Yield" else "Bonds · corporate"
        elif cat == "Municipals":
            out["seg"] = "Bonds · municipal"
        elif cat == "Sovereign" or region not in ("North America", ""):
            out["seg"] = "Bonds · international & emerging"
        else:
            out["seg"] = "Bonds · broad & other"
        out["sub"] = foc or cat or None
    elif ac == "Commodities":
        out["seg"] = "Commodities · precious metals" if cat == "Precious Metals" else "Commodities · energy" if cat == "Energy" else "Commodities · broad & other"
        out["sub"] = foc or cat or None
    elif ac == "Currency":
        if cat == "Alternative Currency" or foc == "Cryptocurrency":
            out["seg"] = "Crypto"
            cur = p.get("currency_exposure") if isinstance(p.get("currency_exposure"), dict) else {}
            lead = max(cur.items(), key=lambda kv: num(kv[1]) or 0)[0] if cur else None
            out["sub"] = {"btc": "Bitcoin", "eth": "Ether", "sol": "Solana", "xrp": "XRP"}.get(str(lead).lower(), "Other and baskets")
        else:
            out["seg"] = "Currencies"
            out["sub"] = foc or cat or None
    elif ac == "Multi Asset":
        out["seg"] = "Volatility" if foc == "Volatility" else "Multi-asset & alternatives"
        out["sub"] = foc or cat or None
    else:
        out["seg"] = "Unclassified"
    return out


def sector_weights(profile):
    """The provider's sector weights of an equity fund, cash and derivatives left out, scaled to at most one."""
    w = (profile or {}).get("sector_exposure")
    if not isinstance(w, dict):
        return {}
    got = {SECTOR_NAMES[k]: max(num(v) or 0.0, 0.0) for k, v in w.items() if k in SECTOR_NAMES}
    tot = sum(got.values())
    if tot <= 0:
        return {}
    scale = 1.0 / tot if tot > 1.0 else 1.0
    return {k: v * scale for k, v in got.items() if v > 0}


# --------------------------------------------------------------------------------------------- one fund's days
def ratio_name(c):
    """A split ratio as funds state it: 8-for-1, 1-for-10, 3-for-2."""
    best = min(RATIOS, key=lambda r: abs(math.log(c / r)))
    if abs(math.log(c / best)) < 0.01:
        return RATIO_NAMES[best]
    return ("%.4g-for-1" % c) if c >= 1 else ("1-for-%.4g" % (1.0 / c))


def _apart(a, b):
    return abs((datetime.strptime(a, "%Y-%m-%d") - datetime.strptime(b, "%Y-%m-%d")).days)


def _nav_leg(rows, i, lc, tol, span=2):
    """The row on which the NAV moved by the other side of a split whose count moved on row i (the same row or a
    neighbour: the provider restates the two on different days); None when the NAV series never moved."""
    for k in [i] + [x for step in range(1, span + 1) for x in (i - step, i + step)]:
        if 1 <= k < len(rows) and abs(math.log(rows[k][2] / rows[k - 1][2]) + lc) < tol:
            return k
    return None


def count_path(rows, recorded=None):
    """One fund's days as money.  rows: [(date, shares, nav, provider figure)] oldest first; recorded: [(date, ratio)]
    from the warehouse's record of splits (ratio = new shares per old share).

    Returns (clean, notes).  clean: [(date, shares, nav, flow, flag)] with shares and NAV on today's basis and the flow
    of a row = (shares - shares of the row before) x NAV of the row before.  flag: None | 'split a-for-b' | 'round trip'
    | 'jump'.  notes: [{d, k, x, usd}] for what was done (k: split | misprint | trip | jump | unseen)."""
    rows = [r for r in rows if r[1] and r[2] and r[1] > 0 and r[2] > 0]
    notes = []
    # 1. a row whose count and NAV both leave and both come back on the next row is another fund's row or a slip
    drop = set()
    for i in range(1, len(rows) - 1):
        if i - 1 in drop:
            continue
        (_, s0, n0, _), (d, s1, n1, _), (_, s2, n2, _) = rows[i - 1], rows[i], rows[i + 1]
        if abs(math.log(s1 / s0)) > JUMP and abs(math.log(s2 / s0)) < 0.1 and abs(math.log(n1 / n0)) > 0.3 and abs(math.log(n2 / n0)) < 0.15:
            drop.add(i)
            notes.append({"d": d, "k": "misprint", "x": "row dropped: count x%.3g and NAV x%.3g for one row" % (s1 / s0, n1 / n0), "usd": abs(s1 - s0) * n0})
    if drop:
        rows = [r for j, r in enumerate(rows) if j not in drop]
    n = len(rows)
    if not n:
        return [], notes
    # 2. splits: the record first, then whole-ratio moves of count and NAV in opposite directions
    events, used = [], set()
    for dr, c in sorted(recorded or ()):
        if not c or c <= 0 or abs(math.log(c)) < 0.05 or not (rows[0][0] < dr <= rows[-1][0] or _apart(dr, rows[-1][0]) <= SPLIT_DAYS):
            continue
        lc = math.log(c)
        tol_s, tol_n = min(0.2, 0.5 * abs(lc)), min(0.25, 0.5 * abs(lc))
        best = None
        for i in range(1, n):
            if i in used or _apart(rows[i][0], dr) > SPLIT_DAYS:
                continue
            dev = abs(math.log(rows[i][1] / rows[i - 1][1]) - lc)
            k = _nav_leg(rows, i, lc, tol_n)
            if dev < tol_s or (k is not None and dev < min(0.4, 0.5 * abs(lc))):
                rank = (_apart(rows[i][0], dr), dev)
                if best is None or rank < best[0]:
                    best = (rank, i, k)
        if best is None:
            notes.append({"d": dr, "k": "unseen", "x": "recorded split %s not found in the share counts" % ratio_name(c), "usd": None})
            continue
        events.append((best[1], best[2], c, True))
        used.add(best[1])
    for i in range(1, n):
        if i in used:
            continue
        ls = math.log(rows[i][1] / rows[i - 1][1])
        if abs(ls) < 0.55:
            continue
        r = math.exp(ls)
        c = float(round(r)) if r > 1 else 1.0 / round(1.0 / r)
        lc = math.log(c)
        if abs(ls - lc) > 0.15:
            continue
        k = _nav_leg(rows, i, lc, 0.2, span=1)
        if k is None:
            continue
        events.append((i, k, c, False))
        used.add(i)
    shares, nav, flag = [r[1] for r in rows], [r[2] for r in rows], [None] * n
    for i, k, c, on_record in events:
        for j in range(i):
            shares[j] *= c
        for j in range(k if k is not None else 0):
            nav[j] /= c
        flag[i] = "split " + ratio_name(c)
        notes.append({"d": rows[i][0], "k": "split", "x": flag[i] + (" (on record)" if on_record else " (read from count and NAV)"), "usd": abs(rows[i][1] - rows[i - 1][1]) * rows[i - 1][2]})
    # 3. round trips: a jump that is back within TRIP_ROWS rows counts as what it left behind
    i = 1
    while i < n:
        if flag[i] is None and abs(math.log(shares[i] / shares[i - 1])) > JUMP:
            base, jump = shares[i - 1], shares[i] - shares[i - 1]
            hit = next((j for j in range(i + 1, min(i + 1 + TRIP_ROWS, n)) if flag[j] is None and abs(shares[j] - base) <= TRIP_LEFT * abs(jump)), None)
            if hit is not None:
                notes.append({"d": rows[i][0], "k": "trip", "x": "count x%.3g and back by %s" % (shares[i] / base, rows[hit][0]), "usd": abs(jump) * nav[i - 1]})
                for j in range(i, hit):
                    shares[j] = base
                    flag[j] = "round trip"
                flag[hit] = "round trip"
                i = hit + 1
                continue
            flag[i] = "jump"
            notes.append({"d": rows[i][0], "k": "jump", "x": "count x%.3g, not undone" % (shares[i] / base), "usd": jump * nav[i - 1]})
        i += 1
    clean = [(rows[0][0], shares[0], nav[0], rows[0][3], flag[0])]          # the first row has no row before it: the provider's figure stands
    for i in range(1, n):
        clean.append((rows[i][0], shares[i], nav[i], (shares[i] - shares[i - 1]) * nav[i - 1], flag[i]))
    return clean, notes


def clean_series(rows, recorded=None):
    return count_path(rows, recorded)[0]


def compact(x):
    """A provider row as (effective day, processed day, shares, NAV, flow); None when it has no day."""
    d = day(x.get("effective_date"))
    return (d, str(x.get("processed_date") or ""), num(x.get("shares_outstanding")), num(x.get("nav")), num(x.get("fund_flow"))) if d else None


def merge_rows(*sources):
    """Compact rows of one fund from several files -> one per effective day (the latest processed wins), oldest first."""
    best = {}
    for rows in sources:
        for x in rows or ():
            if x and (x[0] not in best or x[1] >= best[x[0]][1]):
                best[x[0]] = x
    return [(d, v[2], v[3], v[4]) for d, v in sorted(best.items())]


# --------------------------------------------------------------------------------------------- the build
def calendar(series):
    """Reporting days: days on which at least half as many funds reported as on a normal day."""
    count = defaultdict(int)
    for rows in series.values():
        for r in rows:
            count[r[0]] += 1
    if not count:
        return []
    days = sorted(count)
    recent = sorted(count[d] for d in days[-40:])
    norm = recent[len(recent) // 2]
    return [d for d in days if count[d] >= 0.5 * norm]


def money(v):
    return None if v is None else round(v, 2)


def build(series, profiles, issuer=None, now=None, by_date_months=None, splits=None):
    """series: {fund: [(date, shares, nav, provider figure)] oldest first}; profiles: {fund: provider profile};
    issuer: {fund: {date: shares}}; splits: {fund: [(date, ratio)]} from the warehouse's record.
    Returns (document, funds_document, history_document, shards)."""
    now = now or datetime.now(timezone.utc)
    cal = calendar(series)
    if len(cal) < 6:
        raise ValueError("fewer than six reporting days")
    pos = {d: i for i, d in enumerate(cal)}
    end = cal[-1]
    year0 = end[:4] + "-01-01"
    wdef = []
    for key, label, n in WINDOWS:
        days = [d for d in cal if d >= year0] if n is None else cal[-n:]
        prior = cal[-2 * n:-n] if n and len(cal) >= 2 * n else []
        wdef.append({"key": key, "label": label, "days": days, "prior": prior, "start": days[0], "end": end, "n": len(days),
                     "complete": n is None or len(days) == n, "before": cal[pos[days[0]] - 1] if pos[days[0]] > 0 else None})
    funds, late, empty, flags = {}, [], [], defaultdict(list)
    recent = len(cal) - 63
    restated = {"rows_compared": 0, "rows_equal": 0, "rows_differing": 0, "after_a_missing_day": 0, "differing_after_a_missing_day": 0, "usd_apart": 0.0, "largest": []}
    for t, rows in series.items():
        rows = [r for r in rows if r[0] in pos]
        clean, notes = count_path(rows, (splits or {}).get(t)) if rows else ([], [])
        if not clean:
            empty.append(t)
            continue
        newest = clean[-1][0]
        behind = len(cal) - 1 - pos[newest]
        cls = classify(profiles.get(t))
        by = {r[0]: r for r in clean}
        last = clean[-1]
        assets = (last[1] or 0) * (last[2] or 0)
        rec = {"t": t, "newest": newest, "behind": behind, "assets": assets, "nav": last[2], "shares": last[1], "by": by, "clean": clean, "cls": cls,
               "name": (profiles.get(t) or {}).get("description"), "issuer": (profiles.get(t) or {}).get("issuer"),
               "typ": (profiles.get(t) or {}).get("product_type"), "inc": day((profiles.get(t) or {}).get("inception_date")), "flags": []}
        for r in clean:
            if r[4] and pos[r[0]] >= recent:
                rec["flags"].append("%s %s" % (r[0], r[4]))
        for x in notes:
            flags[x["k"]].append((t, x["d"], x["x"], x["usd"]))
            if x["k"] == "misprint" and x["d"] in pos and pos[x["d"]] >= recent:
                rec["flags"].append("%s misprint dropped" % x["d"])
        rec["flags"].sort()
        # the provider's own figure beside the count path, on untouched rows of the last 63 days
        given = {r[0]: r[3] for r in rows}
        for i in range(1, len(clean)):
            d, s1, _, mine, fl = clean[i]
            d0, s0, n0 = clean[i - 1][0], clean[i - 1][1], clean[i - 1][2]
            theirs = given.get(d)
            if fl or clean[i - 1][4] or theirs is None or pos[d] < recent:
                continue
            gap = pos[d] - pos[d0] != 1
            same = abs(theirs - mine) <= max(2000.0, 0.003 * abs(mine), 2e-4 * s0 * n0)
            restated["rows_compared"] += 1
            restated["rows_equal"] += same
            restated["after_a_missing_day"] += gap
            if not same:
                restated["rows_differing"] += 1
                restated["differing_after_a_missing_day"] += gap
                restated["usd_apart"] += abs(theirs - mine)
                restated["largest"].append((abs(theirs - mine), t, d, theirs, mine, gap))
        if behind > LATE_DAYS:
            late.append(t)
            rec["late"] = True
        funds[t] = rec
    restated["usd_apart"] = money(restated["usd_apart"])
    restated["largest"] = [[t, d, money(a), money(b), bool(g)] for _, t, d, a, b, g in sorted(restated["largest"], reverse=True)[:25]]
    current = [t for t, f in funds.items() if not f.get("late")]

    def window_of(f, w):
        vals = [f["by"][d][3] for d in w["days"] if d in f["by"] and f["by"][d][3] is not None]
        flow = sum(vals) if vals else None
        base = None
        if w["before"] and w["before"] in f["by"]:
            b = f["by"][w["before"]]
            base = (b[1] or 0) * (b[2] or 0)
        if not base:
            first = next((f["by"][d] for d in w["days"] if d in f["by"]), None)
            base = (first[1] or 0) * (first[2] or 0) if first else None
        pct = 100.0 * flow / base if flow is not None and base and base > 0 else None
        pv = [f["by"][d][3] for d in w["prior"] if d in f["by"] and f["by"][d][3] is not None]
        return flow, pct, (sum(pv) if pv else None), len(vals), base

    for f in funds.values():
        f["w"] = {w["key"]: window_of(f, w) for w in wdef}
        # the five-day figure against the fund's own year of five-day figures
        vals = [(f["by"][d][3] if d in f["by"] and f["by"][d][3] is not None else None) for d in cal[-257:]]
        roll = []
        for i in range(4, len(vals)):
            win = vals[i - 4:i + 1]
            if all(v is not None for v in win):
                roll.append(sum(win))
        f["z5"] = None
        if len(roll) >= 60:
            mean = sum(roll) / len(roll)
            sd = math.sqrt(sum((v - mean) ** 2 for v in roll) / (len(roll) - 1))
            if sd > 0 and f["w"]["w1"][0] is not None:
                f["z5"] = (f["w"]["w1"][0] - mean) / sd
        streak = 0
        for r in reversed(f["clean"]):
            v = r[3]
            if not v:
                break
            if streak == 0:
                streak = 1 if v > 0 else -1
            elif (v > 0) == (streak > 0):
                streak += 1 if streak > 0 else -1
            else:
                break
        f["streak"] = streak

    # ---- the issuers' own share counts: does the provider's count match, and on which day?
    check = {"funds": 0, "same_day": 0, "next_day": 0, "no_match": [], "too_few_days": 0}
    for t, own in (issuer or {}).items():
        f = funds.get(t)
        if not f or not own:
            continue
        own_days = sorted(own)
        nxt = {a: b for a, b in zip(own_days, own_days[1:])}
        same = lag = tell = 0
        for d in cal[-31:-1]:
            ps = f["by"].get(d, (None, None))[1]
            if ps is None or d not in own or d not in nxt:
                continue
            if abs(own[d] - own[nxt[d]]) < 1e-6:
                continue
            tell += 1
            same += abs(ps - own[d]) < 1e-3 * max(1.0, abs(ps))
            lag += abs(ps - own[nxt[d]]) < 1e-3 * max(1.0, abs(ps))
        check["funds"] += 1
        if tell < 3:
            check["too_few_days"] += 1
            f["chk"] = None
        elif lag >= 0.8 * tell:
            check["next_day"] += 1
            f["chk"] = 1
        elif same >= 0.8 * tell:
            check["same_day"] += 1
            f["chk"] = 0
        else:
            check["no_match"].append(t)
            f["chk"] = -1

    # ---- groups
    def members(pred):
        return [funds[t] for t in current if pred(funds[t])]

    def agg(group, share=None):
        """group: list of funds; share: optional {fund: weight} for the look-through table."""
        out = {"n": len(group), "assets": money(sum(f["assets"] * (share[f["t"]] if share else 1.0) for f in group)), "w": {}}
        for w in wdef:
            k = w["key"]
            tin = tout = base = prior = 0.0
            nin = nout = 0
            parts = []
            for f in group:
                v, _, pv, _, b = f["w"][k]
                if v is None:
                    continue
                s = share[f["t"]] if share else 1.0
                v *= s
                if v > 0:
                    tin += v
                    nin += 1
                elif v < 0:
                    tout += v
                    nout += 1
                base += (b or 0) * s
                prior += (pv or 0) * s
                parts.append((v, f["t"]))
            parts.sort()
            out["w"][k] = {"net": money(tin + tout), "in": money(tin), "out": money(tout), "n_in": nin, "n_out": nout,
                           "pct": round(100.0 * (tin + tout) / base, 4) if base > 0 else None, "prior": money(prior) if w["prior"] else None,
                           "top_in": [[t, money(v)] for v, t in parts[::-1][:3] if v > 0], "top_out": [[t, money(v)] for v, t in parts[:3] if v < 0]}
        return out

    def table(key_of, pool, minimum=1):
        buckets = defaultdict(list)
        for f in pool:
            k = key_of(f)
            if k:
                buckets[k].append(f)
        rows = [dict(agg(g), id=k) for k, g in buckets.items() if len(g) >= minimum]
        return sorted(rows, key=lambda r: -(r["w"]["w1"]["net"] or 0))

    plain = members(lambda f: f["cls"]["seg"] != "Leveraged & inverse")
    levered = members(lambda f: f["cls"]["seg"] == "Leveraged & inverse")
    equity = [f for f in plain if f["cls"]["ac"] in ("Equity", "Real Estate")]
    through = defaultdict(dict)
    for f in equity:
        for sec, w in sector_weights(profiles.get(f["t"])).items():
            through[sec][f["t"]] = w
    groups = {
        "segments": table(lambda f: f["cls"]["seg"], members(lambda f: True)),
        "classes": table(lambda f: f["cls"]["ac"] or NO_CLASS, plain),
        "categories": table(lambda f: "%s · %s" % (f["cls"]["ac"], f["cls"]["cat"]) if f["cls"]["ac"] and f["cls"]["cat"] else None, plain),
        "focus": table(lambda f: "%s · %s · %s" % (f["cls"]["ac"], f["cls"]["cat"], f["cls"]["foc"]) if f["cls"]["foc"] and f["cls"]["cat"] else None, plain, 2),
        "sectors": table(lambda f: f["cls"]["sec"], plain),
        "sectors_through": sorted((dict(agg([funds[t] for t in share], share), id=sec) for sec, share in through.items()), key=lambda r: -(r["w"]["w1"]["net"] or 0)),
        "countries": table(lambda f: f["cls"]["cty"], equity),
        "regions": table(lambda f: f["cls"]["reg"], equity),
        "styles": table(lambda f: f["cls"]["sub"] if f["cls"]["seg"].startswith("Equity ·") and f["cls"]["seg"] not in ("Equity · sector funds",) else None, plain, 2),
        "bonds": table(lambda f: f["cls"]["seg"] if f["cls"]["ac"] == "Fixed Income" else None, plain),
        "bond_maturity": table(lambda f: f["cls"]["mat"] if f["cls"]["ac"] == "Fixed Income" else None, plain),
        "commodities": table(lambda f: f["cls"]["sub"] if f["cls"]["ac"] == "Commodities" else None, plain),
        "crypto": table(lambda f: f["cls"]["sub"] if f["cls"]["seg"] == "Crypto" else None, plain),
        "leveraged": table(lambda f: f["cls"]["sub"], levered),
        "issuers": table(lambda f: f["issuer"], members(lambda f: True), 1)[:60],
    }
    totals = {"plain": agg(plain), "leveraged": agg(levered), "all": agg(members(lambda f: True))}

    # ---- boards
    boards = {}
    for w in wdef:
        k = w["key"]
        have = [(f["w"][k][0], f["w"][k][1], f) for f in members(lambda f: True) if f["w"][k][0] is not None]
        by_usd = sorted(have, key=lambda x: x[0])
        big = sorted((x for x in have if x[1] is not None and x[2]["assets"] >= 5e7), key=lambda x: x[1])
        boards[k] = {"in_usd": [[x[2]["t"], money(x[0]), None if x[1] is None else round(x[1], 3)] for x in by_usd[::-1][:BOARD] if x[0] > 0],
                     "out_usd": [[x[2]["t"], money(x[0]), None if x[1] is None else round(x[1], 3)] for x in by_usd[:BOARD] if x[0] < 0],
                     "in_pct": [[x[2]["t"], money(x[0]), round(x[1], 3)] for x in big[::-1][:BOARD] if x[1] > 0],
                     "out_pct": [[x[2]["t"], money(x[0]), round(x[1], 3)] for x in big[:BOARD] if x[1] < 0]}
    turned = {"to_in": [], "to_out": []}
    for f in members(lambda f: True):
        v, _, pv, _, _ = f["w"]["w1"]
        if v is None or pv is None or f["assets"] < 1e8:
            continue
        if v > 0 > pv:
            turned["to_in"].append((v - pv, f["t"], v, pv))
        elif v < 0 < pv:
            turned["to_out"].append((pv - v, f["t"], v, pv))
    turned = {k: [[t, money(v), money(pv)] for _, t, v, pv in sorted(rows, reverse=True)[:25]] for k, rows in turned.items()}
    unusual = sorted(((abs(f["w"]["w1"][1]), f["t"], f["w"]["w1"][0], f["w"]["w1"][1]) for f in members(lambda f: True)
                      if f["w"]["w1"][1] is not None and abs(f["w"]["w1"][1]) >= 25 and f["assets"] >= 5e7), reverse=True)

    # ---- the documents
    cols = ["t", "name", "issuer", "typ", "seg", "ac", "cat", "foc", "reg", "sec", "cty", "mat", "sub", "lev", "assets", "nav", "shares", "newest", "behind",
            "d1", "w1", "m1", "m3", "ytd", "y1", "p_w1", "p_m1", "p_m3", "prior_w1", "z5", "streak", "chk", "flags", "inc"]
    frows = []
    for t in sorted(funds):
        f, c = funds[t], funds[t]["cls"]
        frows.append([t, f["name"], f["issuer"], f["typ"], c["seg"], c["ac"], c["cat"], c["foc"], c["reg"], c["sec"], c["cty"], c["mat"], c["sub"], c["lev"] or 0,
                      money(f["assets"]), f["nav"], f["shares"], f["newest"], f["behind"]] + [money(f["w"][k][0]) for k in ("d1", "w1", "m1", "m3", "ytd", "y1")]
                     + [None if f["w"][k][1] is None else round(f["w"][k][1], 3) for k in ("w1", "m1", "m3")]
                     + [money(f["w"]["w1"][2]), None if f["z5"] is None else round(f["z5"], 2), f["streak"], f.get("chk"), f["flags"][-4:], f["inc"]])
    hist_days = cal[-HISTORY_DAYS:]

    def daily(group, share=None):
        out = []
        for d in hist_days:
            tot = 0.0
            for f in group:
                r = f["by"].get(d)
                if r and r[3]:
                    tot += r[3] * (share[f["t"]] if share else 1.0)
            out.append(round(tot))
        return out
    hist = {"contract": "global-flow-map-history.v1", "version": VERSION, "generated_at": now.isoformat(), "as_of": end, "dates": hist_days, "unit": "USD a day, each fund's cleaned flow summed",
            "series": {}}
    for kind in ("segments", "classes", "sectors", "bonds"):
        for g in groups[kind]:
            pick = {"segments": lambda f, i=g["id"]: f["cls"]["seg"] == i, "classes": lambda f, i=g["id"]: (f["cls"]["ac"] or NO_CLASS) == i and f["cls"]["seg"] != "Leveraged & inverse",
                    "sectors": lambda f, i=g["id"]: f["cls"]["sec"] == i and f["cls"]["seg"] != "Leveraged & inverse",
                    "bonds": lambda f, i=g["id"]: f["cls"]["seg"] == i}[kind]
            hist["series"]["%s:%s" % (kind, g["id"])] = daily(members(pick))
    for sec, share in through.items():
        hist["series"]["sectors_through:%s" % sec] = daily([funds[t] for t in share], share)
    hist["series"]["total:plain"] = daily(plain)
    hist["series"]["total:leveraged"] = daily(levered)
    shard_days = cal[-SHARD_DAYS:]
    shards = defaultdict(lambda: {"contract": "global-flow-map-fund-history.v1", "version": VERSION, "as_of": end, "dates": shard_days,
                                  "fields": {"f": "flow, USD a day (cleaned)", "a": "assets = shares x NAV, USD", "x": "days with a note"}, "funds": {}})
    for t, f in funds.items():
        k = t[0] if t[0].isalpha() else "0"
        shards[k]["funds"][t] = {"f": [None if d not in f["by"] or f["by"][d][3] is None else round(f["by"][d][3]) for d in shard_days],
                                 "a": [None if d not in f["by"] or not f["by"][d][1] or not f["by"][d][2] else round(f["by"][d][1] * f["by"][d][2]) for d in shard_days],
                                 "x": {d: r[4] for d, r in f["by"].items() if r[4] and d in shard_days}}
    age = (now.date() - datetime.strptime(end, "%Y-%m-%d").date()).days
    quality = {"funds_with_rows": len(funds), "funds_current": len(current), "funds_late": len(late), "funds_no_rows": len(empty), "late_rule_days": LATE_DAYS,
               "funds_with_profile": sum(1 for t in current if profiles.get(t)), "funds_unclassified": sum(1 for t in current if funds[t]["cls"]["seg"] == "Unclassified"),
               "assets_current_usd": money(sum(funds[t]["assets"] for t in current)), "assets_late_usd": money(sum(funds[t]["assets"] for t in late)),
               "reporting_days": len(cal), "first_day": cal[0], "newest_day": end, "newest_day_age_days": age, "by_date_months": sorted(by_date_months or []),
               "splits_taken_out": [[t, d, x.split(" (")[0]] for t, d, x, _ in sorted(flags.get("split", []), key=lambda r: r[1]) if d in pos and pos[d] >= recent][-120:],
               "splits": {"in_history": len(flags.get("split", [])), "on_record": sum(1 for x in flags.get("split", []) if "on record" in x[2]),
                          "usd_not_money": money(sum(x[3] or 0 for x in flags.get("split", []))), "recorded_but_not_in_the_counts": len(flags.get("unseen", [])),
                          "largest": [[t, d, x.split(" (")[0], money(v)] for t, d, x, v in sorted(flags.get("split", []), key=lambda r: -(r[3] or 0))[:12]]},
               "round_trips": {"in_history": len(flags.get("trip", [])), "last_63_days": sum(1 for x in flags.get("trip", []) if pos.get(x[1], -1) >= recent),
                               "usd_each_way_last_63_days": money(sum(x[3] or 0 for x in flags.get("trip", []) if pos.get(x[1], -1) >= recent)),
                               "largest_last_63_days": [[t, d, money(v), x] for t, d, x, v in sorted((x for x in flags.get("trip", []) if pos.get(x[1], -1) >= recent), key=lambda r: -(r[3] or 0))[:25]]},
               "misprints_dropped": {"in_history": len(flags.get("misprint", [])), "last_63_days": [[t, d, x] for t, d, x, _ in sorted(flags.get("misprint", []), key=lambda r: r[1]) if pos.get(d, -1) >= recent][-40:]},
               "jumps_last_21_days": [[t, d, money(v), x] for t, d, x, v in sorted((x for x in flags.get("jump", []) if pos.get(x[1], -1) >= len(cal) - 21 and x[0] in funds and not funds[x[0]].get("late")),
                                                                                key=lambda r: -abs(r[3] or 0))[:40]],
               "provider_figure": restated,
               "late_largest": [[t, funds[t]["newest"], money(funds[t]["assets"])] for t in sorted(late, key=lambda t: -funds[t]["assets"])[:25]],
               "unusual_week": [[t, money(v), round(p, 1)] for _, t, v, p in unusual[:40]], "issuer_check": check}
    doc = {"engine": "justhodl-global-flow-desk", "version": VERSION, "contract": CONTRACT, "generated_at": now.isoformat(), "as_of": end,
           "orders_through": end, "windows": [{k: w[k] for k in ("key", "label", "start", "end", "n", "complete")} for w in wdef],
           "totals": totals, "groups": groups, "boards": boards, "turned": turned, "quality": quality,
           "documents": {"funds": OUT_FUNDS, "history": OUT_HISTORY, "fund_history": OUT_SHARDS + "<first letter>.json", "research_view": "data/global-flow-desk.json"},
           "method": {
               "flow": "A fund's flow on a day is its share count's path: (shares outstanding - shares on the row before) x the NAV of the row before, dated on the day of the order. On rows that follow each other day by day this equals the provider's own creation and redemption figure; after a missing day the provider's figure is not used, and the flow of the missing days lands on the day the count is next seen.",
               "windows": "Sums over the last 1, 5, 21, 63 or 252 reporting days, or since 1 January. A reporting day is one on which at least half the usual number of funds reported.",
               "percent": "A window's flow as a percentage of the fund's assets (shares x NAV) on the reporting day before the window began.",
               "split_rule": "A share split is taken from the warehouse's record of splits when the share count moves by the recorded ratio within six days of it, or read from the rows when count and NAV move by a whole ratio in opposite directions on the same or the next row. Counts and NAVs before the split are restated on the new basis, so a split is never a flow.",
               "misprint_rule": "A row whose share count and NAV both leave by more than 40% and 30% and both come back on the next row is dropped.",
               "round_trip_rule": "A share count that jumps by more than 40% and is back within two rows to within a fifth of the jump (a heartbeat trade, a reset day, a doubled print) counts as the difference it left behind.",
               "jump_rule": "A jump that stays is counted as money, however large, and is listed.",
               "late_rule": "A fund whose newest row is more than %d reporting days old is left out of every total and listed." % LATE_DAYS,
               "segments": "One segment per fund from the provider's class, category, focus, region and weights. Leveraged and inverse funds have their own segment and are not in the class, sector, country or style tables.",
               "sectors": "Sector funds: funds the provider classes as sector funds, by its focus. Through all equity funds: every unleveraged equity and real-estate fund's flow split by the provider's sector weights.",
               "countries": "Equity funds with 60% or more in one country other than the United States, by the provider's geographic weights. China: mainland, Hong Kong, Cayman Islands and Bermuda weights together reach 60% in an Asia-Pacific fund (Chinese companies are registered offshore); a fund 60% in Hong Kong alone is Hong Kong.",
               "issuer_check": "For funds the issuer's own daily file is kept for, the provider's share count is compared with the issuer's on the same day and on the issuer's next day.",
               "not_measured": "Who bought or sold (institutions, retail, foreigners) is not in this data and is not inferred. ETF flows are not mutual-fund or direct stock purchases."},
           "sources": {"by_date": BY_DATE, "per_fund": PER_FUND, "profiles": PROFILES, "splits": SPLITS, "issuer_files": "data/etf-creations/", "provider": "ETF Global fund flows through the Polygon bank (justhodl-polygon-bank)"}}
    fdoc = {"contract": "global-flow-map-funds.v1", "version": VERSION, "generated_at": now.isoformat(), "as_of": end, "cols": cols, "rows": frows,
            "units": {"assets": "USD", "d1..y1": "USD", "p_w1..p_m3": "% of assets before the window", "prior_w1": "USD, the five days before", "z5": "five-day flow against the fund's own year of five-day flows",
                      "streak": "days in a row of money in (+) or out (-)", "chk": "1 the issuer's next-day file matches, 0 its same-day file matches, -1 neither, null no issuer file", "behind": "reporting days behind the newest"}}
    return doc, fdoc, hist, dict(shards)


# --------------------------------------------------------------------------------------------- reading and writing
def _get(s3, bucket, key):
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)


def _list(s3, bucket, prefix):
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        out += [o["Key"] for o in page.get("Contents", [])]
    return out


def _rows(doc):
    return doc if isinstance(doc, list) else (doc.get("results") if isinstance(doc, dict) and isinstance(doc.get("results"), list) else [])


PROFILE_FIELDS = ("description", "issuer", "product_type", "inception_date", "asset_class", "category", "focus", "region", "development_class", "leverage_style", "levered_amount",
                  "geographic_exposure", "currency_exposure", "sector_exposure", "maturity_exposure", "processed_date")


def collect(s3, bucket, log=print, workers=32):
    """Everything the build needs, from the warehouse."""
    t0 = time.time()
    months = sorted(k for k in _list(s3, bucket, BY_DATE) if k.endswith(".json.gz"))[-MONTHS_READ:]
    fresh = defaultdict(list)
    for k in months:
        for x in _rows(_get(s3, bucket, k)):
            t = x.get("composite_ticker")
            c = compact(x) if isinstance(t, str) and t else None
            if c:
                fresh[t].append(c)
    log("[flow-map] by-date months %d, funds %d, %.1f s" % (len(months), len(fresh), time.time() - t0))

    def fund(key):
        t = key.rsplit("/", 1)[-1].split(".")[0]
        try:
            rows = [compact(x) for x in _rows(_get(s3, bucket, key)) if x.get("composite_ticker") == t]
        except Exception:  # noqa: BLE001
            rows = []
        return t, merge_rows(rows, fresh.get(t))[-KEEP_DAYS:]
    series = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for t, rows in pool.map(fund, [k for k in _list(s3, bucket, PER_FUND) if k.endswith(".json.gz")]):
            if rows:
                series[t] = rows
    for t, rows in fresh.items():
        if t not in series:
            series[t] = merge_rows(rows)[-KEEP_DAYS:]
    log("[flow-map] fund histories %d, %.1f s" % (len(series), time.time() - t0))

    def prof(key):
        t = key.rsplit("/", 1)[-1].split(".")[0]
        try:
            rows = [x for x in _rows(_get(s3, bucket, key)) if x.get("composite_ticker") == t]
        except Exception:  # noqa: BLE001
            return t, None
        if not rows:
            return t, None
        p = max(rows, key=lambda x: str(x.get("processed_date") or ""))
        return t, {k: p.get(k) for k in PROFILE_FIELDS if p.get(k) not in (None, "", {}, [])}
    profiles = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for t, p in pool.map(prof, [k for k in _list(s3, bucket, PROFILES) if k.endswith(".json.gz")]):
            if p and t in series:
                profiles[t] = p
    log("[flow-map] profiles %d, %.1f s" % (len(profiles), time.time() - t0))
    issuer = {}
    try:
        idx = _get(s3, bucket, ISSUER_INDEX)
        listed = idx.get("funds") or []
        names = sorted(listed.keys() if isinstance(listed, dict) else [x.get("ticker") if isinstance(x, dict) else x for x in listed])

        def feed(t):
            try:
                d = _get(s3, bucket, ISSUER_FEED % t)
                days, sh = d.get("d") or [], (d.get("series") or {}).get("shares") or []
                return t, {a: float(b) for a, b in list(zip(days, sh))[-60:] if b is not None}
            except Exception:  # noqa: BLE001
                return t, {}
        with ThreadPoolExecutor(max_workers=16) as pool:
            issuer = {t: v for t, v in pool.map(feed, [t for t in names if t in series]) if v}
    except Exception as exc:  # noqa: BLE001
        log("[flow-map] issuer files not read: %s" % type(exc).__name__)
    log("[flow-map] issuer files %d, %.1f s" % (len(issuer), time.time() - t0))
    return series, profiles, issuer, [k.rsplit("/", 1)[-1][:7] for k in months], recorded_splits(s3, bucket, series, log)


def recorded_splits(s3, bucket, series, log=print):
    """The warehouse's record of splits for the funds and the days in hand: {fund: [(date, new shares per old share)]}."""
    first = min((rows[0][0] for rows in series.values() if rows), default=None)
    out = defaultdict(list)
    if not first:
        return {}
    keys = [k for k in _list(s3, bucket, SPLITS) if k.endswith(".json.gz") and k.rsplit("/", 1)[-1][:7] >= first[:7]]

    def one(key):
        try:
            return _rows(_get(s3, bucket, key))
        except Exception:  # noqa: BLE001
            return None
    read = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        for rows in pool.map(one, keys):
            if rows is None:
                continue
            read += 1
            for x in rows:
                t, d = (x.get("ticker"), day(x.get("execution_date"))) if isinstance(x, dict) else (None, None)
                a, b = (num(x.get("split_from")), num(x.get("split_to"))) if t in series and d else (None, None)
                if a and b and a > 0 and b > 0:
                    out[t].append((d, b / a))
    log("[flow-map] split record: %d of %d months read, %d funds with a split" % (read, len(keys), len(out)))
    return {t: sorted(set(v)) for t, v in out.items()}


def guard(doc):
    q = doc["quality"]
    reasons = []
    if q["funds_current"] < MIN_FUNDS:
        reasons.append("only %d current funds" % q["funds_current"])
    if q["newest_day_age_days"] > MAX_AGE_DAYS:
        reasons.append("newest reporting day %s is %d days old" % (q["newest_day"], q["newest_day_age_days"]))
    if not q["by_date_months"]:
        reasons.append("no by-date month in the warehouse")
    if not doc["groups"]["segments"] or not doc["groups"]["sectors"]:
        reasons.append("no segments or no sectors")
    return reasons


def encode(doc):
    return json.dumps(doc, separators=(",", ":"), allow_nan=False).encode("utf-8")


def run(s3, bucket, now=None, dry_run=False, log=print):
    """Build from the warehouse; publish unless dry_run or a guard refuses.  Never raises past its caller's try."""
    t0 = time.time()
    now = now or datetime.now(timezone.utc)
    series, profiles, issuer, months, splits = collect(s3, bucket, log)
    doc, fdoc, hist, shards = build(series, profiles, issuer, now, months, splits)
    reasons = guard(doc)
    q = doc["quality"]
    summary = {"version": VERSION, "as_of": doc["as_of"], "funds_current": q["funds_current"], "funds_late": q["funds_late"], "assets_current_usd": q["assets_current_usd"],
               "segments": len(doc["groups"]["segments"]), "sectors": len(doc["groups"]["sectors"]), "splits_taken_out": len(q["splits_taken_out"]), "round_trips": q["round_trips"]["last_63_days"], "provider_rows_differing": q["provider_figure"]["rows_differing"],
               "issuer_check": {k: (len(v) if isinstance(v, list) else v) for k, v in q["issuer_check"].items()}, "refused": reasons, "dry_run": bool(dry_run),
               "seconds": round(time.time() - t0, 1)}
    if reasons or dry_run:
        summary["published"] = False
        log("[flow-map] " + json.dumps(summary))
        return summary, (doc, fdoc, hist, shards)
    summary.update(published=True, objects=publish(s3, bucket, doc, fdoc, hist, shards), seconds=round(time.time() - t0, 1))
    log("[flow-map] " + json.dumps(summary))
    return summary, (doc, fdoc, hist, shards)


def publish(s3, bucket, doc, fdoc, hist, shards):
    """Write the four kinds of document; the main one last, so a reader never finds it pointing at files not yet written."""
    puts = [(OUT_SHARDS + k + ".json", v) for k, v in sorted(shards.items())] + [(OUT_HISTORY, hist), (OUT_FUNDS, fdoc), (OUT, doc)]
    for key, body in puts:
        s3.put_object(Bucket=bucket, Key=key, Body=gzip.compress(encode(body), 6), ContentType="application/json", ContentEncoding="gzip", CacheControl="public, max-age=300")
    return len(puts)
