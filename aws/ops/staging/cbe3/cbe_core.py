"""cbe_core - the arithmetic of Credit Before Equity 2.0. Pure: no I/O, no clock.

The credit leg is what the bond market prints: every US-dollar corporate bond the corporate bonds desk holds (the daily holdings
of the iShares investment-grade, high-yield and emerging-market corporate books), each with the yield the fund file prints.

  spread      a bond's yield minus the Treasury par yield at the bond's duration-matched maturity (the maturity of a par
              bond with the same modified duration, never past the bond's own), in basis points. A spread to worst against
              the par curve - not an option-adjusted spread.
  company     the bonds of every legal issuer tied to one listed company, or of one issuer when no listed company is
              tied (cbe_names). Its spread is the average of its bonds weighted by the market value the funds hold.
  change      the same bonds at both dates (a bond present at only one of them is left out), weighted the same way.
  industry    the fund file's sector. Its change is the middle company's change, each company weighted by the market
              value held - a distressed name cannot carry an industry by itself.
  signal      credit moved by more than its own book (investment grade, high yield, emerging) and the stock has not
              followed.
  stress      where a company stands in its own book on three things, averaged: how wide its spread is, how far it has
              widened over the long window (the year to date) as a share of where it started, and how far it widened in
              the month. 0 to 100; it says nothing about the stock. A lead asks whether the stock has noticed; stress
              asks only what the bond market thinks.
"""
from collections import Counter, defaultdict
from datetime import date
import math

import cbe_names as N
import cbe_themes as TH

VERSION = "3.0.0"
CONTRACT = "credit-before-equity.v2"

BOOKS = {"US IG": "ig", "US HY": "hy", "EM corporate": "em", "EM HY": "em"}       # the US-dollar corporate books that are read
BOOK_WORDS = {"ig": "Investment grade", "hy": "High yield", "em": "Emerging markets"}
YIELD_FLOOR, YIELD_CAP = -4.99, 49.99      # the fund file prints -5 and 50 where it stops calculating: not yields
MIN_DUR = 1.0                              # years: inside a year of redemption a yield is the calendar, not the credit
DISTRESSED_BP = 1000.0
PAR_STALE_DAYS = 7

TENOR_YEARS = {"1M": 1 / 12, "2M": 2 / 12, "3M": 0.25, "4M": 4 / 12, "6M": 0.5, "1Y": 1.0, "2Y": 2.0, "3Y": 3.0, "5Y": 5.0, "7Y": 7.0, "10Y": 10.0, "20Y": 20.0, "30Y": 30.0}

# windows: (key, words, days back aimed at, tolerance in days). The reference is the stored day nearest the aim.
WINDOWS = (("w1", "1 week", 7, 3), ("m1", "1 month", 30, 12), ("m3", "3 months", 91, 21))
PRIMARY = "m1"

# the rule
MOVE_FLOOR_BP = 10.0       # a company's change past its book's must be at least this many basis points ...
MOVE_REL = 0.10            # ... and at least this share of the spread it started from
OWN_MOVE_BP = 5.0          # ... and its own spread must have moved this far the same way
FLAT_BAND_PCT = 3.0        # the stock "has not followed" while it is not past this, in the direction credit points
CDS_MOVE_BP = 10.0         # a traded CDS counts as moving past this ...
CDS_MOVE_REL = 0.10        # ... and this share of its level
CDS_BELIEVABLE_BP = 5000.0 # past this a "spread" is a points-upfront quote turned into a running one: shown as points, not ranked
MIN_MATCHED = 1            # bonds seen at both dates
LEADS = ("CREDIT_LEADS_DOWN", "CREDIT_LEADS_UP")
RECOVERY = 0.40            # what a default is assumed to pay of face when a spread is read as a chance of default
WALL_Y = 2.0               # the maturity wall: debt coming due inside this many years
CURVE_SHORT_Y, CURVE_LONG_Y = 3.0, 7.0      # a company's near bonds and its far ones
LONG_MIN_DAYS = 60         # the long window is the year to date once it is this long; before that, three months
STRESSED = 80.0            # a stress rank from here up is "under stress" on the board

NOT_COMPANIES = ("government", "local", "sovereign", "supranational", "treasur", "agenc", "covered", "securitized", "abs", "cmbs", "mbs", "cash", "derivative", "municipal")
INDUSTRY_WORDS = {
    "banking": "Banking", "basic industry": "Basic industry", "brokerage/asset managers/exchanges": "Brokers & asset managers", "capital goods": "Capital goods",
    "communications": "Communications", "consumer cyclical": "Consumer cyclical", "consumer non-cyclical": "Consumer non-cyclical", "electric": "Electric utilities",
    "energy": "Energy", "finance companies": "Finance companies", "healthcare": "Healthcare", "insurance": "Insurance", "natural gas": "Natural gas utilities",
    "reits": "REITs & real estate", "real estate": "REITs & real estate", "technology": "Technology", "transportation": "Transportation", "owned no guarantee": "State-owned",
}
COARSE = {"industrial": "Other industrial", "industrial other": "Other industrial", "corporates": "Other industrial", "other": "Other industrial", "": "Other industrial",
          "utility": "Other utilities", "utility other": "Other utilities", "financial institutions": "Other financial", "financial other": "Other financial"}
FROM_BOOK_SECTOR = {"Energy": "Energy", "Technology": "Technology", "Healthcare": "Healthcare", "Utilities": "Electric utilities", "Basic Materials": "Basic industry",
                    "Industrials": "Capital goods", "Consumer Cyclical": "Consumer cyclical", "Consumer Defensive": "Consumer non-cyclical",
                    "Communication Services": "Communications", "Real Estate": "REITs & real estate"}
TRANSPORT_WORDS = ("airline", "railroad", "trucking", "marine shipping", "freight", "airport")
BANK_TOKENS = {"BANK", "BANCO", "BANC", "BANCORP", "BANKING", "BANQUE", "BANCA", "BANKS"}


def f(v, d=2):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return round(v, d) if v == v and abs(v) != float("inf") else None


def days_between(a, b):
    return (date.fromisoformat(b) - date.fromisoformat(a)).days


# ------------------------------------------------------------------------------------------------------- the curve
def par_on(rows, day, stale=PAR_STALE_DAYS):
    """(curve row, its date) for `day`, or the last earlier day within `stale` days; (None, None) when there is none."""
    if not rows or not day:
        return None, None
    if day in rows:
        return rows[day], day
    earlier = [d for d in rows if d <= day]
    if not earlier:
        return None, None
    last = max(earlier)
    return (rows[last], last) if days_between(last, day) <= stale else (None, None)


def par_yield(row, years):
    """The Treasury par yield (%) at `years`, straight-line between the published maturities, flat past the ends."""
    pts = sorted((TENOR_YEARS[k], float(v)) for k, v in (row or {}).items() if k in TENOR_YEARS and isinstance(v, (int, float)))
    if not pts:
        return None
    if years <= pts[0][0]:
        return pts[0][1]
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if years <= x1:
            return y0 + (y1 - y0) * (years - x0) / (x1 - x0)
    return pts[-1][1]


def tenor_of(dur, yield_pct, years_left=None):
    """The maturity (years) of a par bond with this modified duration at this yield, never past the bond's own maturity."""
    y = max(float(yield_pct), 0.5) / 100.0
    x = float(dur) * y
    t = 30.0 if x >= 0.97 else -math.log(1.0 - x) / math.log(1.0 + y)
    t = max(t, float(dur))
    if years_left is not None and years_left > 0:
        t = min(t, max(float(years_left), float(dur)))
    return min(t, 30.0)


# ------------------------------------------------------------------------------------------------------- the bonds
def industry_of(fund_sector, company=None, issuer_key=""):
    """The board's industry for a fund sector; None when the sector is not companies at all."""
    s = str(fund_sector or "").strip().lower()
    if any(s.startswith(w) for w in NOT_COMPANIES):
        return None
    if s in INDUSTRY_WORDS:
        return INDUSTRY_WORDS[s]
    coarse = COARSE.get(s, "Other industrial")
    if company:
        ind = str(company.get("industry") or "").lower()
        if coarse == "Other financial":
            if "bank" in ind:
                return "Banking"
            if "insurance" in ind:
                return "Insurance"
            if "reit" in ind or company.get("sector") == "Real Estate":
                return "REITs & real estate"
            return coarse
        if coarse == "Other utilities":
            return "Natural gas utilities" if "gas" in ind else "Electric utilities" if company.get("sector") == "Utilities" else coarse
        if any(w in ind for w in TRANSPORT_WORDS):
            return "Transportation"
        return FROM_BOOK_SECTOR.get(company.get("sector")) or coarse
    if coarse == "Other financial" and BANK_TOKENS & set(str(issuer_key).split()):
        return "Banking"
    return coarse


def bond_points(bonds, as_of, par_rows, refs):
    """Every bond the board reads, with its spread today and at each derived reference.

    refs: {field of the bond holding the yield change: reference date}, e.g. {"chg": "2026-10-30", "chg_y": "2025-12-31"}.
    Returns (points, counts, par dates). A point: {k, issuer, sector, country, book, v, y, dur, t, s, ref: {date: spread}, why}.
    """
    row_now, par_now = par_on(par_rows, as_of)
    ref_rows = {d: par_on(par_rows, d) for d in set(refs.values()) if d}
    out, counts = [], Counter()
    for b in bonds or []:
        if not isinstance(b, dict) or b.get("left"):
            continue
        if b.get("ccy") != "USD":
            counts["not in US dollars"] += 1
            continue
        book = BOOKS.get(b.get("book"))
        if not book:
            counts["floating-rate, convertible or another book"] += 1
            continue
        if industry_of(b.get("sector")) is None:
            counts["not a company (government, agency, local authority)"] += 1
            continue
        p = {"k": b.get("k"), "issuer": b.get("issuer") or "", "sector": b.get("sector") or "", "country": b.get("country") or "", "book": book,
             "v": float(b.get("demand") or 0.0), "y": b.get("rate"), "dur": b.get("dur"), "t": None, "s": None, "ref": {}, "why": None,
             "c": b.get("coupon") if isinstance(b.get("coupon"), (int, float)) else None, "m": str(b.get("maturity"))[:10] if b.get("maturity") else None,
             "px": b.get("price") if isinstance(b.get("price"), (int, float)) and b.get("price") > 0 else None, "yrs": None}
        try:
            p["yrs"] = round(days_between(as_of, p["m"]) / 365.25, 2) if p["m"] else None
        except (TypeError, ValueError):
            p["yrs"] = None
        y, dur = b.get("rate"), b.get("dur")
        if not isinstance(y, (int, float)):
            p["why"] = "no yield printed"
        elif not YIELD_FLOOR < y < YIELD_CAP:
            p["why"] = "yield at the file's limit"
        elif y <= 0:
            p["why"] = "priced to an early call"
        elif not isinstance(dur, (int, float)) or dur < MIN_DUR:
            p["why"] = "under a year to run"
        elif row_now is None:
            p["why"] = "no Treasury curve for the day"
        counts["read"] += 1
        if p["why"]:
            counts[p["why"]] += 1
            out.append(p)
            continue
        t = tenor_of(dur, y, p["yrs"])
        p["t"] = round(t, 3)
        p["s"] = round((y - par_yield(row_now, t)) * 100.0, 1)
        counts["with a spread"] += 1
        for field, ref in refs.items():
            chg = b.get(field)
            row, _ = ref_rows.get(ref) or (None, None)
            if not ref or row is None or not isinstance(chg, (int, float)):
                continue
            y0 = y - chg
            if not 0 < y0 < YIELD_CAP:
                continue
            t0 = min(t + max(days_between(ref, as_of), 0) / 365.25, 30.0)
            p["ref"][ref] = round((y0 - par_yield(row, t0)) * 100.0, 1)
        out.append(p)
    return out, counts, {"as_of": par_now, **{d: v[1] for d, v in ref_rows.items()}}


def snapshot_of(points, day, derived_from=None):
    """What the ledger keeps for one day: every bond's spread, by the bond desk's own key."""
    if derived_from:
        bonds = {p["k"]: p["ref"][day] for p in points if p.get("k") and day in p["ref"]}
    else:
        bonds = {p["k"]: p["s"] for p in points if p.get("k") and p["s"] is not None}
    return {"as_of": day, "derived_from": derived_from, "n": len(bonds), "bonds": bonds}


def pick_windows(as_of, days):
    """{window: reference day} out of the stored days: the nearest to each aim inside its tolerance, and the year's start."""
    out = {}
    for key, _, aim, tol in WINDOWS:
        best = None
        for d in days:
            back = days_between(d, as_of)
            if back > 0 and abs(back - aim) <= tol and (best is None or abs(back - aim) < abs(days_between(best, as_of) - aim)):
                best = d
        out[key] = best
    year_end = "%d-12-31" % (int(as_of[:4]) - 1)
    near = [d for d in days if d <= year_end and days_between(d, year_end) <= 10]
    out["ytd"] = max(near) if near else None
    return out


def window_words(key):
    return {k: w for k, w, _, _ in WINDOWS}.get(key, "Year to date")


# ------------------------------------------------------------------------------------------------------- companies
def weighted_median(pairs):
    """The value at which half the weight lies on each side. pairs: [(value, weight)]; an unweighted median when no weight."""
    pairs = sorted((float(v), max(float(w), 0.0)) for v, w in pairs if v is not None)
    if not pairs:
        return None
    total = sum(w for _, w in pairs)
    if total <= 0:
        n = len(pairs)
        return pairs[n // 2][0] if n % 2 else (pairs[n // 2 - 1][0] + pairs[n // 2][0]) / 2.0
    run = 0.0
    for v, w in pairs:
        run += w
        if run >= total / 2.0:
            return v
    return pairs[-1][0]


def wavg(pairs):
    pairs = [(float(v), float(w)) for v, w in pairs if v is not None]
    total = sum(w for _, w in pairs)
    if not pairs:
        return None
    return sum(v * w for v, w in pairs) / total if total > 0 else sum(v for v, _ in pairs) / len(pairs)


def companies(points, matcher, snapshots, windows):
    """One row for every company (or untied issuer) with bonds on the board.

    snapshots: {day: {bond key: spread}}; windows: {window: reference day or None}. Returns (rows, ties) - ties is the audit of
    how each issuer name was tied.
    """
    by_key = {}
    for p in points:
        key, cut = N.issuer_key(p["issuer"])
        if not key:
            continue
        e = by_key.setdefault(key, {"cut": False, "sector": Counter(), "country": Counter(), "raw": Counter(), "points": []})
        e["cut"] = e["cut"] or cut
        e["sector"][p["sector"]] += p["v"] or 1.0
        e["country"][p["country"]] += p["v"] or 1.0
        e["raw"][N.clean_issuer(p["issuer"])] += p["v"] or 1.0
        e["points"].append(p)
    groups, ties = {}, []
    for key, e in by_key.items():
        sector = e["sector"].most_common(1)[0][0]
        country = e["country"].most_common(1)[0][0]
        co, how = matcher.match(key, e["cut"], sector, foreign=country not in ("", "United States"))
        ident = "T:" + co["symbol"] if co else "I:" + key
        g = groups.setdefault(ident, {"id": ident, "company": co, "keys": [], "points": [], "sector": Counter(), "country": Counter(), "raw": Counter(), "how": set()})
        g["keys"].append(key)
        g["points"].extend(e["points"])
        g["sector"].update(e["sector"])
        g["country"].update(e["country"])
        g["raw"].update(e["raw"])
        g["how"].add(how if co else "untied")
        value = sum(p["v"] for p in e["points"])
        ties.append({"issuer": e["raw"].most_common(1)[0][0], "key": key, "ticker": co["symbol"] if co else None, "company": co["name"] if co else None,
                     "how": how if co else "untied", "why": None if co else how, "value": round(value, 0), "bonds": len(e["points"])})
    rows = []
    for ident, g in groups.items():
        pts = g["points"]
        good = [p for p in pts if p["s"] is not None]
        co = g["company"]
        book = Counter()
        for p in pts:
            book[p["book"]] += p["v"] or 1.0
        fund_sector = g["sector"].most_common(1)[0][0]
        row = {
            "id": ident, "ticker": co["symbol"] if co else None, "name": co["name"] if co else g["raw"].most_common(1)[0][0],
            "industry": industry_of(fund_sector, co, g["keys"][0]), "fund_sector": fund_sector, "book": book.most_common(1)[0][0],
            "country": g["country"].most_common(1)[0][0] or None, "n_bonds": len(pts), "n_spread": len(good), "n_issuers": len(g["keys"]),
            "value": round(sum(p["v"] for p in pts), 0), "yield_pct": f(wavg([(p["y"], p["v"]) for p in good]), 2), "dur": f(wavg([(p["dur"], p["v"]) for p in good]), 2),
            "spread_bp": f(wavg([(p["s"], p["v"]) for p in good]), 1), "tie": sorted(g["how"])[0] if co else "untied", "win": {},
            "theme": None, "layer": None,
            "n_limit": sum(1 for p in pts if p["why"] == "yield at the file's limit"),
        }
        row["distressed"] = bool(row["spread_bp"] is not None and row["spread_bp"] >= DISTRESSED_BP) or (row["n_limit"] > 0 and not good)
        row["theme"], row["layer"] = TH.theme_of(row["ticker"], row["name"])
        row.update(bond_facts(pts))
        for p in pts:
            p["co"] = ident
        for wkey, ref in windows.items():
            snap = snapshots.get(ref) if ref else None
            if not snap:
                continue
            both = [(p["s"], snap[p["k"]], p["v"]) for p in good if p["k"] in snap and snap[p["k"]] is not None]
            if len(both) < MIN_MATCHED:
                continue
            was = wavg([(s0, v) for _, s0, v in both])
            d = wavg([(s - s0, v) for s, s0, v in both])
            row["win"][wkey] = {"d_bp": f(d, 1), "from_bp": f(was, 1), "n": len(both), "d_pct": f(100.0 * d / was, 1) if was is not None and was >= 10.0 else None}
        rows.append(row)
    rows.sort(key=lambda r: (-(r["value"] or 0), r["name"]))
    return rows, ties


def bond_facts(pts):
    """What a company's own bonds say besides the spread - the things a credit analyst reads off the debt itself.

      wam        years to maturity, weighted by market value held
      due2y      share of that value coming due inside 24 months, in percent: the maturity wall
      coupon     the coupon it pays today, weighted the same way
      step_bp    today's yield less that coupon, in basis points, on the bonds with a yield: what refinancing would cost
                 over what the company pays now (positive: it would pay more)
      px         dollar price, weighted; px_min the lowest of its bonds
      curve_bp   the spread of its bonds with 7 years or more to run less the spread of those with 3 or less. A company
                 whose near bonds carry MORE spread than its far ones (negative) is priced for trouble soon.
    """
    dated = [(p["yrs"], p["v"]) for p in pts if p.get("yrs") is not None and p["yrs"] > 0 and p["v"] > 0]
    total = sum(v for _, v in dated)
    good = [p for p in pts if p["s"] is not None]
    short = wavg([(p["s"], p["v"]) for p in good if p["t"] is not None and p["t"] <= CURVE_SHORT_Y])
    far = wavg([(p["s"], p["v"]) for p in good if p["t"] is not None and p["t"] >= CURVE_LONG_Y])
    prices = [p["px"] for p in pts if p.get("px") is not None]
    return {
        "wam": f(wavg(dated), 1), "due2y": f(100.0 * sum(v for y, v in dated if y <= WALL_Y) / total, 1) if total > 0 else None,
        "coupon": f(wavg([(p["c"], p["v"]) for p in pts if p.get("c") is not None]), 2),
        "step_bp": f(wavg([((p["y"] - p["c"]) * 100.0, p["v"]) for p in good if p.get("c") is not None]), 0),
        "px": f(wavg([(p["px"], p["v"]) for p in pts if p.get("px") is not None]), 1), "px_min": f(min(prices), 1) if prices else None,
        "s_short": f(short, 1), "s_long": f(far, 1), "curve_bp": f(far - short, 1) if short is not None and far is not None else None,
    }


def book_medians(rows, windows):
    """{window: {book: the middle company's change, by market value held}}."""
    out = {}
    for wkey in windows:
        per = defaultdict(list)
        for r in rows:
            w = r["win"].get(wkey)
            if w and w.get("d_bp") is not None:
                per[r["book"]].append((w["d_bp"], r["value"]))
        out[wkey] = {b: f(weighted_median(v), 1) for b, v in per.items()}
    return out


def add_excess(rows, medians):
    for r in rows:
        for wkey, w in r["win"].items():
            m = (medians.get(wkey) or {}).get(r["book"])
            w["x_bp"] = f(w["d_bp"] - m, 1) if m is not None and w.get("d_bp") is not None else None


def aggregate(rs, windows):
    """What the board says of a group of companies: its middle spread and change by market value held, and its stress."""
    value = sum(r["value"] for r in rs)
    stressed = [r for r in rs if r.get("stress") is not None and r["stress"] >= STRESSED]
    row = {"n": len(rs), "n_listed": sum(1 for r in rs if r["ticker"]), "value": round(value, 0),
           "spread_bp": f(weighted_median([(r["spread_bp"], r["value"]) for r in rs if r["spread_bp"] is not None]), 1),
           "n_distressed": sum(1 for r in rs if r["distressed"]),
           "stress": f(weighted_median([(r["stress"], r["value"]) for r in rs if r.get("stress") is not None]), 1), "n_stressed": len(stressed),
           "stressed_value_pct": f(100.0 * sum(r["value"] for r in stressed) / value, 1) if value > 0 else None,
           # the names a reader is told: the largest borrowers among those under stress, not a $5M holding that happens to rank highest
           "largest_stressed": [[r["name"], r["stress"]] for r in sorted(stressed, key=lambda r: (-r["value"], -r["stress"]))[:3]], "win": {}}
    for wkey in windows:
        have = [(r["win"][wkey]["d_bp"], r["value"], r) for r in rs if wkey in r["win"] and r["win"][wkey].get("d_bp") is not None]
        if not have:
            continue
        d = weighted_median([(x, v) for x, v, _ in have])
        was = weighted_median([(r["win"][wkey]["from_bp"], v) for _, v, r in have if r["win"][wkey].get("from_bp") is not None])
        wider = sum(1 for x, _, _ in have if x >= 5.0)
        tighter = sum(1 for x, _, _ in have if x <= -5.0)
        worst = max(have, key=lambda t: t[0])[2]
        best = min(have, key=lambda t: t[0])[2]
        row["win"][wkey] = {"d_bp": f(d, 1), "from_bp": f(was, 1), "d_pct": f(100.0 * d / was, 1) if was and was >= 10.0 else None, "n": len(have),
                            "wider": wider, "tighter": tighter, "widest": [worst["name"], worst["win"][wkey]["d_bp"]], "tightest": [best["name"], best["win"][wkey]["d_bp"]]}
    return row


def industries(rows, windows, book=None):
    """The heat map's rows: one per industry, for all books or one of them."""
    per = defaultdict(list)
    for r in rows:
        if r.get("industry") and (book is None or r["book"] == book) and r["n_bonds"]:
            per[r["industry"]].append(r)
    out = [{"industry": name, **aggregate(rs, windows)} for name, rs in per.items()]
    out.sort(key=lambda r: (-(r["value"] or 0), r["industry"]))
    return out


def themes(rows, windows):
    """One row a theme (cbe_themes), with a row for each of its layers that has a company on the board."""
    out = []
    for key, meta in TH.THEMES.items():
        rs = [r for r in rows if r.get("theme") == key]
        if not rs:
            continue
        row = {"key": key, "name": meta["name"], "note": meta["note"], **aggregate(rs, windows), "layers": []}
        for layer in TH.LAYER_ORDER:
            ls = [r for r in rs if r.get("layer") == layer]
            if ls:
                row["layers"].append({"key": layer, "name": TH.LAYER_WORDS[layer], **aggregate(ls, windows)})
        out.append(row)
    return out


# ------------------------------------------------------------------------------------------------------- stress
def long_window(windows, as_of):
    """The window that stands for the build-up: the year to date once it is LONG_MIN_DAYS long, else three months, else none."""
    for key in ("ytd", "m3"):
        ref = windows.get(key)
        if ref and days_between(ref, as_of) >= LONG_MIN_DAYS:
            return key
    return None


def standings(pairs):
    """{id: 0..100} - where each value stands among the others, ties sharing the middle of their places. One value has no standing."""
    vals = sorted(v for _, v in pairs)
    n = len(vals)
    if n < 2:
        return {}
    out = {}
    for ident, v in pairs:
        lo = hi = None
        a, b = 0, n
        while a < b:                                   # first place of v
            m = (a + b) // 2
            if vals[m] < v:
                a = m + 1
            else:
                b = m
        lo = a
        a, b = lo, n
        while a < b:                                   # one past its last place
            m = (a + b) // 2
            if vals[m] <= v:
                a = m + 1
            else:
                b = m
        hi = a
        out[ident] = round(100.0 * (lo + hi - 1) / 2.0 / (n - 1), 1)
    return out


def add_stress(rows, windows, as_of):
    """Give every company its three standings inside its own book and their average. Returns the long window used."""
    long = long_window(windows, as_of)
    for book in sorted({r["book"] for r in rows}):
        rs = [r for r in rows if r["book"] == book]
        level = standings([(r["id"], r["spread_bp"]) for r in rs if r["spread_bp"] is not None])
        year = standings([(r["id"], r["win"][long]["d_pct"]) for r in rs if long and long in r["win"] and r["win"][long].get("d_pct") is not None])
        month = standings([(r["id"], r["win"][PRIMARY]["d_bp"]) for r in rs if PRIMARY in r["win"] and r["win"][PRIMARY].get("d_bp") is not None])
        for r in rs:
            r["rk_level"], r["rk_year"], r["rk_month"] = level.get(r["id"]), year.get(r["id"]), month.get(r["id"])
            moved = [x for x in (r["rk_year"], r["rk_month"]) if x is not None]
            r["stress"] = round((r["rk_level"] + sum(moved)) / (1 + len(moved)), 1) if r["rk_level"] is not None and moved else None
    return long


def book_levels(rows):
    """{book: the middle company's spread} - what a company's own spread is a multiple of."""
    out = {}
    for book in sorted({r["book"] for r in rows}):
        vals = sorted(r["spread_bp"] for r in rows if r["book"] == book and r["spread_bp"] is not None)
        if vals:
            n = len(vals)
            out[book] = f(vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2.0, 1)
    return out


def available_from(as_of, days, aim, tol):
    """The first holdings day on which a stored day will be `aim` days back, give or take `tol`; None when no stored day can be."""
    best = None
    for d in days:
        back = days_between(d, as_of)
        if back > aim + tol:
            continue                                   # already too old: it will never be near enough again
        need = max((aim - tol) - back, 1)
        day = date.fromordinal(date.fromisoformat(as_of).toordinal() + need).isoformat()
        if best is None or day < best:
            best = day
    return best


# ------------------------------------------------------------------------------------------------------- the stock
def price_change(closes, ref, last_days=6):
    """(change % from the last close on or before `ref` to the newest close, the two dates). closes: {day: close}."""
    if not closes or not ref:
        return None, None, None
    days = sorted(closes)
    before = [d for d in days if d <= ref]
    if not before or days_between(before[-1], ref) > last_days:
        return None, None, days[-1]
    a, b = closes[before[-1]], closes[days[-1]]
    if not a or not b or a <= 0:
        return None, before[-1], days[-1]
    return round(100.0 * (b / a - 1.0), 2), before[-1], days[-1]


# ------------------------------------------------------------------------------------------------------- the CDS
def cds_rows(desk, matcher):
    """Every corporate reference entity of the CDS desk, tied to a listed company where one exists.

    Returns (priced rows, names that trade with no level, {ticker: best priced row}, {issuer key: priced row}).
    """
    priced, bare, by_ticker, by_key = [], [], {}, {}
    for group, foreign in (("us_corp", False), ("global_corp", True)):
        blk = ((desk or {}).get("groups") or {}).get(group) or {}
        for kind in ("rows", "unpriced", "dormant"):
            for x in blk.get(kind) or []:
                if not isinstance(x, dict) or not x.get("name"):
                    continue
                key, _ = N.issuer_key(x["name"])
                co, how = matcher.match(key, False, None, foreign=foreign)
                base = {"name": x["name"], "ticker": co["symbol"] if co else None, "company": co["name"] if co else None, "sector": x.get("sector"),
                        "region": "US" if group == "us_corp" else "Global", "ccy": x.get("ccy"), "key": key}
                if kind != "rows" or not isinstance(x.get("spread_bp"), (int, float)):
                    bare.append({**base, "status": "dormant" if kind == "dormant" else "trades, no level", "trades_30d": x.get("trades_30d"), "last_date": x.get("last_date")})
                    continue
                row = {**base, "spread_bp": f(x.get("spread_bp"), 1), "d1d": f(x.get("chg_1d_bp"), 1), "d1w": f(x.get("chg_1w_bp"), 1), "d1m": f(x.get("chg_1m_bp"), 1),
                       "d3m": f(x.get("chg_3m_bp"), 1), "last_date": x.get("last_date"), "stale_days": x.get("stale_days"), "liquid": bool(x.get("liquid")),
                       "trades_30d": x.get("trades_30d"), "rank_1y": f(x.get("pct_rank_1y"), 1), "hi_1y": f(x.get("hi_1y_bp"), 1), "lo_1y": f(x.get("lo_1y_bp"), 1),
                       "points_upfront": f(x.get("points_upfront"), 2), "believable": bool(x["spread_bp"] <= CDS_BELIEVABLE_BP)}
                if not row["believable"]:
                    row.update({"d1d": None, "d1w": None, "d1m": None, "d3m": None})
                priced.append(row)
                rank = (row["liquid"], row["trades_30d"] or 0)
                if row["ticker"]:
                    cur = by_ticker.get(row["ticker"])
                    if cur is None or rank > (cur["liquid"], cur["trades_30d"] or 0):
                        by_ticker[row["ticker"]] = row
                cur = by_key.get(key)
                if cur is None or rank > (cur["liquid"], cur["trades_30d"] or 0):
                    by_key[key] = row
    priced.sort(key=lambda r: (-(r["d1m"] if r["d1m"] is not None else -1e9), r["name"]))
    return priced, bare, by_ticker, by_key


def cds_direction(row):
    """WIDER / TIGHTER / FLAT for a traded CDS over its month, or None when it cannot be said."""
    if not row or not row.get("liquid") or row.get("d1m") is None or row.get("spread_bp") is None or row.get("believable") is False:
        return None
    was = row["spread_bp"] - row["d1m"]
    bar = max(CDS_MOVE_BP, CDS_MOVE_REL * max(was, 0.0))
    return "WIDER" if row["d1m"] >= bar else "TIGHTER" if row["d1m"] <= -bar else "FLAT"


# ------------------------------------------------------------------------------------------------------- the rule
def credit_direction(win):
    """(direction, the bar it had to pass) from one window of a company row."""
    if not win or win.get("x_bp") is None or win.get("from_bp") is None:
        return None, None
    bar = max(MOVE_FLOOR_BP, MOVE_REL * max(win["from_bp"], 0.0))
    d = win.get("d_bp") or 0.0
    # past its book AND in that direction itself: a spread that widened less than its book has held up, it has not improved
    return ("DETERIORATING" if win["x_bp"] >= bar and d >= OWN_MOVE_BP else "IMPROVING" if win["x_bp"] <= -bar and d <= -OWN_MOVE_BP else "FLAT"), round(bar, 1)


def signal_of(direction, d_price_pct):
    """The board's verdict from credit's direction and the stock's change over the same days."""
    if direction is None:
        return "INSUFFICIENT_HISTORY", None
    if d_price_pct is None:
        return "NONE", None
    if direction == "DETERIORATING":
        state = "confirmed" if d_price_pct <= -FLAT_BAND_PCT else "opposite" if d_price_pct >= FLAT_BAND_PCT else "flat"
        return ("NONE" if state == "confirmed" else "CREDIT_LEADS_DOWN"), state
    if direction == "IMPROVING":
        state = "confirmed" if d_price_pct >= FLAT_BAND_PCT else "opposite" if d_price_pct <= -FLAT_BAND_PCT else "flat"
        return ("NONE" if state == "confirmed" else "CREDIT_LEADS_UP"), state
    return "NONE", ("flat" if abs(d_price_pct) <= FLAT_BAND_PCT else "moved")


CELLS = {"worse/down": "Credit worse, stock already fell", "worse/flat": "Credit worse, stock flat", "worse/up": "Credit worse, stock rose",
         "flat/down": "Credit unmoved, stock fell", "flat/flat": "Both quiet", "flat/up": "Credit unmoved, stock rose",
         "better/down": "Credit better, stock fell", "better/flat": "Credit better, stock flat", "better/up": "Credit better, stock already rose"}


def cell_of(direction, d_price_pct):
    """Where a company stands on the board's two questions - what credit did past its book, what the stock did over the same
    days - as one of nine cells, "credit/stock". The two leads are four of them; the other five the board used to call NONE."""
    credit = {"DETERIORATING": "worse", "IMPROVING": "better", "FLAT": "flat"}.get(direction)
    if credit is None or d_price_pct is None:
        return None
    return credit + "/" + ("down" if d_price_pct <= -FLAT_BAND_PCT else "up" if d_price_pct >= FLAT_BAND_PCT else "flat")


def implied_default(spread_bp, years=1.0, recovery=RECOVERY):
    """The chance of default inside `years` that a spread prices, in percent, if a default pays `recovery` of face: the
    textbook reading 1 - exp(-spread x years / (1 - recovery)). It is what the price implies to a buyer who is paid for
    nothing but default - an upper reading, since a spread also pays for illiquidity and risk; not a forecast."""
    if spread_bp is None or spread_bp <= 0:
        return None
    return round(100.0 * (1.0 - math.exp(-(spread_bp / 10000.0) * years / (1.0 - recovery))), 1)


RULES = {
    "spread": "A bond's yield minus the Treasury par yield at the bond's duration-matched maturity, in basis points. Not an option-adjusted spread.",
    "company": "The bonds of every legal issuer tied to one listed company (or of one issuer when none is tied), weighted by the market value the scanned funds hold.",
    "change": "The same bonds at both dates; a bond seen at only one of them is left out.",
    "industry": "The fund file's sector. Its change is the middle company's change, each company weighted by market value held.",
    "credit_moved": "A company's spread moved, and moved past its own book's change (investment grade, high yield, emerging markets) by at least %g bp and at least %g%% of the spread it started from, its own spread having moved %g bp or more the same way. A spread that only widened less than its book has held up; it has not improved." % (MOVE_FLOOR_BP, MOVE_REL * 100, OWN_MOVE_BP),
    "stock_has_not_followed": "From the reference day's close to the newest close the stock is not %g%% or more in the direction credit points." % FLAT_BAND_PCT,
    "stress": "A company's stress rank is the average of where it stands in its own book (investment grade, high yield, emerging markets) on three things: how wide its spread is, how far it has widened over the long window as a share of where it started, and how far it widened in the month. 0 to 100; %g or more is shown as under stress. It does not look at the stock." % STRESSED,
    "no_lead_when_credit_disagrees": "When a liquid traded CDS moved the opposite way to the bonds over the month, the two credit markets disagree and no lead is called.",
    "theme": "A theme is a reviewed list of companies across industries. The AI stack is the AI infrastructure stack engine's layers and tickers, with the hyperscalers, the server makers and the financing vehicles of AI computing sites.",
    "cds": "A traded CDS level is the CDS desk's (DTCC public prints). It moves when its one-month change is past %g bp and %g%% of its level, and only a liquid name is read. A level past %g bp is a points-upfront quote turned into a running spread: it is shown in points and not ranked." % (CDS_MOVE_BP, CDS_MOVE_REL * 100, CDS_BELIEVABLE_BP),
    "left_out": "Bonds not in US dollars, floating-rate and convertible bonds, bonds under a year to run, bonds whose yield the fund file does not print, prints at its limit or prints at zero or less (priced to an early call), and government or local-authority issuers.",
}
