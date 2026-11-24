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
"""
from collections import Counter, defaultdict
from datetime import date
import math

import cbe_names as N

VERSION = "2.0.0"
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
             "v": float(b.get("demand") or 0.0), "y": b.get("rate"), "dur": b.get("dur"), "t": None, "s": None, "ref": {}, "why": None}
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
        left = None
        try:
            left = days_between(as_of, str(b.get("maturity"))[:10]) / 365.25
        except (TypeError, ValueError):
            left = None
        t = tenor_of(dur, y, left)
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
            "n_limit": sum(1 for p in pts if p["why"] == "yield at the file's limit"),
        }
        row["distressed"] = bool(row["spread_bp"] is not None and row["spread_bp"] >= DISTRESSED_BP) or (row["n_limit"] > 0 and not good)
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


def industries(rows, windows, book=None):
    """The heat map's rows: one per industry, for all books or one of them."""
    per = defaultdict(list)
    for r in rows:
        if r.get("industry") and (book is None or r["book"] == book) and r["n_bonds"]:
            per[r["industry"]].append(r)
    out = []
    for name, rs in per.items():
        row = {"industry": name, "n": len(rs), "n_listed": sum(1 for r in rs if r["ticker"]), "value": round(sum(r["value"] for r in rs), 0),
               "spread_bp": f(weighted_median([(r["spread_bp"], r["value"]) for r in rs if r["spread_bp"] is not None]), 1),
               "n_distressed": sum(1 for r in rs if r["distressed"]), "win": {}}
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
        out.append(row)
    out.sort(key=lambda r: (-(r["value"] or 0), r["industry"]))
    return out


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


RULES = {
    "spread": "A bond's yield minus the Treasury par yield at the bond's duration-matched maturity, in basis points. Not an option-adjusted spread.",
    "company": "The bonds of every legal issuer tied to one listed company (or of one issuer when none is tied), weighted by the market value the scanned funds hold.",
    "change": "The same bonds at both dates; a bond seen at only one of them is left out.",
    "industry": "The fund file's sector. Its change is the middle company's change, each company weighted by market value held.",
    "credit_moved": "A company's spread moved, and moved past its own book's change (investment grade, high yield, emerging markets) by at least %g bp and at least %g%% of the spread it started from, its own spread having moved %g bp or more the same way. A spread that only widened less than its book has held up; it has not improved." % (MOVE_FLOOR_BP, MOVE_REL * 100, OWN_MOVE_BP),
    "stock_has_not_followed": "From the reference day's close to the newest close the stock is not %g%% or more in the direction credit points." % FLAT_BAND_PCT,
    "cds": "A traded CDS level is the CDS desk's (DTCC public prints). It moves when its one-month change is past %g bp and %g%% of its level, and only a liquid name is read. A level past %g bp is a points-upfront quote turned into a running spread: it is shown in points and not ranked." % (CDS_MOVE_BP, CDS_MOVE_REL * 100, CDS_BELIEVABLE_BP),
    "left_out": "Bonds not in US dollars, floating-rate and convertible bonds, bonds under a year to run, bonds whose yield the fund file does not print, prints at its limit or prints at zero or less (priced to an early call), and government or local-authority issuers.",
}
