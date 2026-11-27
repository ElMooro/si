"""Credit Before Equity, the fund layer: who holds each company's bonds and loans, how far apart they mark them, and the
credit default swaps funds hold on it. Arithmetic only: no bucket, no file.

Khalid 2026-11-26: "go" - Credit Before Equity first: who holds each bond, how far apart funds mark it, and how funds are
positioned in each company's credit default swaps.

The source is the SEC's Form N-PORT data sets as justhodl-sec-datasets reads them (one row a security across every
registered fund that holds it, no identifier). This module ties those rows to the board's companies by name:

    issuer      the security's issuer is written the way an issuer already on the board is
    cut name    the board's own issuer name was cut short by the fund file it came from, and this one starts with it
    exact, alias, core, subsidiary, truncated    the board's own naming rules (cbe_names.Matcher), used as they are
    short name  funds that file under 24-character market names abbreviate ("GENERAL MOTORS FINL CO", "PHILIP MORRIS INTL
                INC"): the abbreviations are spelled out from a fixed list, and the name must then be one the rules above tie
    co-issuer   two issuers written with a stroke and no space ("CCO HLDGS LLC/CAP CORP"): the first is the company
    name cut    the fund's own name was cut at 24 characters and exactly one company on the board has an issuer that starts
                with it ("HILTON DOMESTIC OPERATIN")

A name that more than one company answers to is tied to none. Every tie not made by the issuer's own name is counted and
listed in the block (ties_by_rule), so a wrong one can be seen.

and adds, for every company it can tie: how many of its bonds and loans funds hold, the most funds in any one of them,
face and value, the value-weighted middle mark, the widest gap between the 10th and 90th percentile mark of one bond,
how many of its securities most positions flag in default or paid in kind, and what funds have sold and bought of
protection on its name.

What this is not: fund holdings are as of each fund's own report date, two to five months before the SEC posts them. A
mark is a fund's own valuation, not a trade. None of it enters the board's spreads, signals, stress rank or chances.
"""
from bisect import bisect_left
from collections import Counter, defaultdict
from itertools import product
import re

import cbe_names as N

VERSION = "1.0.0"
COLS = ["f_n", "f_funds", "f_face", "f_value", "f_px", "f_gap", "f_def", "f_pik", "f_loans", "fc_sold", "fc_bought", "fc_net", "fc_funds"]
ROW_COLS = ["co", "issuer", "title", "cls", "cpn", "mat", "funds", "rows", "face", "value", "px", "px_lo", "px_hi", "n_px", "n_default", "n_arrears", "n_pik",
            "n_restricted", "n_l3", "d"]
CDS_COLS = ["reference", "co", "ticker", "positions", "funds", "sold", "bought", "unclear", "net_sold", "not_in_dollars", "usual_end_year", "dealer"]
GAP_COLS = ["co", "name", "title", "cpn", "mat", "funds", "n_px", "px", "px_lo", "px_hi", "gap", "value"]
FLAG_COLS = ["co", "name", "title", "cpn", "mat", "funds", "rows", "n_default", "n_pik", "px", "value"]
CLASSES = ("DBT", "LON")                              # a company's own bonds and loans; asset-backed deals are their own issuers
NOT_COMPANIES = ("MUN", "UST", "USGA", "USGSE", "NUSS")   # municipal, Treasury, agency and foreign-government issuers are not on this board
MIN_MARKS = 5                                         # a gap between marks is read off five funds or more
CUT_AT = 20                                           # a board issuer name this long may have been cut short by its fund file
LIST_N = 100
MARKET_NAME = (23, 24)                                # a market name is cut at 24 characters (23 when the 24th was a space)
HEAD_AT = 14                                          # a fund's cut name must be this long, and two words, to stand for the issuer it starts
OWN_NAME = ("issuer", "cut name")                     # ties made by the name an issuer on the board is written by
# The abbreviations of 24-character market names, spelled out. A word with two readings is tried both ways, and a name that
# then answers to two companies is tied to neither. None: the word is dropped (it is part of the legal form).
SPELL = {
    "INTL": ("INTERNATIONAL",), "INTERNATL": ("INTERNATIONAL",), "INT": ("INTERNATIONAL", "INTERMEDIATE"), "FINL": ("FINANCIAL",), "FIN": ("FINANCE", "FINANCIAL"),
    "HLDGS": ("HOLDINGS",), "HLDG": ("HOLDING", "HOLDINGS"), "HOLD": ("HOLDINGS", "HOLDING"), "GRP": ("GROUP",), "GP": ("GROUP",), "NATL": ("NATIONAL",),
    "COMM": ("COMMUNICATIONS",), "COMMUN": ("COMMUNICATIONS",), "COMMS": ("COMMUNICATIONS",), "TECH": ("TECHNOLOGIES", "TECHNOLOGY"), "SYS": ("SYSTEMS", "SYSTEM"),
    "ELEC": ("ELECTRIC",), "PWR": ("POWER",), "PETE": ("PETROLEUM",), "PHARM": ("PHARMACEUTICALS", "PHARMACEUTICAL"), "MTR": ("MOTOR",), "NY": ("NEW YORK",),
    "CAL": ("CALIFORNIA",), "MGMT": ("MANAGEMENT",), "SVCS": ("SERVICES",), "SERV": ("SERVICES",), "SVC": ("SERVICE", "SERVICES"), "INDS": ("INDUSTRIES",),
    "LABS": ("LABORATORIES",), "ENTERTAIN": ("ENTERTAINMENT",), "ENTMT": ("ENTERTAINMENT",), "RES": ("RESOURCES",), "INS": ("INSURANCE",), "BNCRP": ("BANCORP",),
    "OPER": ("OPERATING",), "OPT": ("OPERATING",), "CAP": ("CAPITAL",), "FNDG": ("FUNDING",), "FDG": ("FUNDING",), "UTIL": ("UTILITIES",), "HLTH": ("HEALTH",),
    "INFO": ("INFORMATION",), "PRODS": ("PRODUCTS",), "DEV": ("DEVELOPMENT",), "INVT": ("INVESTMENT",), "COOP": ("COOPERATIVE",), "MKTS": ("MARKETS",), "ENER": ("ENERGY",),
    "PPTY": ("PROPERTY", "PROPERTIES"), "PROP": ("PROPERTIES", "PROPERTY"), "RLTY": ("REALTY",), "TR": ("TRUST",), "ASSD": ("ASSOCIATED",), "COS": None,
}


class Ties:
    """Names as funds write them, tied to the board's companies. Built once a run from the board's own ties."""

    def __init__(self, ties, matcher, on_board):
        self.matcher, self.on_board, self.cache = matcher, set(on_board), {}
        self.exact = {t["key"]: ("T:" + t["ticker"]) if t.get("ticker") else ("I:" + t["key"]) for t in ties if t.get("key")}
        self.heads = defaultdict(list)
        for key in self.exact:
            if len(key) >= CUT_AT:
                self.heads[key[:CUT_AT]].append(key)
        self.sorted = sorted(self.exact)

    def of(self, name, country=None):
        """(company id or None, how, listed ticker when the company is listed but not on the board)."""
        raw = re.sub(r"\s+", " ", str(name or "").upper()).strip()
        hit = self.cache.get((raw, country))
        if hit is None:
            hit = self.cache[(raw, country)] = self._of(raw, country)
        return hit

    def _own(self, key, country):
        """The board's own ways of knowing a name: an issuer it holds, one it holds cut short, its naming rules."""
        if not key:
            return None, "no name", None
        if key in self.exact:
            return self.exact[key], "issuer", None
        longest = max((k for k in self.heads.get(key[:CUT_AT], ()) if key.startswith(k)), key=len, default=None)
        if longest:
            return self.exact[longest], "cut name", None
        co, how = self.matcher.match(key, False, None, foreign=bool(country) and country != "US")
        if not co:
            return None, how, None
        ident = "T:" + co["symbol"]
        return (ident, how, None) if ident in self.on_board else (None, "listed, not on the board", co["symbol"])

    def _starts(self, key, whole_words):
        """The companies with an issuer on the board whose name starts with this one. whole_words: the fund's name is as long
        as a market name gets, so it may have been cut between two words; otherwise only a name cut inside a word counts
        ("APLD COMPUTECO" is not "APLD COMPUTECO 2" cut short)."""
        out, i = set(), bisect_left(self.sorted, key)
        while i < len(self.sorted) and self.sorted[i].startswith(key):
            if whole_words or self.sorted[i][len(key):len(key) + 1] != " ":
                out.add(self.exact[self.sorted[i]])
            i += 1
        return out

    def _of(self, raw, country):
        key = N.norm(N.clean_issuer(raw))
        first = self._own(key, country)
        if first[0] or not key:
            return first
        # the name as a fund that abbreviates writes it: spelled out, the first of two co-issuers, or cut at 24 characters
        tries = [(k, "short name") for k in spelled(key)]
        if "/" in raw and " / " not in raw:
            parts = [N.norm(N.clean_issuer(p)) for p in raw.split("/")]
            lead = parts[0] if len(parts[0]) >= 5 or len(parts) < 2 else parts[1]      # "CHS/COMMUNITY HEALTH SYS": the short form first, then the name
            if lead and lead != key:
                tries += [(lead, "co-issuer")] + [(k, "co-issuer") for k in spelled(lead)]
        found, listed = {}, first[2]
        for k, label in tries:
            co, how, tk = self._own(k, country)
            if co:
                found.setdefault(co, label)
            listed = listed or tk
        if not found:
            for k, label in [(key, "name cut")] + tries:
                if len(k) >= HEAD_AT and len(k.split()) >= 2:
                    for co in self._starts(k, len(raw) in MARKET_NAME):
                        found.setdefault(co, "name cut")
                    if found:
                        break
        if len(found) == 1:
            co, label = next(iter(found.items()))
            return co, label, None
        if len(found) > 1:
            return None, "more than one company answers to this short name", None
        return (None, "listed, not on the board", listed) if listed else first


def spelled(key):
    """A name with the abbreviations of market names spelled out: every reading of it, none when it holds no abbreviation."""
    toks = key.split()
    if not any(t in SPELL for t in toks):
        return []
    choices = [(SPELL[t] or ("",)) if t in SPELL else (t,) for t in toks]
    out = []
    for combo in product(*choices):
        k = " ".join(" ".join(w for w in combo if w).split())
        if k and k != key and k not in out:
            out.append(k)
        if len(out) >= 16:
            break
    return out


_TAIL = re.compile(r"( (LLC|INC|NA|NATIONAL ASSOCIATION|LTD|LIMITED|LP))+$")


def plain(name):
    """A name as a dealer or a reference is written, without its punctuation and its legal tail: 'Goldman Sachs & Co. LLC (gov repo)' -> 'GOLDMAN SACHS AND CO'."""
    t = re.sub(r"\([^)]*\)", " ", (name or "").upper()).replace("&", " AND ")
    t = re.sub(r"\bCOMPANY\b", "CO", re.sub(r"[^A-Z0-9]+", " ", t)).strip()
    return _TAIL.sub("", t).strip()


def _rows(table):
    cols = table.get("cols") or []
    return [dict(zip(cols, r)) for r in table.get("rows") or [] if isinstance(r, list) and len(r) == len(cols)]


def _most(rows, flag):
    """Securities where most of the positions funds report carry the flag."""
    return sum(1 for b in rows if b.get("rows") and (b.get(flag) or 0) * 2 >= b["rows"] and (b.get(flag) or 0) > 0)


def join(bonds, cds, ties, names):
    """The fund layer for one board.

    bonds, cds: the N-PORT tables of justhodl-sec-datasets (their documents, as published). ties: Ties.
    names: {company id: the name the board shows}. Returns {"by_co": {id: {column: value}}, "rows": [...], "block": {...}}.
    """
    held, read, how_n, how_v = defaultdict(list), [0, 0.0], Counter(), Counter()
    untied, absent, by_rule = defaultdict(lambda: [0, 0.0, 0]), defaultdict(lambda: [0, 0.0, 0, ""]), {}
    for b in _rows(bonds):
        if b.get("cls") not in CLASSES or b.get("itype") in NOT_COMPANIES:
            continue
        value = b.get("value") or 0
        read[0] += 1
        read[1] += value
        co, how, listed = ties.of(b.get("issuer"), b.get("ctry"))
        if co:
            held[co].append(b)
            how_n[how] += 1
            how_v[how] += value
            if how not in OWN_NAME:
                r = by_rule.setdefault((N.norm(N.clean_issuer(b.get("issuer"))), co), [b.get("issuer"), how, co, 0, 0.0])
                r[3] += 1
                r[4] += value
        elif listed:
            a = absent[listed]
            a[0] += 1
            a[1] += value
            a[2] = max(a[2], b.get("funds") or 0)
            a[3] = a[3] or (b.get("issuer") or "")
        else:
            u = untied[(b.get("issuer") or "").upper()[:60]]
            u[0] += 1
            u[1] += value
            u[2] = max(u[2], b.get("funds") or 0)

    by_co, out_rows, gaps, flags = {}, [], [], []
    for co, rows in held.items():
        rows.sort(key=lambda b: -(b.get("value") or 0))
        marked = [b for b in rows if b.get("px") is not None and (b.get("value") or 0) > 0]
        weight = sum(b["value"] for b in marked)
        wide = [b for b in rows if (b.get("n_px") or 0) >= MIN_MARKS and b.get("px_hi") is not None and b.get("px_lo") is not None]
        by_co[co] = {"f_n": len(rows), "f_funds": max((b.get("funds") or 0) for b in rows), "f_face": int(sum(b.get("face") or 0 for b in rows)),
                     "f_value": int(sum(b.get("value") or 0 for b in rows)), "f_px": round(sum(b["px"] * b["value"] for b in marked) / weight, 2) if weight else None,
                     "f_gap": round(max(b["px_hi"] - b["px_lo"] for b in wide), 2) if wide else None, "f_def": _most(rows, "n_default"), "f_pik": _most(rows, "n_pik"),
                     "f_loans": sum(1 for b in rows if b.get("cls") == "LON")}
        wide_ids = {id(b) for b in wide}
        for b in rows:
            out_rows.append([co] + [b.get(c) for c in ROW_COLS[1:]])
            if id(b) in wide_ids:
                gaps.append([co, names.get(co), b.get("title"), b.get("cpn"), b.get("mat"), b.get("funds"), b.get("n_px"), b.get("px"), b.get("px_lo"), b.get("px_hi"),
                             round(b["px_hi"] - b["px_lo"], 2), b.get("value")])
            if _most([b], "n_default") or _most([b], "n_pik"):
                flags.append([co, names.get(co), b.get("title"), b.get("cpn"), b.get("mat"), b.get("funds"), b.get("rows"), b.get("n_default"), b.get("n_pik"), b.get("px"), b.get("value")])

    # credit default swaps funds hold, by reference name: tied to a company where the name is one
    # A fund sometimes writes the dealer it faces where the reference belongs. A reference that is also a dealer's name is tied to a
    # company only when a bond issuer on the board is written that way; otherwise it stays in the list, marked, and enters no company.
    cds_cols = cds.get("cols") or []
    cds_names, n_tied, n_dealer = [], 0, 0
    dealers = {plain(c[0]) for c in cds.get("counterparties") or [] if c and c[0]} - {""}
    swaps = defaultdict(lambda: {"fc_sold": 0, "fc_bought": 0, "fc_funds": 0})
    for row in cds.get("names") or []:
        x = dict(zip(cds_cols, row))
        co, how, listed = ties.of(x.get("reference"))
        dealer = 1 if plain(x.get("reference")) in dealers else 0
        if dealer and how not in OWN_NAME:
            co, listed = None, None
            n_dealer += 1
        if co:
            n_tied += 1
            s = swaps[co]
            s["fc_sold"] += x.get("sold") or 0
            s["fc_bought"] += x.get("bought") or 0
            s["fc_funds"] = max(s["fc_funds"], x.get("funds") or 0)
        cds_names.append([x.get("reference"), co, co[2:] if co and co.startswith("T:") else listed, x.get("positions"), x.get("funds"), x.get("sold"), x.get("bought"),
                          x.get("unclear"), x.get("net_sold"), x.get("not_in_dollars"), x.get("usual_end_year"), dealer])
    for co, s in swaps.items():
        s["fc_net"] = s["fc_sold"] - s["fc_bought"]
        by_co.setdefault(co, {}).update(s)

    gaps.sort(key=lambda g: (-g[10], -(g[11] or 0)))
    flags.sort(key=lambda g: -(g[10] or 0))
    order = {co: i for i, co in enumerate(names)}
    out_rows.sort(key=lambda r: (order.get(r[0], 1e9), -(r[9] or 0)))
    tied_n, tied_v = sum(how_n.values()), sum(how_v.values())
    block = {
        "version": VERSION, "as_of": bonds.get("as_of"), "file": bonds.get("source_file"), "cols": ROW_COLS, "n": len(out_rows), "company_cols": COLS,
        "read": {"securities": read[0], "value": round(read[1])},
        "tied": {"securities": tied_n, "value": round(tied_v), "value_pct": round(100.0 * tied_v / read[1], 1) if read[1] else None, "companies": len(held),
                 "listed": sum(1 for co in held if co.startswith("T:")), "how": {k: [how_n[k], round(how_v[k])] for k in sorted(how_n, key=lambda k: -how_v[k])}},
        "ties_by_rule": [[r[0], r[1], r[2], names.get(r[2]), r[3], round(r[4])] for r in sorted(by_rule.values(), key=lambda r: -r[4])[:LIST_N]], "n_ties_by_rule": len(by_rule),
        "not_on_board": [[t, a[3], a[0], round(a[1]), a[2]] for t, a in sorted(absent.items(), key=lambda kv: -kv[1][1])[:LIST_N]],
        "n_not_on_board": len(absent),
        "untied": [[k, u[0], round(u[1]), u[2]] for k, u in sorted(untied.items(), key=lambda kv: -kv[1][1])[:LIST_N]], "n_untied": len(untied),
        "gap_cols": GAP_COLS, "gaps": gaps[:LIST_N], "n_gaps": len(gaps), "min_marks": MIN_MARKS,
        "flag_cols": FLAG_COLS, "flagged": flags, "n_flagged": len(flags),
        "cds": {"as_of": cds.get("as_of"), "cols": CDS_COLS, "names": cds_names, "tied": n_tied, "dealer_names_not_tied": n_dealer, "index_cols": cds_cols, "indices": cds.get("indices") or [],
                "counterparties": cds.get("counterparties") or [], "totals": cds.get("totals"), "reading": cds.get("reading")},
        "reading": "Fund holdings are as of each fund's own report date, two to five months before the SEC posts them; a mark is a fund's own valuation of what it holds, "
                   "per 100 of face, not a trade. Nothing here enters the board's spreads, signals, stress rank or chances.",
    }
    review = [[r[0], r[1], r[2], names.get(r[2]), r[3], round(r[4])] for r in sorted(by_rule.values(), key=lambda r: -r[4])]      # every tie by a rule, for a dry run to read; not published
    return {"by_co": by_co, "rows": out_rows, "block": block, "review": review}


def add_columns(issuers, by_co):
    """The board's company table with the fund columns after its own: every earlier column keeps its place."""
    cols = list(issuers["cols"]) + [c for c in COLS if c not in issuers["cols"]]
    at = issuers["cols"].index("id")
    rows = [list(r) + [(by_co.get(r[at]) or {}).get(c) for c in COLS if c not in issuers["cols"]] for r in issuers["rows"]]
    return {"cols": cols, "rows": rows}
