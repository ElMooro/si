"""A listing the reference list types as common stock that is another security of the same issuer (factor book v1.0.9).

Polygon's active-ticker list types AGNCO, HBANL, ATLCL, BHFAL ... as "CS".  They are preferred shares, baby bonds and
notes of AGNC, Huntington, Atlanticus, Brighthouse; the book carried them as common stock, so every common-only screen
and board could rank a note beside a company (ops 6852, ops 6859).

Two kinds of evidence, both from the listing itself:
  by name       the listing's own name states the instrument: a coupon ("6.125%"), "Notes", "Debentures", "SR NT",
                "Preferred Shares", "Depositary Shares ... Preferred", "Perpetual", "Cumulative";
  by structure  a five-letter NASDAQ ticker that extends a listed three- or four-letter ticker of the same issuer name
                with one of NASDAQ's fifth letters for a second issue (G-J a convertible bond, L and Z a miscellaneous
                issue such as a baby bond, M-P a preferred share, W a warrant) AND trades under a tenth of that
                ticker's dollar volume.  A share class (GOOGL beside GOOG, FOXA beside FOX, NWSA beside NWS) trades as
                much as its sibling or more and stays common stock.

Pure: no I/O, no clock.
"""
import re

NAMED_NOTE = re.compile(r"\d(\.\d+)?\s*%|\bnotes?\b|\bdebentures?\b|\bsr\.?\s+nt\b|\b\dm\s+bd\b", re.I)       # a coupon, "Senior Notes due 2031", "SR NT", "1M BD"
NAMED_PREFERRED = re.compile(r"\bpreferred\s+(stock|shares?|units?|securities|l\.?p\.?)|\bdepositary\s+shares?\b.*\bpreferred\b|\bperpetual\b|\bcumulative\b", re.I)
NASDAQ_FIFTH_LETTER = {"G": "is_debt_or_structured", "H": "is_debt_or_structured", "I": "is_debt_or_structured", "J": "is_debt_or_structured",
                       "L": "is_debt_or_structured", "Z": "is_debt_or_structured",
                       "M": "is_preferred", "N": "is_preferred", "O": "is_preferred", "P": "is_preferred", "W": "is_warrant_unit"}
OTHER_ISSUE_DV_SHARE = 0.10                    # ... when the line trades under a tenth of the dollar volume of the ticker it extends
FLAG_WORD = {"is_debt_or_structured": "a note or bond", "is_preferred": "a preferred share", "is_warrant_unit": "a warrant"}


def _num(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v if v == v else None


def find(rows, dollar_vol=None):
    """{symbol: {"flag": identity flag to set, "why": reason, "base": ticker it extends or None}} for the rows that are
    common stock by the book's flags (is_common true) and are another security by the evidence above.

    rows: identity dicts (symbol, name, exchange, is_common).  dollar_vol: {symbol: 20-session average dollar volume};
    without it the structure test cannot run and only names are read."""
    common = [r for r in rows if r.get("is_common") and r.get("symbol")]
    by_name = {}
    for r in common:
        key = str(r.get("name") or "").strip().lower()
        if key:
            by_name.setdefault(key, []).append(str(r["symbol"]).upper())
    dv = dollar_vol or {}
    out = {}
    for r in common:
        sym, name = str(r["symbol"]).upper(), str(r.get("name") or "")
        if NAMED_NOTE.search(name):
            out[sym] = {"flag": "is_debt_or_structured", "why": "a note by its own name", "base": None}
        elif NAMED_PREFERRED.search(name):
            out[sym] = {"flag": "is_preferred", "why": "a preferred share by its own name", "base": None}
        elif dv and len(sym) == 5 and sym.isalpha() and sym[-1] in NASDAQ_FIFTH_LETTER and "NASDAQ" in str(r.get("exchange") or "").upper() and name.strip():
            mine = _num(dv.get(sym)) or 0.0
            for base in by_name.get(name.strip().lower(), []):
                if base != sym and len(base) in (3, 4) and sym.startswith(base) and mine < OTHER_ISSUE_DV_SHARE * (_num(dv.get(base)) or 0.0):
                    flag = NASDAQ_FIFTH_LETTER[sym[-1]]
                    out[sym] = {"flag": flag, "why": "%s of %s (NASDAQ fifth letter %s, under a tenth of its dollar volume)" % (FLAG_WORD[flag], base, sym[-1]), "base": base}
                    break
    return out


def apply(universe, dollar_vol=None):
    """Set the flag and clear is_common on every row find() names.  universe: {symbol: identity dict}, changed in
    place.  Returns find()'s answer (the reasons are published in the book's manifest, coverage.other_issues)."""
    hits = find(list(universe.values()), dollar_vol)
    for sym, hit in hits.items():
        ident = universe.get(sym) or next((u for u in universe.values() if str(u.get("symbol") or "").upper() == sym), None)
        if ident is None:
            continue
        ident[hit["flag"]] = True
        ident["is_common"] = False
    return hits
