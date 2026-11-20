"""A listing the reference list types as common stock that is another security of the same issuer (factor book v1.0.9).

Polygon's active-ticker list types AGNCO, HBANL, ATLCL, BHFAL ... as "CS".  They are preferred shares, baby bonds and
notes of AGNC, Huntington, Atlanticus, Brighthouse; the book carried them as common stock, so every common-only screen
and board could rank a note beside a company (ops 6852, ops 6859).

Three kinds of evidence, all from the listing itself, read in this order:
  1. the book's name states the instrument: a coupon ("6.125%"), "Notes", "Debentures", "SR NT", "1M BD",
     "Preferred Shares", "Perpetual", "Cumulative";
  2. Polygon's own name for a listing it types "CS" states it: the book's provider shortens many of these names to the
     issuer ("AGNC Investment Corp.", "Hancock Whitney Corporation - 6"), Polygon spells the instrument out
     ("... Depositary Shares Each Representing a 1/1,000th Interest in a Share of 6.125% Series F ... Preferred
     Stock", "... 6.25% Subordinated Notes due 2060").  Only "CS" rows are read this way: an ADR whose shares
     represent preferred shares (Banco Bradesco, Braskem - type ADRC) is the company's listed equity and stays;
  3. structure, for a row neither name describes: a five-letter NASDAQ ticker that extends a listed three- or
     four-letter ticker of the same issuer name with one of NASDAQ's fifth letters for a second issue (G-J a
     convertible bond, L and Z a miscellaneous issue such as a baby bond, M-P a preferred share, W a warrant) AND
     trades under a tenth of that ticker's dollar volume.  A share class (GOOGL beside GOOG, FOXA beside FOX, NWSA
     beside NWS) trades as much as its sibling or more and stays common stock.

Pure: no I/O, no clock.
"""
import re

NAMED_NOTE = re.compile(r"\d(\.\d+)?\s*%|\bnotes?\b|\bdebentures?\b|\bsr\.?\s+nt\b|\b\dm\s+bd\b|\bmortgage\s+bonds?\b", re.I)       # a coupon, "Senior Notes due 2031", "SR NT", "1M BD"
NAMED_PREFERRED = re.compile(r"\bpreferred\s+(stock|shares?|units?|securities|l\.?p\.?|series)|\bperpetual\b|\bcumulative\b|\bpfd\b", re.I)
# "Depositary Shares" of a US issuer are a slice of a preferred share; "American / Global Depositary Shares" are an ADR
NAMED_DEPOSITARY = re.compile(r"(?<!american\s)(?<!global\s)\bdeposit[ao]ry\s+sh(?:are|r)?s?\b", re.I)
NASDAQ_FIFTH_LETTER = {"G": "is_debt_or_structured", "H": "is_debt_or_structured", "I": "is_debt_or_structured", "J": "is_debt_or_structured",
                       "L": "is_debt_or_structured", "Z": "is_debt_or_structured",
                       "M": "is_preferred", "N": "is_preferred", "O": "is_preferred", "P": "is_preferred", "W": "is_warrant_unit"}
OTHER_ISSUE_DV_SHARE = 0.10                    # ... when the line trades under a tenth of the dollar volume of the ticker it extends
FLAG_WORD = {"is_debt_or_structured": "a note or bond", "is_preferred": "a preferred share", "is_warrant_unit": "a warrant"}
POLYGON_COMMON_STOCK = "CS"


def _num(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v if v == v else None


def by_name(name, depositary=False):
    """The identity flag a listing's name states, or None.  A preferred share is read before a note: "Depositary
    Shares ... of 6.125% Series F Preferred Stock" carries a coupon and is not a note."""
    name = str(name or "")
    if NAMED_PREFERRED.search(name) or (depositary and NAMED_DEPOSITARY.search(name)):
        return "is_preferred"
    if NAMED_NOTE.search(name):
        return "is_debt_or_structured"
    return None


def find(rows, dollar_vol=None, ref_names=None):
    """{symbol: {"flag": identity flag to set, "why": reason, "evidence": "book name" | "reference name" | "structure",
    "base": ticker it extends or None}} for the rows that are common stock by the book's flags (is_common true) and
    are another security by the evidence above.

    rows: identity dicts (symbol, name, exchange, is_common, security_type).  dollar_vol: {symbol: 20-session average
    dollar volume}; without it the structure test cannot run.  ref_names: {symbol: Polygon's name}; without it only
    the book's own names are read."""
    common = [r for r in rows if r.get("is_common") and r.get("symbol")]
    same_name = {}                                  # every common row is a candidate "ticker it extends"
    for r in common:
        key = str(r.get("name") or "").strip().lower()
        if key:
            same_name.setdefault(key, []).append(str(r["symbol"]).upper())
    dv, ref = dollar_vol or {}, ref_names or {}
    out = {}
    for r in common:
        if r.get("is_adr"):
            continue                                # an ADR is the company's listed equity whatever its shares represent
        sym, name = str(r["symbol"]).upper(), str(r.get("name") or "")
        cs = str(r.get("security_type") or "").upper() == POLYGON_COMMON_STOCK
        flag = by_name(name, depositary=cs)
        if flag:
            out[sym] = {"flag": flag, "why": "%s by its own name" % FLAG_WORD[flag], "evidence": "book name", "base": None}
            continue
        other = (ref.get(sym) or ref.get(sym.replace("-", "."))) if cs else None
        flag = by_name(other, depositary=True) if other else None
        if flag:
            out[sym] = {"flag": flag, "why": "%s by the reference list's name for it" % FLAG_WORD[flag], "evidence": "reference name", "base": None}
            continue
        if dv and len(sym) == 5 and sym.isalpha() and sym[-1] in NASDAQ_FIFTH_LETTER and "NASDAQ" in str(r.get("exchange") or "").upper() and name.strip():
            mine = _num(dv.get(sym)) or 0.0
            for base in same_name.get(name.strip().lower(), []):
                if base != sym and len(base) in (3, 4) and sym.startswith(base) and mine < OTHER_ISSUE_DV_SHARE * (_num(dv.get(base)) or 0.0):
                    flag = NASDAQ_FIFTH_LETTER[sym[-1]]
                    out[sym] = {"flag": flag, "why": "%s of %s (NASDAQ fifth letter %s, under a tenth of its dollar volume)" % (FLAG_WORD[flag], base, sym[-1]), "evidence": "structure", "base": base}
                    break
    return out


def apply(universe, dollar_vol=None, ref_names=None):
    """Set the flag and clear is_common on every row find() names.  universe: {symbol: identity dict}, changed in
    place.  Returns find()'s answer (the reasons are published in the book's manifest, coverage.other_issues)."""
    hits = find(list(universe.values()), dollar_vol, ref_names)
    for sym, hit in hits.items():
        ident = universe.get(sym) or next((u for u in universe.values() if str(u.get("symbol") or "").upper() == sym), None)
        if ident is None:
            continue
        ident[hit["flag"]] = True
        ident["is_common"] = False
    return hits
