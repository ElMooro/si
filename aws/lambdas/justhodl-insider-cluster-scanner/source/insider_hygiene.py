"""insider_hygiene - what the scanner does to a purchase row before it counts it.  Pure functions, no I/O.

Three things were wrong with the raw list on 2026-11-17 (ops 6831 read the banked feed row by row):

1. THE FEED REPEATS A FILING.  The walker banks the provider's "latest" pages every day, so one Form 4 line sits in
   three consecutive day files.  `unique` keeps one row per (ticker, accession, insider, date, shares, price).

2. ONE FILING UNDER SEVERAL TICKERS.  Mallinckrodt's purchases arrive under MNK, MNKKQ and MNKPF - same issuer CIK,
   same accession, same insider, date, shares and price.  `merge_same_filing` keeps them once, under the ticker the
   tape knows (else the shortest symbol), and records the others in `also_filed_as`.  Transactions that are NOT
   identical across an issuer's tickers are different share classes and stay where they are.

3. A PRICE THAT CANNOT BE.  LONA: 3,149,470 shares "at $3,149.47" = $9.9B for a company the tape prices at $3.47 and
   values at $13.7M.  AASP: 200,000 shares "at $1,000,000" = $200B.  `tape_check` compares every price with the factor
   book's tape (52-week range, last price, market value):
       in_range    inside 0.5 x the 52-week low .. 1.5 x the 52-week high (within 3x of the last price when the tape
                   has no range)                                                   -> counted
       off_tape    outside that band but possible: another share class, preferred units, an ADR's local line,
                   a split since the trade (BH's class A at $1,768 against BH at $362)  -> counted, labelled
       impossible  off the tape by 5x or more AND worth more than the whole company (no market value on the book:
                   100x or more off the last price)                                -> set aside, never counted
       no_tape     the book does not carry the ticker                              -> counted, labelled
   A row is only ever set aside for being impossible, and every row set aside is published with its reason.
"""
from __future__ import annotations

BAND_LOW, BAND_HIGH = 0.5, 1.5        # around the 52-week range
REF_BAND = 3.0                        # around the last price, when the tape has no range
FAR = 5.0                             # "far off the tape": 5x either way
FAR_NO_MCAP = 100.0                   # without a market value to compare with: two orders of magnitude
REF_FIELDS = ("name", "price", "close", "high_52w", "low_52w", "mcap")


def _num(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v if v == v and v not in (float("inf"), float("-inf")) else None


def _key(t):
    return (t.get("ticker"), t.get("accession"), t.get("insider_name"), t.get("txn_date"), int(_num(t.get("shares")) or 0), round(_num(t.get("price")) or 0, 4))


def unique(txns):
    """One row per filing line (the feed repeats a line on consecutive days); order kept."""
    seen, out = set(), []
    for t in txns or []:
        k = _key(t)
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


def merge_same_filing(txns, has_tape=None):
    """(transactions, merged).  Lines identical on issuer CIK + accession + insider + date + shares + price that arrive
    under more than one ticker are one filing: kept under the ticker `has_tape` knows, else the shortest symbol, else
    the first alphabetically; the kept rows carry `also_filed_as`.  merged = [{cik, kept, also_filed_as, n_transactions}]."""
    has_tape = has_tape or (lambda _t: False)
    groups = {}
    for t in txns or []:
        cik = str(t.get("cik") or "").strip()
        if not cik:
            continue
        gk = (cik,) + _key(t)[1:]
        groups.setdefault(gk, set()).add(t.get("ticker"))
    choice, sets = {}, {}
    for gk, tickers in groups.items():
        if len(tickers) < 2:
            continue
        ordered = sorted(tickers, key=lambda s: (0 if has_tape(s) else 1, len(s or ""), s or ""))
        choice[gk] = ordered[0]
        sk = (gk[0], ordered[0], tuple(ordered[1:]))
        sets[sk] = sets.get(sk, 0) + 1
    if not choice:
        return list(txns or []), []
    out = []
    for t in txns or []:
        cik = str(t.get("cik") or "").strip()
        gk = (cik,) + _key(t)[1:]
        kept = choice.get(gk)
        if kept is None:
            out.append(t)
            continue
        if t.get("ticker") != kept:
            continue
        row = dict(t)
        row["also_filed_as"] = sorted(s for s in groups[gk] if s != kept)
        out.append(row)
    merged = [{"cik": cik, "kept": kept, "also_filed_as": list(others), "n_transactions": n} for (cik, kept, others), n in sorted(sets.items())]
    return out, merged


def tape_check(txn, ref):
    """{status, tape_price, ratio, market_value, reason} for one transaction against the book's row for its ticker."""
    price, value = _num(txn.get("price")), _num(txn.get("value"))
    ref = ref if isinstance(ref, dict) else {}
    last = _num(ref.get("price")) or _num(ref.get("close"))
    high, low, mcap = _num(ref.get("high_52w")), _num(ref.get("low_52w")), _num(ref.get("mcap"))
    if not price or price <= 0 or not ((high and low and high > 0 and low > 0) or (last and last > 0)):
        return {"status": "no_tape", "tape_price": last, "ratio": None, "market_value": mcap, "reason": "the factor book has no tape for this ticker"}
    if high and low and high > 0 and low > 0:
        inside = BAND_LOW * low <= price <= BAND_HIGH * high
        anchor = last if last and last > 0 else (high + low) / 2.0
    else:
        inside = last / REF_BAND <= price <= last * REF_BAND
        anchor = last
    ratio = price / anchor
    if inside:
        return {"status": "in_range", "tape_price": anchor, "ratio": round(ratio, 4), "market_value": mcap, "reason": ""}
    far = ratio >= FAR or ratio <= 1.0 / FAR
    if mcap and mcap > 0:
        impossible = far and value is not None and value > mcap
        why = "priced at %s the tape and worth more than the whole company (%s of its market value)" % (_times(ratio), _pct(value / mcap)) if impossible else ""
    else:
        impossible = ratio >= FAR_NO_MCAP or ratio <= 1.0 / FAR_NO_MCAP
        why = "priced at %s the tape, with no market value on the book to compare" % _times(ratio) if impossible else ""
    if impossible:
        return {"status": "impossible", "tape_price": anchor, "ratio": round(ratio, 4), "market_value": mcap, "reason": why}
    return {"status": "off_tape", "tape_price": anchor, "ratio": round(ratio, 4), "market_value": mcap,
            "reason": "priced at %s the tape: another share class, a preferred or local line, or a split since the trade" % _times(ratio)}


def _times(ratio):
    return "%s times" % ("{:,.1f}".format(ratio) if ratio >= 1 else "1/{:,.0f}".format(1.0 / ratio)) if ratio else "?"


def _pct(x):
    return "{:,.0f}%".format(x * 100)


def screen(txns, refs):
    """(counted, set_aside, summary).  Every counted row carries price_check / tape_ratio; set_aside rows are the
    impossible ones, each with its reason.  refs = {ticker: book row}; an empty refs labels everything no_tape."""
    refs = refs or {}
    counted, set_aside = [], []
    summary = {"n_checked": 0, "n_in_range": 0, "n_off_tape": 0, "n_no_tape": 0, "n_set_aside": 0, "usd_set_aside": 0.0}
    for t in txns or []:
        chk = tape_check(t, refs.get(t.get("ticker")))
        summary["n_checked"] += 1
        row = dict(t)
        row["price_check"] = chk["status"]
        if chk["ratio"] is not None:
            row["tape_ratio"] = chk["ratio"]
        if chk["status"] == "impossible":
            summary["n_set_aside"] += 1
            summary["usd_set_aside"] += _num(t.get("value")) or 0.0
            set_aside.append({"ticker": t.get("ticker"), "company": t.get("company") or (refs.get(t.get("ticker")) or {}).get("name") or "", "cik": t.get("cik") or "",
                              "insider_name": t.get("insider_name"), "role": t.get("role"), "security": t.get("security") or "", "shares": t.get("shares"), "price": t.get("price"),
                              "value": t.get("value"), "txn_date": t.get("txn_date"), "accession": t.get("accession"), "tape_price": chk["tape_price"], "tape_ratio": chk["ratio"],
                              "market_value": chk["market_value"], "reason": chk["reason"]})
            continue
        summary["n_" + chk["status"]] += 1
        counted.append(row)
    summary["usd_set_aside"] = round(summary["usd_set_aside"], 2)
    return counted, set_aside, summary
