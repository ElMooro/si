"""ark_moves - which snapshot a day's ARK holdings are measured against, and the complete list of what changed.

Pure functions, no I/O (the engine does the reading and writing).  Two rules live here:

1. THE BASELINE.  The engine used to measure every run against the snapshot of the run before it.  That is right the
   morning after a trading day, and wrong every other time: the source republishes Friday's holdings on Saturday,
   Sunday and Monday, so from the second read on the "previous" snapshot IS the current one and the page said
   "0 changes" until Tuesday; a second run on the same day emptied the day's moves the same way.  The rule now:
   a snapshot becomes the baseline only when the source publishes a LATER holdings date.  Until then every run keeps
   measuring against the retained baseline, so the last trading day's moves stay on the page, dated.

2. EVERY MOVE.  The feed used to carry the 15 largest additions and cuts by percent.  `moves_block` carries all of
   them (new, added to, cut, closed), each list in the engine's own order, so a reader can rank by dollars or filter by
   fund without the engine deciding what is worth showing.  The 1% share-count threshold is the engine's, unchanged.
"""
from __future__ import annotations

THRESHOLD_PCT = 1.0
KINDS = ("new_positions", "position_adds", "position_trims", "closed_positions")


def holdings_date(holdings_by_fund) -> str:
    """The newest position date in a {fund: [positions]} snapshot ('' when the source dated nothing)."""
    best = ""
    for positions in (holdings_by_fund or {}).values():
        for p in positions or []:
            d = str((p or {}).get("date") or "")[:10]
            if d > best:
                best = d
    return best


def snapshot_date(doc) -> str:
    """A stored snapshot's holdings date: its own `holdings_date` field, else the newest position date inside it."""
    if not isinstance(doc, dict):
        return ""
    return str(doc.get("holdings_date") or "")[:10] or holdings_date(doc.get("holdings_by_fund"))


def snapshot_map(doc) -> dict:
    """{(fund, ticker): {shares, market_value, weight, company}} for a stored snapshot ({} when there is none)."""
    out = {}
    for fund, positions in ((doc or {}).get("holdings_by_fund") or {}).items():
        for p in positions or []:
            if not isinstance(p, dict) or not p.get("ticker"):
                continue
            out[(fund, p["ticker"])] = {"shares": p.get("shares", 0), "market_value": p.get("market_value", 0), "weight": p.get("weight", 0), "company": p.get("company") or ""}
    return out


def choose_baseline(current_date, latest, base):
    """(baseline snapshot or None, new_day).

    latest = the snapshot the previous run stored (the newest holdings the engine has seen);
    base   = the snapshot the diff now on the page was measured against.
    new_day is True when the source has published a later holdings date than `latest` holds: yesterday's snapshot
    becomes the baseline and the caller retains it.  Otherwise the same holdings are being read again and the retained
    baseline stands.  Without usable dates the rule cannot be applied and the engine behaves as it always did
    (measure against the previous run, treat it as a new day)."""
    cur = str(current_date or "")[:10]
    latest_date, base_date = snapshot_date(latest), snapshot_date(base)
    if not isinstance(latest, dict) or not latest.get("holdings_by_fund"):
        return None, True
    if not cur or not latest_date:
        return latest, True
    if cur > latest_date:
        return latest, True
    if isinstance(base, dict) and base.get("holdings_by_fund") and base_date and base_date < cur:
        return base, False
    return latest, False


def moves_block(diffs) -> dict:
    """The complete lists, in the engine's order, with their counts.  `diffs` is compute_diffs()' return value."""
    out = {k: list((diffs or {}).get(k) or []) for k in KINDS}
    return {"threshold_pct": THRESHOLD_PCT, "counts": {k: len(out[k]) for k in KINDS}, **out}


def reconciles(block, published) -> bool:
    """True when the complete lists agree with the counts the feed publishes beside them."""
    c = (block or {}).get("counts") or {}
    p = published or {}
    return (c.get("new_positions") == p.get("n_new_positions") and c.get("position_adds") == p.get("n_position_adds")
            and c.get("position_trims") == p.get("n_position_trims") and c.get("closed_positions") == p.get("n_closed_positions"))
