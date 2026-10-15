"""Let an inferred own CDS level sign a later print when the two branches sit far apart.

The public tape does not say who paid the upfront. A name stays active-but-unpriced when
later capped prints cannot be signed. A firm level already could, for 365 days. An inferred
level (curve shape, or a level carried forward) could not, so a name last fixed at 18bp
stayed unsigned. The far-branch test is unchanged.
Idempotent.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DESK = ROOT / "aws/shared/cds_desk.py"
TESTS = ROOT / "aws/lambdas/justhodl-cds-desk/tests/run_tests.py"

HELPER = '''
def _median_branch_picks(anchor_bp, cd):
    """True when one stored median branch is within 70% (log) of anchor_bp and the other is BRANCH_FAR_LOG further.
    This is the same far-only test resolve_spread applies to a single print, applied to the day's median branches."""
    if anchor_bp is None or not cd:
        return False
    for pair in cd.values():
        if not pair or len(pair) < 2 or not pair[0] or not pair[1]:
            continue
        d0, d1 = _log_dist(pair[0], anchor_bp), _log_dist(pair[1], anchor_bp)
        best, other = (d0, d1) if d0 <= d1 else (d1, d0)
        if best <= 0.7 and other - best >= BRANCH_FAR_LOG:
            return True
    return False


'''

OLD_DOC = "    the name's own last resolved level within 365 days, usable only when the branches sit far apart ('trailing_long') >\n"
NEW_DOC = "    the name's own last resolved level within 365 days, usable only when the branches sit far apart ('trailing_long';\n    firm, or an earlier inferred level which stays inferred) >\n"

OLD_TAIL = '''        # the name's own earlier resolved level (firm evidence only); resolve_spread demands far-apart branches for it
        old = [(d, row) for d, row in rows if start_long <= d < as_of_iso and row.get("s") is not None and (row.get("aq") == "firm" or row.get("q"))]
        if old:
            out[name] = (statistics.median([row["s"] for d, row in old[-5:]]), "firm", "trailing_long")
            continue
        info = meta.get(key) or {}
        grp = classify_entity(info.get("kind"), key, info.get("ccy") or {})["group"]
        cm = _comove_anchor(series, series_map.get("IDX:" + COMOVE_INDEX.get(grp, "CDX.NA.IG")), start_comove, as_of_iso)
        if cm:
            out[name] = (cm[0], "inferred", "comove")
    return out
'''

NEW_TAIL = '''        # the name's own earlier resolved level; resolve_spread demands far-apart branches for it.
        # A firm (or quoted) level outranks an inferred one. An inferred level — curve shape, a carried
        # trailing print — may sign a later print only when the stored branches are far enough apart that
        # the other side is not a plausible continuation. It stays inferred, and if it cannot discriminate
        # the latest branches, index co-movement still gets its turn.
        old = [(d, row) for d, row in rows if start_long <= d < as_of_iso and row.get("s") is not None and (row.get("aq") == "firm" or row.get("q"))]
        if old:
            out[name] = (statistics.median([row["s"] for d, row in old[-5:]]), "firm", "trailing_long")
            continue
        old_inf = [(d, row) for d, row in rows if start_long <= d < as_of_iso and row.get("s") is not None]
        anchor_inf = statistics.median([row["s"] for d, row in old_inf[-5:]]) if old_inf else None
        latest_cd = next((row.get("cd") for d, row in reversed(rows) if d < as_of_iso and row.get("cd")), None)
        if anchor_inf is not None and _median_branch_picks(anchor_inf, latest_cd):
            out[name] = (anchor_inf, "inferred", "trailing_long")
            continue
        info = meta.get(key) or {}
        grp = classify_entity(info.get("kind"), key, info.get("ccy") or {})["group"]
        cm = _comove_anchor(series, series_map.get("IDX:" + COMOVE_INDEX.get(grp, "CDX.NA.IG")), start_comove, as_of_iso)
        if cm:
            out[name] = (cm[0], "inferred", "comove")
            continue
        if anchor_inf is not None:
            out[name] = (anchor_inf, "inferred", "trailing_long")
    return out
'''

OLD_RULE = "the name's own resolved level within 365 days (only when the two branches sit >e^1 apart)"
NEW_RULE = "the name's own resolved level within 365 days (firm, or an earlier inferred level which stays inferred; only when the two branches sit >e^1 apart)"

OLD_TEST = '    assert "PGE" not in C.anchors_from_bank(stale, "2026-10-02")\n\n\nif __name__ == "__main__":\n'
NEW_TEST = '''    assert "PGE" not in C.anchors_from_bank(stale, "2026-10-02")


def test_inferred_own_level_signs_a_later_far_branch():
    """Belgium-shaped. The only resolved level is inferred, three weeks old, and the next prints are capped
    blocks the old writer stored with no side. The near branch is kept; a close pair is not."""
    series = {
        "2026-09-16": {"n": 6, "n5": 6, "np": 6, "s": 17.59, "sb": "derived", "q": 0, "amb": 3, "nm": 8.0,
                       "aq": "inferred", "as": "curve_shape", "cd": {"100": [197.8, 19.7]}},
        "2026-10-08": {"n": 1, "n5": 1, "np": 0, "s": None, "q": 0, "amb": 1, "nm": 5.0, "cd": {"100": [173.9, 32.5]}},
    }
    meta = {"KINGDOM OF BELGIUM": {"kind": "sov", "raw": "Kingdom of Belgium", "ccy": {"EUR": 4}}}
    bank = {"version": C.VERSION, "series": {"KINGDOM OF BELGIUM": series}, "meta": meta, "entity_map": {}, "days": ["2026-09-16", "2026-10-08"]}
    anc = C.anchors_from_bank(bank, "2026-10-09")
    assert anc["KINGDOM OF BELGIUM"][2] == "trailing_long" and anc["KINGDOM OF BELGIUM"][1] == "inferred"
    assert abs(anc["KINGDOM OF BELGIUM"][0] - 17.59) < 0.01
    trade = C.normalize_trade(derived_row(32.5, coupon_bp=100.0, day="2026-10-08"), "sec")
    spread, basis = C.resolve_spread(trade, RATE, anc["KINGDOM OF BELGIUM"][0], far_only=True)
    assert basis == "derived" and spread is not None and abs(spread - 32.5) < 8, (spread, basis)
    near = {"2026-09-16": series["2026-09-16"], "2026-10-08": {"n": 1, "n5": 1, "np": 0, "s": None, "q": 0, "amb": 1, "cd": {"100": [140.0, 70.0]}}}
    bank_near = {"version": C.VERSION, "series": {"AVIS": near}, "meta": {"AVIS": {"kind": "corp", "raw": "Avis", "ccy": {"USD": 2}}}, "entity_map": {}, "days": ["2026-09-16", "2026-10-08"]}
    assert C._median_branch_picks(17.59, near["2026-10-08"]["cd"]) is False
    offered = C.anchors_from_bank(bank_near, "2026-10-09")["AVIS"]
    assert offered[2] == "trailing_long" and offered[1] == "inferred"
    close = C.normalize_trade(derived_row(80, coupon_bp=100.0, day="2026-10-08"), "sec")
    cands = C.candidate_spreads(close, RATE)
    if len(cands) == 2 and abs(C.math.log(cands[0][0] / cands[1][0])) < C.BRANCH_FAR_LOG:
        assert C.resolve_spread(close, RATE, 17.59, far_only=True)[0] is None


if __name__ == "__main__":
'''

def apply(path, old, new, label):
    text = path.read_text()
    if old not in text and new in text:
        print("already", label)
        return 0
    n = text.count(old)
    if n != 1:
        raise SystemExit("%s: expected 1 match, found %d" % (label, n))
    path.write_text(text.replace(old, new, 1))
    print("patched", label)
    return 1


def main():
    changed = 0
    text = DESK.read_text()
    if "def _median_branch_picks" not in text:
        changed += apply(DESK, "def anchors_from_bank", HELPER + "def anchors_from_bank", "helper")
    else:
        print("already helper")
    changed += apply(DESK, OLD_DOC, NEW_DOC, "doc")
    changed += apply(DESK, OLD_TAIL, NEW_TAIL, "tail")
    changed += apply(DESK, OLD_RULE, NEW_RULE, "rule")
    changed += apply(TESTS, OLD_TEST, NEW_TEST, "test")
    desk = DESK.read_text()
    if "def _median_branch_picks" not in desk or "earlier inferred level which stays inferred" not in desk:
        raise SystemExit("desk markers missing")
    if "def test_inferred_own_level_signs_a_later_far_branch" not in TESTS.read_text():
        raise SystemExit("test marker missing")
    print("cds inferred-anchor patch changed", changed)


if __name__ == "__main__":
    main()
