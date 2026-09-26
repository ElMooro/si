"""Wyckoff campaign projection over justhodl-bottom v1.2.0 board rows.

Additive fields only. Does not rewrite bottom.json schema.
Used by justhodl-bottom companion, justhodl-khalid scoring, and tests.

Campaign map (locked 2026-09-25):
  BOTTOM = CLIMAX | TESTING | NO_RALLY | NO_TEST_BREAKOUT   # Proof 1 stopping action; not a buy
  ACCUM  = ST_CONFIRMED | TRIGGERED                         # Proofs 2-4 absorption / LPS
  PUMP   = MARKUP | COMPLETED                               # Proof 5 SOS / start of bull run
  ABORT  = FAILED | STOPPED

Lesson map (7 signs of demand absorbing supply, 2026-09-26):
  1 SC     P1_SC
  2 ST     P3_ST after P2_AR (automatic rally sets the creek)
  3 dry-up hinge_of + shrinking reactions inside the range
  4 spring P4_SPRING
  5 SOS    P5_SOS jump the creek; LPS / backup is the entry
  6 RS     P6_RS vs the average during the range — confirmation only
  7 trend  P7_UPTREND higher highs / higher lows + effort/result — confirmation only

Hinge / springboard (transcript 2 — dullness before expansion):
  approach_vol_slope < 0 AND approach_range_x < 1 AND st_vol_ratio_sc <= 0.4

Historical numbers in HISTORICAL are copied from live data/bottom.json.
Do not invent additional hit rates here.
"""
from __future__ import annotations

CAMPAIGN_STATES = {
    "BOTTOM": {"CLIMAX", "TESTING", "NO_RALLY", "NO_TEST_BREAKOUT"},
    "ACCUM": {"ST_CONFIRMED", "TRIGGERED"},
    "PUMP": {"MARKUP", "COMPLETED"},
    "ABORT": {"FAILED", "STOPPED"},
}

PROOF_DEFS = (
    ("P1_SC", "Selling climax", "panic supply meets absorption — stopping action"),
    ("P2_AR", "Automatic rally", "demand exists; range top / creek is set"),
    ("P3_ST", "Secondary test on diminished volume", "supply is drying"),
    ("P4_SPRING", "Spring / terminal shakeout", "undercut recovered; weak holders gone"),
    ("P5_SOS", "Sign of strength / jump the creek", "range resolves up; markup begins"),
    ("P6_RS", "Comparative strength vs the average", "holds or leads while the tape is weak — confirmation, not a buy"),
    ("P7_UPTREND", "Higher highs and higher lows", "effort expands on advances, contracts on reactions — confirmation"),
)

# Copied from live justhodl-bottom v1.2.0 as read 2026-09-26 from data/bottom.json.
# Do not invent.
HISTORICAL = {
    "source": "data/bottom.json method/base_rates + board states",
    "as_of": "2026-09-26",
    "window": "2021-09-20 to 2026-09-25",
    "sc_sequences": 6551,
    "with_rally": 4882,
    "triggered": 2033,
    "unmanaged_trigger": {"bars": 63, "median_pct": 2.56, "hit_pct": 55},
    "grade_a": {"bars": 63, "median_pct": 19.85, "hit_pct": 89, "n": 464},
    "quiet_test_le_0_4x_sc": {"bars": 63, "median_pct": 3.2},
    "loud_test": {"bars": 63, "median_pct": 1.9},
    "crowd_ar_bounce": {"bars": 63, "median_pct": 5.64, "hit_pct": 60, "max_dd_pct": -12.2, "later_undercut_sc_low_pct": 49},
    "paper_stop_0_1atr_under_test": {"stop_hit_pct": 62, "managed_63_median_pct": -6.0},
    "note": (
        "Quiet tests (<=0.4x SC volume) are the hinge/dry-up edge. "
        "Crowd bounce off the climax is not the entry: 49% later undercut the SC low. "
        "Grade A (full sequence, quiet test, trigger) is the only published bucket with an 89% 63-bar hit rate. "
        "Raw trigger is 55% at 63 bars. Comparative strength vs SPY is not in the harvest yet, so it is confirmation-only."
    ),
}


def campaign_of(state):
    s = (state or "").upper()
    for name, states in CAMPAIGN_STATES.items():
        if s in states:
            return name
    return "WATCH"


def _ev(row, *path):
    cur = row.get("event") or {}
    for key in path:
        cur = (cur or {}).get(key) if isinstance(cur, dict) else None
    return cur


def _num(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed and abs(parsed) != float("inf") else None


def hinge_of(row):
    slope = row.get("approach_vol_slope")
    rng = row.get("approach_range_x")
    vol_ratio = row.get("st_vol_ratio_sc")
    if vol_ratio is None:
        vol_ratio = _ev(row, "st", "vol_ratio_sc")
    dry = (
        slope is not None and slope < 0
        and rng is not None and float(rng) < 1.0
        and vol_ratio is not None and float(vol_ratio) <= 0.4
    )
    depth = (row.get("st_depth_class") or "").upper()
    state = (row.get("state") or "").upper()
    springboard = bool(
        dry
        and (
            depth in ("SPRING", "HIGHER_LOW")
            or state in ("ST_CONFIRMED", "TRIGGERED", "MARKUP", "COMPLETED")
        )
    )
    return {
        "hinge": bool(dry),
        "springboard": springboard,
        "approach_vol_slope": slope,
        "approach_range_x": rng,
        "st_vol_ratio_sc": vol_ratio,
        "why": (
            "volume and range contracted into the test (hinge dullness / springboard)"
            if dry
            else "hinge not printed — need declining volume, range < 1 ATR, test <=0.4x climax volume"
        ),
    }


def rs_of(row):
    """Comparative strength vs the average during the range. Confirmation only."""
    raw = row.get("rs_vs_spy")
    if raw is None:
        raw = row.get("rel_strength")
    if raw is None:
        raw = _ev(row, "rs", "vs_spy")
    label = str(row.get("rs_class") or row.get("rel_strength_class") or "").upper()
    score = _num(raw)
    leader = label in {"LEADER", "STRONG", "OUTPERFORM"} or (score is not None and score > 0)
    return {
        "passed": bool(leader),
        "score": score,
        "class": label or None,
        "why": (
            "beats or holds vs the average during the range"
            if leader
            else "no comparative-strength field yet — confirmation only, not scored as a hard gate"
        ),
    }


def uptrend_of(row):
    """Sign 7: higher highs / higher lows after the jump. Confirmation only."""
    state = (row.get("state") or "").upper()
    explicit = row.get("hh_hl")
    if explicit is None:
        explicit = _ev(row, "trend", "hh_hl")
    markup = state in {"MARKUP", "COMPLETED"}
    passed = bool(explicit) or markup
    return {
        "passed": passed,
        "state": state,
        "why": (
            "markup / completed — demand still in control until character changes"
            if markup
            else ("hh/hl flag set" if explicit else "uptrend not confirmed — wait for higher highs and higher lows after the jump")
        ),
    }


def proofs_of(row):
    state = (row.get("state") or "").upper()
    sc_date = row.get("sc_date") or _ev(row, "sc", "date")
    ar_high = row.get("ar_high")
    if ar_high is None:
        ar_high = _ev(row, "ar", "high")
    st_date = row.get("st_date") or _ev(row, "st", "date")
    st_vol = row.get("st_vol_ratio_sc")
    if st_vol is None:
        st_vol = _ev(row, "st", "vol_ratio_sc")
    depth = (row.get("st_depth_class") or "").upper()
    spring_flag = depth == "SPRING" or bool(_ev(row, "st", "spring"))
    last = row.get("last")
    sos = (
        state in ("MARKUP", "COMPLETED")
        or bool(_ev(row, "trigger", "sos_date"))
        or (
            state == "TRIGGERED"
            and row.get("trigger_date")
            and last is not None
            and ar_high is not None
            and float(last) > float(ar_high)
        )
    )
    p3 = bool(st_date) and (st_vol is None or float(st_vol) <= 0.8)
    rs = rs_of(row)
    trend = uptrend_of(row)
    return [
        {
            "id": "P1_SC",
            "label": "Selling climax",
            "passed": bool(sc_date),
            "evidence": "sc_date=%s vol_x=%s range_x=%s decline=%s"
            % (sc_date, row.get("sc_vol_x"), row.get("sc_range_x"), row.get("sc_decline_pct")),
        },
        {
            "id": "P2_AR",
            "label": "Automatic rally",
            "passed": ar_high is not None,
            "evidence": "ar_high=%s rally_pct=%s" % (ar_high, row.get("ar_rally_pct")),
        },
        {
            "id": "P3_ST",
            "label": "Secondary test on diminished volume",
            "passed": bool(p3),
            "evidence": "st_date=%s vol_ratio_sc=%s n_tests=%s"
            % (st_date, st_vol, row.get("n_tests")),
        },
        {
            "id": "P4_SPRING",
            "label": "Spring / terminal shakeout",
            "passed": bool(spring_flag),
            "evidence": "depth_class=%s spring=%s" % (depth or None, spring_flag),
        },
        {
            "id": "P5_SOS",
            "label": "Sign of strength / jump the creek",
            "passed": bool(sos),
            "evidence": "state=%s trigger=%s sos=%s"
            % (state, row.get("trigger_date"), _ev(row, "trigger", "sos_date")),
        },
        {
            "id": "P6_RS",
            "label": "Comparative strength vs the average",
            "passed": bool(rs["passed"]),
            "hard_gate": False,
            "evidence": rs["why"],
        },
        {
            "id": "P7_UPTREND",
            "label": "Higher highs and higher lows",
            "passed": bool(trend["passed"]),
            "hard_gate": False,
            "evidence": trend["why"],
        },
    ]


def pump_start_of(row, campaign=None, hinge=None, proofs=None):
    campaign = campaign or campaign_of(row.get("state"))
    hinge = hinge or hinge_of(row)
    proofs = proofs or proofs_of(row)
    bars = row.get("bars_in_state") or 0
    spring_passed = any(p["id"] == "P4_SPRING" and p["passed"] for p in proofs)
    higher_low = (row.get("st_depth_class") or "").upper() == "HIGHER_LOW"
    return {
        "flag": campaign == "PUMP",
        "ready": campaign == "ACCUM" and bool(hinge.get("hinge")) and (spring_passed or higher_low),
        "chase": campaign == "PUMP" and bars > 5,
        "rule": "LPS is the entry. Do not chase SOS. Pump = MARKUP/COMPLETED after hinge + spring. RS and HH/HL confirm; they do not create the entry.",
    }


def annotate(row):
    """Return a shallow copy of a bottom board row with campaign fields added."""
    campaign = campaign_of(row.get("state"))
    hinge = hinge_of(row)
    proofs = proofs_of(row)
    out = dict(row)
    out["campaign"] = campaign
    out["proofs"] = proofs
    out["proofs_passed"] = sum(1 for p in proofs if p["passed"])
    out["proofs_hard_passed"] = sum(1 for p in proofs if p["passed"] and p.get("hard_gate") is not False)
    out["hinge"] = hinge
    out["rs"] = rs_of(row)
    out["uptrend"] = uptrend_of(row)
    out["pump_start"] = pump_start_of(row, campaign, hinge, proofs)
    return out


def project_board(document):
    """Project campaign lists from a bottom.json document."""
    board = []
    if isinstance(document, dict):
        board = document.get("board") or document.get("board_all") or document.get("rows") or []
    elif isinstance(document, list):
        board = document
    annotated = [annotate(r) for r in board if isinstance(r, dict)]
    buckets = {"BOTTOM": [], "ACCUM": [], "PUMP": [], "ABORT": [], "WATCH": []}
    for row in annotated:
        buckets.setdefault(row["campaign"], []).append(row)
    return {
        "engine": "wyckoff-campaign",
        "version": "1.1.0",
        "source_engine": (document or {}).get("engine") if isinstance(document, dict) else "justhodl-bottom",
        "source_version": (document or {}).get("version") if isinstance(document, dict) else None,
        "historical": HISTORICAL,
        "counts": {k: len(v) for k, v in buckets.items()},
        "hinge_n": sum(1 for r in annotated if r["hinge"]["hinge"]),
        "rs_n": sum(1 for r in annotated if r["rs"]["passed"]),
        "pump_ready_n": sum(1 for r in annotated if r["pump_start"]["ready"]),
        "pump_chase_n": sum(1 for r in annotated if r["pump_start"]["chase"]),
        "BOTTOM": buckets["BOTTOM"],
        "ACCUM": buckets["ACCUM"],
        "PUMP": buckets["PUMP"],
        "ABORT": buckets["ABORT"],
        "rows": annotated,
    }
