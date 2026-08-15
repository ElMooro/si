"""
aws/lambdas/justhodl-invest/source/scoring.py
════════════════════════════════════════════════════════════════════════════
Pure functions, zero AWS/network dependency, fully unit-testable in
isolation (see ../tests/test_scoring.py). lambda_function.py is the only
module that touches boto3 / S3 / other engines' outputs; everything that is
actually "the math" lives here so it can be verified without a live fleet.

THREE TIERS, one function family each:

  Tier 1  confirm_indicator()          -- is there a real, corroborated
                                           leading demand signal, or is this
                                           one noisy print?
  Tier 2  spx_opportunity_cost_gate()  -- even if demand is real, is the
                                           risk-adjusted excess return over
                                           SPX big enough to justify leaving
                                           the index? (Khalid's founding
                                           question, made quantitative.)
  Tier 3  stock_composite_score()      -- within a gated-through industry,
                                           which names should beat their OWN
                                           industry ETF, and which should not
                                           be touched (peak-margin trap).

Every function returns a dict with an explicit "status" field so a caller
(or a human reading invest.json) never has to infer INSUFFICIENT_DATA from
a bare null -- that silent-null failure mode is exactly what the fleet's
impact_mapper.py / BUG-4 doctrine exists to prevent, and this module follows
the same discipline even though it does not import impact_mapper directly.
"""
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Optional


# ── Tier 1: leading-indicator confirmation ─────────────────────────────

MIN_LEGS_AVAILABLE = 2          # below this, we cannot corroborate at all
Z_CONFIRM = 1.0                 # a leg "votes confirm" at |z| >= 1.0 in the
                                 # expansionary direction
Z_CONFLICT = 1.0                # a leg "votes conflict" at |z| >= 1.0 against
CONFIRM_FRACTION = 0.6          # need >=60% of AVAILABLE legs voting confirm


def zscore(value: Optional[float], history: list) -> Optional[float]:
    """z-score of value against a trailing history sample. None if either
    is missing or history is too short to mean anything (n<8, mirroring the
    fleet's own n_obs>=8 floor)."""
    if value is None or not history or len(history) < 8:
        return None
    mean = sum(history) / len(history)
    var = sum((x - mean) ** 2 for x in history) / (len(history) - 1)
    sd = math.sqrt(var)
    if sd == 0:
        return None
    return (value - mean) / sd


@dataclass
class LegResult:
    leg_id: str
    z: Optional[float]
    direction: int = 1
    available: bool = True
    voting: bool = True   # False = diagnostic/context leg (see causal_graph.Leg)

    @property
    def signed_z(self) -> Optional[float]:
        if self.z is None:
            return None
        return self.z * self.direction


def confirm_indicator(leg_results: list) -> dict:
    """Tier 1 verdict for one leading-indicator bundle.

    Only VOTING legs count toward available/confirmed/conflicting and the
    MIN_LEGS_AVAILABLE floor. Non-voting (diagnostic) legs are still fully
    reported in `detail` for audit -- they explain the WHY, they just don't
    get to independently veto a confirmation (see korea_port_volume in
    causal_graph.py for the canonical example of why: a soft volume print
    next to a strong value print is the diagnostic signature of a
    price-driven boom, not evidence against one).

    Returns:
      status: CONFIRMED | TURNING | CONFLICTING | INSUFFICIENT_DATA
      confirmed_legs, conflicting_legs, available_legs, total_legs: int
      detail: per-leg signed z for audit (voting and non-voting alike)
    """
    total = len(leg_results)
    voting_legs = [l for l in leg_results if l.voting]
    available = [l for l in voting_legs if l.available and l.z is not None]
    detail = [{"leg_id": l.leg_id, "z": l.z, "signed_z": l.signed_z,
               "available": l.available, "voting": l.voting} for l in leg_results]

    if len(available) < MIN_LEGS_AVAILABLE:
        return {"status": "INSUFFICIENT_DATA", "confirmed_legs": 0,
                "conflicting_legs": 0, "available_legs": len(available),
                "total_legs": total, "detail": detail,
                "reason": f"only {len(available)}/{len(voting_legs)} voting legs "
                          f"have live data (need >= {MIN_LEGS_AVAILABLE})"}

    confirmed = [l for l in available if l.signed_z >= Z_CONFIRM]
    conflicting = [l for l in available if l.signed_z <= -Z_CONFLICT]

    n_confirm, n_conflict, n_avail = len(confirmed), len(conflicting), len(available)

    if n_confirm > 0 and n_conflict > 0:
        status = "CONFLICTING"
    elif n_confirm / n_avail >= CONFIRM_FRACTION and n_confirm >= 2:
        status = "CONFIRMED"
    elif n_confirm >= 1:
        status = "TURNING"          # early / partial -- the pre-consensus case
    else:
        status = "NOT_CONFIRMED"

    return {"status": status, "confirmed_legs": n_confirm,
            "conflicting_legs": n_conflict, "available_legs": n_avail,
            "total_legs": total, "detail": detail}


# ── Tier 2: SPX opportunity-cost gate ───────────────────────────────────
# Directly formalizes Khalid's founding question: "why own a sector/stock
# if SPX gives the same or better return with a lot more safety?" The gate
# is two-part on purpose -- IR alone lets a tiny, low-vol edge pass; excess
# return alone ignores that concentration adds risk SPX investors don't
# carry. Both must clear.

IR_MIN_DEFAULT = 0.40            # institutional-quality info-ratio floor
MIN_EXCESS_RETURN_PP_DEFAULT = 3.00   # must beat SPX by >=300bps/yr, not just IR


def spx_opportunity_cost_gate(
    sector_er_pp: Optional[float],
    spx_er_pp: Optional[float],
    tracking_error_pp: Optional[float],
    *,
    ir_min: float = IR_MIN_DEFAULT,
    min_excess_pp: float = MIN_EXCESS_RETURN_PP_DEFAULT,
    n_obs: Optional[int] = None,
    ci: Optional[tuple] = None,
) -> dict:
    """
    excess_return_pp = sector_er_pp - spx_er_pp
    information_ratio = excess_return_pp / tracking_error_pp

    pp_kind is always "estimated" here (it is a forward-looking model
    output, never a measured fact) -- per impact_mapper.py doctrine this
    MUST carry ci + n_obs or it is rejected upstream at construction.
    """
    if sector_er_pp is None or spx_er_pp is None:
        return {"status": "INSUFFICIENT_DATA", "pass": False,
                "reason": "missing sector or SPX expected return"}

    excess = round(sector_er_pp - spx_er_pp, 2)

    if tracking_error_pp is None or tracking_error_pp <= 0:
        return {"status": "INSUFFICIENT_DATA", "pass": False,
                "excess_return_pp": excess,
                "reason": "no tracking-error/vol estimate -- cannot risk-adjust, "
                          "refusing to gate on raw return alone"}

    ir = round(excess / tracking_error_pp, 3)
    passed = (ir >= ir_min) and (excess >= min_excess_pp)

    return {
        "status": "OK",
        "pass": passed,
        "verdict": "OUTPERFORM_EXPECTED" if passed else (
            "NEUTRAL" if excess > 0 else "UNDERPERFORM_EXPECTED"),
        "excess_return_pp": excess,
        "pp_kind": "estimated",
        "information_ratio": ir,
        "ir_min": ir_min,
        "min_excess_pp": min_excess_pp,
        "n_obs": n_obs,
        "ci": list(ci) if ci else None,
        "basis": "excess_return = sector_ER - SPX_ER (forward-returns market-implied "
                 "method); IR = excess / tracking_error; both must clear their floor",
    }


# ── Tier 3: stock-level composite (only run for industries that passed Tier 2) ──

DEFAULT_WEIGHTS = {
    "backlog_growth": 0.30,
    "valuation_discount": 0.25,   # PEG / P-E discount to industry median
    "catalyst_strength": 0.20,    # trailing 90d contract/8-K catalyst weight
    "net_share_retirement": 0.15,
    "qoq_acceleration": 0.10,
}


def cycle_awareness_flag(pe_now: Optional[float], pe_5y_pctile: Optional[float],
                          margin_pctile: Optional[float]) -> str:
    """Reused doctrine from justhodl-opportunity-engine v2: a cyclical name
    with a low P/E sitting on peak margins is a trap, not a bargain; a
    depressed-earnings name with a falling P/E on trough margins is an
    early-cycle turn. Applied here because every industry this engine
    surfaces (copper/memory/grid/EV) is by construction cyclical."""
    if pe_5y_pctile is None or margin_pctile is None:
        return "UNKNOWN"
    if pe_5y_pctile <= 35 and margin_pctile >= 80:
        return "PEAK_MARGIN_TRAP"
    if margin_pctile <= 30:
        return "EARLY_CYCLE_WATCH"
    return "NORMAL"


def stock_composite_score(components: dict, weights: Optional[dict] = None) -> dict:
    """components: dict of component_name -> value in [0,100] or None if
    that leg's data is missing for this ticker. Missing components are
    EXCLUDED and their weight redistributed proportionally across the
    remaining available components -- never silently treated as zero
    (a ticker with no backlog data is not the same as a ticker with zero
    backlog growth). If fewer than half the weight is available, the
    ticker is INSUFFICIENT_DATA rather than scored on a thin slice.

    `weights` defaults to DEFAULT_WEIGHTS but the caller (lambda_function)
    should pass evidence_weights-adjusted weights in production, per the
    fleet's existing learned-weight doctrine (aws/shared/evidence_weights.py) --
    this module stays AWS-free, so it accepts weights as data, it does not
    fetch them.
    """
    w = dict(weights or DEFAULT_WEIGHTS)
    available = {k: v for k, v in components.items() if v is not None and k in w}
    missing = [k for k in w if k not in available]

    available_weight = sum(w[k] for k in available)
    if available_weight < 0.5 * sum(w.values()):
        return {"status": "INSUFFICIENT_DATA", "score": None,
                "missing_components": missing,
                "reason": f"only {available_weight:.2f}/{sum(w.values()):.2f} "
                          f"weight has data"}

    scale = sum(w.values()) / available_weight
    score = sum(v * w[k] * scale for k, v in available.items())

    return {"status": "OK", "score": round(score, 1),
            "missing_components": missing,
            "components_used": list(available.keys()),
            "reweighted": missing != []}


def vs_industry_etf_verdict(stock_score: Optional[float],
                             industry_median_score: Optional[float],
                             threshold: float = 8.0) -> str:
    """A stock only earns OUTPERFORM_EXPECTED vs its OWN industry ETF if it
    clears the industry's median composite by a real margin -- being merely
    average within an already-gated-through industry is not a stock pick,
    it is a reason to just buy the ETF (proxy_etf in causal_graph)."""
    if stock_score is None or industry_median_score is None:
        return "INSUFFICIENT_DATA"
    if stock_score - industry_median_score >= threshold:
        return "OUTPERFORM_EXPECTED"
    if stock_score - industry_median_score <= -threshold:
        return "UNDERPERFORM_EXPECTED"
    return "IN_LINE_BUY_THE_ETF"
