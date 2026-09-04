"""Deterministic scoring for the Khalid asymmetric-opportunity engine.

The module has no AWS or network dependency.  It intentionally treats missing
evidence as missing, never as zero, and keeps discovery separate from entry
timing.  Long-horizon location is a hard gate; daily/4h signals may only arm an
entry after the thesis passes.
"""
from __future__ import annotations

from typing import Any


ACTION_ORDER = {
    "READY_TO_SNIPE": 4,
    "BUILDING_BASE": 3,
    "WATCH_RECLAIM": 2,
    "REJECTED": 1,
}


def number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def first_number(*values: Any) -> float | None:
    for value in values:
        parsed = number(value)
        if parsed is not None:
            return parsed
    return None


def contract_error(spec: dict, payload: dict) -> str | None:
    """Validate the explicit adapter contract for one source artifact."""
    contract = spec.get("contract") or {}
    required_all = contract.get("required_all") or []
    required_any = contract.get("required_any") or []
    missing = [key for key in required_all if payload.get(key) is None]
    if missing:
        return "missing required fields: " + ", ".join(missing)
    if required_any and not any(payload.get(key) is not None for key in required_any):
        return "missing all alternative fields: " + ", ".join(required_any)

    for key, expected in (contract.get("types") or {}).items():
        value = payload.get(key)
        if value is None:
            continue
        valid = (
            (expected == "list" and isinstance(value, list))
            or (expected == "string" and isinstance(value, str) and bool(value.strip()))
            or (
                expected == "number"
                and not isinstance(value, bool)
                and isinstance(value, (int, float))
            )
        )
        if not valid:
            return f"{key} must be {expected}"

    for key, allowed in (contract.get("allowed") or {}).items():
        value = payload.get(key)
        allowed_normalized = {str(item).upper() for item in allowed}
        if value is not None and str(value).upper() not in allowed_normalized:
            return f"{key} has unsupported value {value!r}"
    return None


def _add(items: list[dict], label: str, value: Any, source: str, detail: str) -> None:
    items.append({"label": label, "value": value, "source": source, "detail": detail})


def _location_score(vs200: float, vs250: float | None) -> float:
    # The ideal zone is just below the long moving averages, not a deep and
    # unconfirmed collapse.  Deep discounts can still pass, but only after
    # the independent bottom/accumulation gates below.
    score = 100.0 - min(abs(vs200) * 1.7, 55.0)
    if vs250 is not None:
        score = score * 0.65 + (100.0 - min(abs(vs250) * 1.4, 55.0)) * 0.35
    return clamp(score)


def score_candidate(
    row: dict,
    *,
    asset_class: str,
    risk_allows_entries: bool,
    bottom_confirmation: dict | None = None,
    best_setup: dict | None = None,
    floor: dict | None = None,
    buyback: dict | None = None,
    options: dict | None = None,
    source: str | None = None,
) -> dict:
    # evidence rows name the producer that supplied the field: fortress (default) or katlin (market-wide scan)
    src = source or str(row.get("_source") or "fortress")
    ticker = str(row.get("ticker") or row.get("symbol") or "").upper().strip()
    name = row.get("name") or row.get("company_name") or ticker
    bottom_confirmation = bottom_confirmation or {}
    best_setup = best_setup or {}
    floor = floor or {}
    buyback = buyback or {}
    options = options or row.get("options") or {}

    vs200 = first_number(row.get("vs_ema200_pct"), row.get("vs_sma200_pct"))
    vs250 = first_number(row.get("vs_ema250_pct"), row.get("vs_sma250_pct"))
    bb_pctile = number(row.get("bb_width_pctile"))
    rsi = first_number(bottom_confirmation.get("rsi"), row.get("rsi"), row.get("rsi_14"))
    plan = row.get("trade_plan") or {}
    price = number(row.get("close") or row.get("price"))
    planned_entry = first_number(plan.get("pivot"), row.get("breakout_level"), price)
    planned_stop = first_number(plan.get("stop"), row.get("range_low_60"))
    planned_target = first_number(plan.get("target_2"), row.get("target_price"), plan.get("target"))
    rr = None
    if (
        planned_entry is not None
        and planned_stop is not None
        and planned_target is not None
        and planned_stop < planned_entry < planned_target
    ):
        rr = (planned_target - planned_entry) / (planned_entry - planned_stop)
    adv = number(row.get("adv_usd_20d"))
    dilution = first_number(row.get("dilution_yoy_pct"), floor.get("dilution_yoy"))

    evidence: dict[str, list[dict]] = {
        "location": [],
        "accumulation": [],
        "flows": [],
        "catalysts": [],
        "valuation": [],
        "risk_reward": [],
    }
    vetoes: list[str] = []
    cautions: list[str] = []

    if vs200 is None:
        vetoes.append("No verified 200-day moving-average distance")
    elif vs200 > 0:
        vetoes.append(f"Price is {vs200:.1f}% above the 200-day average; Khalid does not chase")
    else:
        _add(evidence["location"], "Below 200-day", round(vs200, 1), src, "Required long-term location gate passed")

    if vs250 is not None and vs250 <= 0:
        _add(evidence["location"], "Below 250-day", round(vs250, 1), src, "Preferred deep-location confirmation")
    elif vs250 is not None:
        cautions.append(f"Price is {vs250:.1f}% above the 250-day average")

    if vs200 is not None and vs200 < -45:
        cautions.append("More than 45% below the 200-day average; falling-knife risk is elevated")
    if adv is not None and adv < 1_000_000:
        vetoes.append("20-day average dollar volume is below $1M")

    if bb_pctile is not None and bb_pctile <= 20:
        _add(evidence["accumulation"], "Tight Bollinger width", round(bb_pctile, 1), src, "Bandwidth is in the lowest quintile of its own history")
    if row.get("vcp_ok"):
        _add(evidence["accumulation"], "Volatility contraction", True, src, "Successive contractions suggest supply is drying up")
    volume_dryup = number(row.get("volume_dryup"))
    if volume_dryup is not None and volume_dryup <= 0.8:
        _add(evidence["accumulation"], "Supply dry-up precondition", round(volume_dryup, 2), src, "Base volume is at least 20% below its reference level; this is not proof by itself")
    if number(row.get("higher_lows")) and number(row.get("higher_lows")) >= 2:
        _add(evidence["accumulation"], "Higher lows", int(number(row.get("higher_lows"))), src, "Base structure is improving")
    if row.get("obv_divergence") or row.get("ad_divergence"):
        _add(evidence["accumulation"], "Volume divergence", True, src, "OBV or accumulation/distribution is improving before price")
    if (bottom_confirmation.get("phase") == "ACCUMULATION"
            or bottom_confirmation.get("flag") == "LIKELY_BOTTOM"):
        _add(evidence["accumulation"], "Independent bottom model", bottom_confirmation.get("flag") or bottom_confirmation.get("phase"), "accumulation-radar", "Separate Wyckoff/bottom model confirms the base")
    if rsi is not None and rsi <= 45:
        _add(evidence["accumulation"], "RSI reset", round(rsi, 1), "accumulation-radar", "Momentum is washed out rather than extended")
    if (row.get("dark_pool_state") == "ACCUMULATION"
            or (bottom_confirmation.get("dark_pool") or {}).get("state") == "ACCUMULATION"):
        cautions.append("Dark-pool volume has no verified trade-side direction and is context only")

    flows = row.get("flows") or {}
    if row.get("industry_inflow_major") or row.get("inflow_major") or flows.get("major"):
        _add(evidence["flows"], "Major fund inflow", True, src, "Industry or ETF flow is large relative to assets")
    flow_score = first_number(row.get("flow_score"), flows.get("score"))
    if flow_score is not None and flow_score >= 65:
        _add(evidence["flows"], "Persistent flow score", round(flow_score, 1), src, "Multi-source fund-flow score is strong")
    if first_number(row.get("inst_net_usd"), row.get("whale_net_usd"), bottom_confirmation.get("whale_flow_usd")) not in (None, 0):
        inst = first_number(row.get("inst_net_usd"), row.get("whale_net_usd"), bottom_confirmation.get("whale_flow_usd"))
        if inst and inst > 0:
            _add(evidence["flows"], "Institutional ownership flow", round(inst), src + "/13F", "Reported institutional or whale net flow is positive")
            cautions.append("Institutional filings are delayed context, not an entry trigger")
    option_blocks = number(options.get("n_smart_money_blocks"))
    if option_blocks is not None and option_blocks >= 2:
        cautions.append(f"{int(option_blocks)} options blocks observed; direction is not independently verified")

    catalyst_score = number(row.get("catalyst_score"))
    if row.get("has_contract_catalyst") or (catalyst_score is not None and catalyst_score >= 50):
        _add(evidence["catalysts"], "Contract catalyst", round(catalyst_score or 50, 1), src, "Recent contracts or an explicit catalyst are material")
    if row.get("demand_accelerating"):
        _add(evidence["catalysts"], "Demand acceleration", True, src, "Backlog or demand is accelerating")
    boom = number(row.get("industry_boom_score"))
    boom_delta = number(row.get("industry_boom_delta_20d"))
    if boom is not None and boom >= 60 and (boom_delta is None or boom_delta > 0):
        _add(evidence["catalysts"], "Industry boom", round(boom, 1), "industry-boom", "Physical/fundamental industry activity is strong and not decelerating")
    backlog_growth = first_number(row.get("backlog_qoq_pct"), row.get("rpo_qoq_pct"))
    if backlog_growth is not None and backlog_growth >= 10:
        _add(evidence["catalysts"], "Backlog/RPO acceleration", round(backlog_growth, 1), src + "/backlog", "Committed demand is rising")
    eps_next = number(row.get("eps_next_y_pct"))
    if eps_next is not None and eps_next >= 15:
        _add(evidence["catalysts"], "Forward EPS inflection", round(eps_next, 1), src, "Expected earnings growth can re-rate the asset")
    if row.get("insider_cluster"):
        _add(evidence["catalysts"], "Insider cluster", True, "insider-clusters", "Multiple insiders bought")
    net_buyback = first_number(row.get("net_buyback_yield_pct"), buyback.get("net_buyback_yield"))
    if net_buyback is not None and net_buyback >= 2:
        _add(evidence["catalysts"], "Net share retirement", round(net_buyback, 1), "buyback-engine", "Repurchases exceed issuance")

    valuation_score = number(row.get("valuation_score"))
    if valuation_score is not None:
        _add(evidence["valuation"], "Peer valuation score", round(valuation_score, 1), src, "Percentile score within the relevant industry")
    for key, label in (("fwd_pe", "Forward P/E"), ("peg", "PEG"), ("ev_ebitda", "EV/EBITDA"), ("fcf_yield_pct", "FCF yield")):
        value = number(row.get(key))
        if value is not None:
            _add(evidence["valuation"], label, round(value, 2), src, "Context metric, not a standalone buy signal")

    if rr is not None:
        _add(evidence["risk_reward"], "Entry-based reward/risk", round(rr, 2), "khalid", "Upside and downside are recomputed from the planned entry, target and stop")
        if rr < 2.0:
            vetoes.append(f"Modeled reward/risk is only {rr:.2f}x")
        elif rr < 2.5:
            cautions.append(f"Reward/risk {rr:.2f}x is below the preferred 2.5x bar")
    else:
        cautions.append("No valid entry/stop/target geometry for reward/risk")

    if dilution is not None:
        if dilution >= 10:
            vetoes.append(f"Share count increased {dilution:.1f}% year over year")
        elif dilution >= 3:
            cautions.append(f"Share count increased {dilution:.1f}% year over year")
    if buyback.get("net_issuer"):
        vetoes.append("Company is a net share issuer")
    dilution_checked = asset_class != "STOCK" or dilution is not None or buyback.get("net_issuer") is not None or net_buyback is not None
    if asset_class == "STOCK" and not dilution_checked:
        cautions.append("Dilution coverage is unavailable; the setup cannot be armed")
    if row.get("beneish_m") is not None and number(row.get("beneish_m")) is not None and number(row.get("beneish_m")) > -1.78:
        cautions.append("Beneish screen warrants accounting review")
    if best_setup.get("red_flags"):
        cautions.extend(str(x) for x in best_setup.get("red_flags")[:3])

    location_score = _location_score(vs200, vs250) if vs200 is not None else 0.0
    accum_score = clamp(len(evidence["accumulation"]) * 18.0)
    flow_component = clamp((flow_score or 0.0) * 0.65 + len(evidence["flows"]) * 12.0)
    catalyst_component = clamp((catalyst_score or 0.0) * 0.55 + len(evidence["catalysts"]) * 16.0)
    valuation_component = clamp(valuation_score if valuation_score is not None else (50.0 if evidence["valuation"] else 0.0))
    asymmetry = number(row.get("asymmetry"))
    safety = number(row.get("safety_score"))
    rr_component = clamp((rr or 0.0) * 22.0)
    if safety is not None:
        rr_component = clamp(rr_component * 0.65 + safety * 0.35)
    score = round(
        location_score * 0.22
        + accum_score * 0.22
        + flow_component * 0.18
        + catalyst_component * 0.16
        + valuation_component * 0.10
        + rr_component * 0.12,
        1,
    )
    reported_confidence = number(row.get("confidence"))
    candidate_confidence = (
        clamp(reported_confidence, 0.0, 1.0)
        if reported_confidence is not None
        else min(0.95, 0.45 + 0.05 * sum(len(x) for x in evidence.values()))
    )
    confidence_ok = candidate_confidence >= 0.60
    if not confidence_ok:
        cautions.append(f"Candidate confidence {candidate_confidence:.2f} is below the 0.60 entry floor")

    compression_ok = bb_pctile is not None and bb_pctile <= 20
    supply_ok = volume_dryup is not None and volume_dryup <= 0.8
    rsi_reset = rsi is not None and rsi <= 45
    structural_ok = (
        (number(row.get("higher_lows")) or 0) >= 2
        or bool(row.get("obv_divergence") or row.get("ad_divergence"))
        or bottom_confirmation.get("phase") == "ACCUMULATION"
        or bottom_confirmation.get("flag") == "LIKELY_BOTTOM"
    )
    critical_ok = vs200 is not None and vs200 <= 0 and compression_ok and supply_ok and structural_ok
    thesis_ok = critical_ok and len(evidence["flows"]) >= 1 and len(evidence["catalysts"]) >= 1
    entry_ok = (
        thesis_ok
        and rsi_reset
        and dilution_checked
        and confidence_ok
        and not vetoes
        and rr is not None
        and rr >= 2.5
        and risk_allows_entries
    )
    if entry_ok:
        action = "READY_TO_SNIPE"
    elif vs200 is not None and 0 < vs200 <= 3 and not vetoes[1:]:
        action = "WATCH_RECLAIM"
    elif critical_ok and not vetoes:
        action = "BUILDING_BASE"
    else:
        action = "REJECTED"

    if not risk_allows_entries and action == "READY_TO_SNIPE":
        action = "BUILDING_BASE"
        cautions.append("Macro/risk gate blocks new risk until conditions improve")

    plain = []
    if vs200 is not None:
        plain.append(f"{ticker} is {abs(vs200):.1f}% {'below' if vs200 <= 0 else 'above'} its 200-day trend")
    if evidence["accumulation"]:
        plain.append(f"{len(evidence['accumulation'])} separate base/accumulation clues agree")
    if evidence["flows"]:
        plain.append(f"{len(evidence['flows'])} capital-flow clues are positive")
    if evidence["catalysts"]:
        plain.append(f"{len(evidence['catalysts'])} identifiable catalyst clues could unlock the setup")
    if vetoes:
        plain.append("it is not actionable because " + "; ".join(vetoes[:2]).lower())
    elif action == "READY_TO_SNIPE":
        plain.append("the long-term thesis passes, but entry still waits for a daily/4h trigger")
    elif action == "BUILDING_BASE":
        plain.append("the base is interesting, but at least one required confirmation is still missing")

    katlin_block = row.get("_katlin") if isinstance(row.get("_katlin"), dict) else None
    return {
        "source": src,
        "katlin": katlin_block,
        "ticker": ticker,
        "name": name,
        "asset_class": asset_class,
        "action": action,
        "score": score,
        "confidence": round(candidate_confidence, 2),
        "timeframe": {
            "thesis": "3M / 1M / weekly",
            "entry": "daily confirmation, then 4h execution",
            "rule": "4h and daily may time an entry; they never create the long-term thesis",
        },
        "price": price,
        "technical": {
            "vs_200d_pct": round(vs200, 2) if vs200 is not None else None,
            "vs_250d_pct": round(vs250, 2) if vs250 is not None else None,
            "rsi": round(rsi, 1) if rsi is not None else None,
            "bb_width_percentile": round(bb_pctile, 1) if bb_pctile is not None else None,
            "higher_lows": row.get("higher_lows"),
            "bottom_confirmation": bottom_confirmation.get("flag") or bottom_confirmation.get("phase"),
        },
        "valuation": {
            "score": round(valuation_score, 1) if valuation_score is not None else None,
            "pe": number(row.get("pe")),
            "forward_pe": number(row.get("fwd_pe")),
            "peg": number(row.get("peg")),
            "ev_ebitda": number(row.get("ev_ebitda")),
            "fcf_yield_pct": number(row.get("fcf_yield_pct")),
        },
        "dilution": {
            "yoy_pct": round(dilution, 2) if dilution is not None else None,
            "active": floor.get("dilution_active"),
            "net_issuer": buyback.get("net_issuer"),
            "net_buyback_yield_pct": round(net_buyback, 2) if net_buyback is not None else None,
        },
        "risk_reward": {
            "ratio": round(rr, 2) if rr is not None else None,
            "pivot": planned_entry,
            "stop": planned_stop,
            "target_1": number(plan.get("target")),
            "target_2": planned_target,
            "empirical_dump_loss_pct": number(row.get("empirical_dump_loss_pct")),
            "asymmetry": round(asymmetry, 2) if asymmetry is not None else None,
        },
        "entry_trigger": {
            "state": "ARMED" if action == "READY_TO_SNIPE" else "WAIT",
            "daily": "Close through the base pivot with volume; no gap-chasing",
            "four_hour": "Retest/hold of the pivot or higher low after breakout",
            "invalidation": row.get("invalidation") or "Close below the base low on expanding volume",
        },
        "evidence": evidence,
        "vetoes": vetoes,
        "cautions": list(dict.fromkeys(cautions)),
        "plain_english": ". ".join(plain) + ("." if plain else ""),
        "deep_links": [
            "/fortress.html",
            "/accumulation.html",
            "/dark-pool.html",
            "/options.html",
            "/catalyst.html",
        ],
    }


def rank_candidates(candidates: list[dict]) -> list[dict]:
    return sorted(
        candidates,
        key=lambda x: (ACTION_ORDER.get(x.get("action"), 0), x.get("score") or 0),
        reverse=True,
    )


def risk_policy(risk_gate: dict, crisis: dict, source_health: list[dict]) -> dict:
    posture = str(risk_gate.get("posture") or "UNKNOWN").upper()
    composite = number(risk_gate.get("composite"))
    sizing = number(risk_gate.get("sizing_multiplier"))
    defcon = first_number(crisis.get("defcon_level"), crisis.get("defcon"))
    critical_bad = [
        x for x in source_health
        if x.get("critical") and x.get("status") not in ("FRESH", "OK")
    ]
    reasons = []
    allowed_postures = {"RISK_ON", "NEUTRAL", "RISK_OFF", "SEVERE"}
    if critical_bad or posture not in allowed_postures or defcon is None:
        mode = "DATA_HOLD"
        reasons.append("Critical risk inputs are stale, missing, or outside their contract")
    elif posture in {"SEVERE", "RISK_OFF"} or (defcon is not None and defcon <= 2):
        mode = "DEFENSIVE"
        reasons.append("Macro/credit/plumbing gate vetoes new risk")
    elif posture == "NEUTRAL":
        mode = "SELECTIVE"
        reasons.append("Only fully confirmed asymmetric setups may pass")
    else:
        mode = "SELECTIVE_RISK_ON"
        reasons.append("Risk backdrop permits entries, but location and evidence gates still apply")
    allows = mode in {"SELECTIVE", "SELECTIVE_RISK_ON"}
    if posture == "NEUTRAL":
        allows = True
    return {
        "mode": mode,
        "allows_new_entries": allows,
        "risk_gate_posture": posture,
        "risk_gate_composite": composite,
        "risk_gate_sizing_multiplier": sizing,
        "crisis_defcon": int(defcon) if defcon is not None else None,
        "reasons": reasons,
        "default_shelter": {
            "primary": "CASH" if mode == "DATA_HOLD" else "SGOV / BIL",
            "why": "Preserve optionality until the risk gate and opportunity evidence both clear",
            "avoid": "Unhedged duration or credit risk unless the bond desk independently confirms it",
        },
    }
