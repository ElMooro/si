"""Dependency-free authoritative Khalid risk policy.

This is a local refactor of the mature Khalid risk-board rules.  It intentionally
lives with its Lambda (no cross-Lambda imports).  Independent lenses remain
visible and rule-based vetoes/caps are authoritative; no mega-average controls
capital.  Missing values stay missing and never become neutral scores.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, time, timezone
from typing import Any

SCHEMA_VERSION = "1.0.0"
MAX_FUTURE_SKEW_HOURS = 5 / 60
MODES = {"DATA_HOLD", "DEFENSIVE", "SELECTIVE", "SELECTIVE_RISK_ON"}
DECISIONS = {
    "STAY IN CASH / SHORT-TERM TREASURIES",
    "INVEST SELECTIVELY",
    "WAIT IN CASH / SHORT-TERM TREASURIES",
}


def number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def mapping(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def rows(value: Any) -> list[dict]:
    return [x for x in value if isinstance(x, dict)] if isinstance(value, list) else []


def text(value: Any, fallback: str = "Unavailable") -> str:
    out = str(value or "").strip()
    return out if out else fallback


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def dotted_get(payload: dict, path: str) -> Any:
    value: Any = payload
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def parse_time(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, time.min, tzinfo=timezone.utc)
    else:
        raw = str(value).strip()
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = datetime.strptime(raw[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def age_hours(value: Any, now: datetime) -> float | None:
    dt = parse_time(value)
    if dt is None:
        return None
    return round((now.astimezone(timezone.utc) - dt).total_seconds() / 3600.0, 2)


def contract_error(spec: dict, payload: dict) -> str | None:
    if not isinstance(payload, dict):
        return "payload must be object"
    contract = spec.get("contract") or {}
    missing = [key for key in contract.get("required_all") or [] if payload.get(key) is None]
    if missing:
        return "missing required fields: " + ", ".join(missing)
    alternatives = contract.get("required_any") or []
    if alternatives and not any(payload.get(key) is not None for key in alternatives):
        return "missing all alternative fields: " + ", ".join(alternatives)
    for key, expected in (contract.get("types") or {}).items():
        value = payload.get(key)
        if value is None:
            continue
        valid = (
            (expected == "dict" and isinstance(value, dict))
            or (expected == "list" and isinstance(value, list))
            or (expected == "string" and isinstance(value, str) and bool(value.strip()))
            or (expected == "number" and number(value) is not None and isinstance(value, (int, float)))
        )
        if not valid:
            return f"{key} must be {expected}"
    for key, expected in (contract.get("item_types") or {}).items():
        value = payload.get(key)
        if value is None:
            continue
        if not isinstance(value, list):
            return f"{key} must be list before validating items"
        if expected == "dict" and any(not isinstance(item, dict) for item in value):
            return f"{key} items must be dict"
    for key, allowed in (contract.get("allowed") or {}).items():
        value = payload.get(key)
        if value is not None and str(value).upper() not in {str(x).upper() for x in allowed}:
            return f"{key} has unsupported value {value!r}"
    for key, limits in (contract.get("ranges") or {}).items():
        value = payload.get(key)
        if value is None:
            continue
        parsed = number(value)
        if parsed is None or not limits[0] <= parsed <= limits[1]:
            return f"{key} must be finite and within {limits[0]}..{limits[1]}"
    return None


def _series_error(series: Any, label: str) -> str | None:
    if not isinstance(series, list) or not series:
        return f"treasury.{label} must be a non-empty series"
    last_date = None
    for point in series:
        if not isinstance(point, list) or len(point) != 2 or parse_time(point[0]) is None:
            return f"treasury.{label} has malformed point"
        value = number(point[1])
        if value is None or value < 0 or value > 1_000_000:
            return f"treasury.{label} value must be finite and within 0..1000000 USD bn par"
        if last_date is not None and str(point[0]) <= last_date:
            return f"treasury.{label} dates must be strictly increasing"
        last_date = str(point[0])
    return None


def semantic_error(source_id: str, payload: dict) -> str | None:
    if source_id in {"credit_composite", "eurodollar_stress"}:
        score = number(payload.get("composite") if source_id == "credit_composite" and payload.get("composite") is not None else payload.get("composite_score"))
        return None if score is not None and 0 <= score <= 100 else f"{source_id} score must be finite and within 0..100"
    if source_id == "bond_warroom":
        checks = [
            ("heartbeat", "regime"),
            ("equity_risk", "state"),
            ("eurodollar_shortage", "state"),
        ]
        missing = []
        for block, state_key in checks:
            item = mapping(payload.get(block))
            score = number(item.get("score"))
            state = item.get(state_key) or (item.get("level") if block == "equity_risk" else None)
            if score is None or not 0 <= score <= 100 or not text(state, ""):
                missing.append(f"{block}.score/{state_key}")
        if not isinstance(payload.get("panels"), dict) or not payload.get("panels"):
            missing.append("panels")
        return "bond risk fields missing: " + ", ".join(missing) if missing else None
    if source_id == "crisis":
        components = rows(payload.get("components"))
        reported = number(payload.get("components_available"))
        if not components or reported is None:
            return "crisis component coverage missing"
        available = [r for r in components if r.get("available") is True]
        if reported < max(1, len(components) * 0.75) or len(available) < len(components) * 0.75:
            return "crisis component coverage below 75%"
        fresh = 0
        for row in available:
            max_age = 192 if str(row.get("source") or "").lower() in {"ciss", "ciss_ea"} else 72
            age = number(row.get("age_hours"))
            fresh += bool(age is not None and age <= max_age)
        if fresh < len(components) * 0.75:
            return "crisis fresh component coverage below 75%"
        return None
    if source_id == "settlement_fails":
        treasury = mapping(payload.get("treasury"))
        if treasury.get("scope") != "US_TREASURY_INCLUDING_TIPS" or treasury.get("unit") != "USD_bn_par":
            return "treasury scope/unit contract invalid"
        if treasury.get("complete") is not True:
            return "treasury common-date completeness is false"
        for key in ("ftd_bn", "ftr_bn", "gross_bn"):
            value = number(treasury.get(key))
            if value is None or value < 0 or value > 1_000_000:
                return f"treasury.{key} must be finite and within 0..1000000"
        if abs((number(treasury.get("ftd_bn")) or 0) + (number(treasury.get("ftr_bn")) or 0) - (number(treasury.get("gross_bn")) or 0)) > 0.02:
            return "treasury gross must equal ftd + ftr"
        for key in ("ftd", "ftr", "gross"):
            err = _series_error(treasury.get(key), key)
            if err:
                return err
        stats = mapping(treasury.get("stats"))
        for side in ("ftd", "ftr", "gross"):
            block = mapping(stats.get(side))
            for metric in ("latest", "z", "pctile"):
                value = number(block.get(metric))
                if value is None:
                    return f"treasury.stats.{side}.{metric} must be finite"
            if not 0 <= number(block.get("pctile")) <= 100:
                return f"treasury.stats.{side}.pctile outside 0..100"
        if text(treasury.get("regime"), "") not in {"CALM", "ELEVATED", "STRESS", "CRISIS"}:
            return "treasury.regime invalid"
    return None


def source_timestamp(spec: dict, payload: dict, meta: dict) -> Any:
    for path in spec.get("timestamp_paths") or []:
        value = dotted_get(payload, path)
        if value is not None:
            return value
    return meta.get("last_modified")


def audit_sources(registry: dict, feeds: dict[str, dict], metas: dict[str, dict], now: datetime) -> tuple[list[dict], dict[str, dict]]:
    health = []
    active = {}
    for spec in registry.get("inputs") or []:
        source_id = spec["id"]
        payload = feeds.get(source_id)
        meta = metas.get(source_id) or {}
        timestamp = source_timestamp(spec, payload or {}, meta)
        age = age_hours(timestamp, now)
        error = None
        if meta.get("error") or not isinstance(payload, dict) or not payload:
            status = "MISSING"
            error = meta.get("error") or "artifact missing or empty"
        else:
            error = contract_error(spec, payload) or semantic_error(source_id, payload)
            if error:
                status = "INVALID"
            elif age is None:
                status = "UNKNOWN"
                error = "no parseable contract timestamp"
            elif age < -MAX_FUTURE_SKEW_HOURS:
                status = "INVALID"
                error = f"timestamp is {abs(age):.1f}h in the future"
            elif age > float(spec["max_age_hours"]):
                status = "STALE"
                error = f"{age:.1f}h old; SLA {spec['max_age_hours']}h"
            else:
                status = "FRESH"
                active[source_id] = payload
        health.append({
            "name": source_id,
            "key": spec["artifact"],
            "producer": spec["producer"],
            "domain": spec["domain"],
            "critical": bool(spec.get("critical")),
            "status": status,
            "as_of": timestamp,
            "age_h": age,
            "max_age_h": spec["max_age_hours"],
            "freshness_basis": spec.get("freshness_basis", "artifact_timestamp"),
            "error": error,
        })
    return health, active


def _health(source_health: list[dict], source_id: str) -> dict:
    return next((row for row in source_health if row.get("name") == source_id), {})


def risk_level(score: float | None) -> str:
    if score is None:
        return "UNKNOWN"
    if score >= 80:
        return "CRITICAL"
    if score >= 65:
        return "HIGH"
    if score >= 45:
        return "ELEVATED"
    if score >= 25:
        return "WATCH"
    return "CALM"


def card(source_health: list[dict], source_id: str, label: str, score: float | None, state: Any, summary: Any, metrics: list[dict]) -> dict:
    health = _health(source_health, source_id)
    return {
        "id": source_id,
        "domain": health.get("domain"),
        "label": label,
        "score": round(score, 1) if score is not None else None,
        "severity": risk_level(score),
        "state": text(state, "UNKNOWN"),
        "summary": text(summary),
        "status": health.get("status", "UNKNOWN"),
        "age_h": health.get("age_h"),
        "as_of": health.get("as_of"),
        "artifact": health.get("key"),
        "metrics": [m for m in metrics if m.get("value") is not None or m.get("state") is not None],
    }


def currency(currencies: Any, code: str) -> dict:
    if isinstance(currencies, dict):
        return mapping(currencies.get(code) or currencies.get(code.lower()))
    return next((row for row in rows(currencies) if str(row.get("code") or "").upper() == code), {})


def bond_panel(panels: Any, key: str) -> dict:
    for panel_rows in mapping(panels).values():
        for row in rows(panel_rows):
            if str(row.get("key") or "").upper() == key.upper():
                return row
    return {}


def auction_summary(auction: dict) -> tuple[float | None, str, str, list[dict]]:
    today = mapping(auction.get("today")); verdict = mapping(today.get("verdict"))
    auctions = rows(today.get("auctions")); coupons = [r for r in auctions if str(r.get("type") or "").lower() != "bill"]
    observed = coupons or auctions
    demand = [number(r.get("demand_score")) for r in observed]
    demand = [x for x in demand if x is not None]
    score = clamp(50 - (sum(demand) / len(demand)) * 25) if demand else None
    state = str(verdict.get("risk_assets") or "UNKNOWN").upper()
    if state == "BEARISH": score = max(score if score is not None else 0, 65)
    latest = observed[0] if observed else {}
    return score, state, text(verdict.get("headline"), "No current auction verdict"), [
        {"label":"Latest grade","value":latest.get("grade")}, {"label":"Bid-to-cover","value":number(latest.get("btc"))},
        {"label":"Indirect bidders","value":number(latest.get("indirect_pct")),"unit":"%"}, {"label":"Dealer take","value":number(latest.get("pd_pct")),"unit":"%"},
        {"label":"Tail","value":number(latest.get("tail_bp")),"unit":"bp"},
    ]


def base_policy(risk_gate: dict, crisis: dict, source_health: list[dict]) -> dict:
    posture = str(risk_gate.get("posture") or "UNKNOWN").upper()
    composite = number(risk_gate.get("composite")); sizing = number(risk_gate.get("sizing_multiplier")); defcon = number(crisis.get("defcon_level"))
    bad = [x for x in source_health if x.get("critical") and x.get("status") != "FRESH"]
    reasons = []
    if bad or posture not in {"RISK_ON","NEUTRAL","RISK_OFF","SEVERE"} or composite is None or sizing is None or not 0 <= sizing <= 1 or defcon is None:
        mode = "DATA_HOLD"; reasons.append("Critical risk inputs are stale, missing, or outside their contract")
    elif posture in {"SEVERE","RISK_OFF"} or defcon <= 2:
        mode = "DEFENSIVE"; reasons.append("Master risk gate or crisis DEFCON vetoes new risk")
    elif posture == "NEUTRAL":
        mode = "SELECTIVE"; reasons.append("Only fully confirmed asymmetric setups may pass")
    else:
        mode = "SELECTIVE_RISK_ON"; reasons.append("Master risk gate permits entries subject to independent tighten-only lenses")
    return {
        "mode": mode, "allows_new_entries": mode in {"SELECTIVE","SELECTIVE_RISK_ON"},
        "risk_gate_posture": posture, "risk_gate_composite": composite,
        "risk_gate_sizing_multiplier": sizing, "sizing_multiplier": sizing if sizing is not None else 0.0,
        "crisis_defcon": int(defcon) if defcon is not None else None, "reasons": reasons,
        "default_shelter": {"primary":"CASH" if mode == "DATA_HOLD" else "SGOV / BIL", "why":"Preserve optionality until every critical risk contract clears", "avoid":"Unhedged duration or credit risk unless independently confirmed"},
    }


def build_board(active: dict[str, dict], source_health: list[dict], policy: dict) -> tuple[dict, dict]:
    policy = {**policy, "reasons": list(policy.get("reasons") or [])}
    risk_gate=mapping(active.get("risk_gate")); crisis=mapping(active.get("crisis")); bond=mapping(active.get("bond_warroom")); dollar=mapping(active.get("dollar_radar")); euro=mapping(active.get("euro_fragmentation")); auction=mapping(active.get("auction_desk")); eurodollar=mapping(active.get("eurodollar_stress")); credit=mapping(active.get("credit_composite")); vol=mapping(active.get("fifx_vol")); liquidity=mapping(active.get("global_liquidity")); cycle=mapping(active.get("cycle_clock")); fx=mapping(active.get("fx_intelligence")); settlement=mapping(active.get("settlement_fails"))
    heartbeat=mapping(bond.get("heartbeat")); bond_equity=mapping(bond.get("equity_risk")); shortage=mapping(bond.get("eurodollar_shortage"))
    frag=mapping(euro.get("fragmentation")); countries=mapping(euro.get("countries")); italy=mapping(countries.get("IT")); spain=mapping(countries.get("ES"))
    btp=number(italy.get("spread_vs_bund_bp")); ites=None
    if number(italy.get("yield_10y_pct")) is not None and number(spain.get("yield_10y_pct")) is not None: ites=(number(italy.get("yield_10y_pct"))-number(spain.get("yield_10y_pct")))*100
    if btp is None:
        v=number(bond_panel(bond.get("panels"),"BTP-BUND").get("last")); btp=v*100 if v is not None else None
    if ites is None:
        v=number(bond_panel(bond.get("panels"),"IT-ES").get("last")); ites=v*100 if v is not None else None
    usd=mapping(fx.get("usd_regime")); jpy=currency(fx.get("currencies"),"JPY"); eur=currency(fx.get("currencies"),"EUR")
    legs=mapping(vol.get("legs")); migration=mapping(vol.get("migration")); synthesis=mapping(cycle.get("synthesis")); cycle_block=mapping(cycle.get("cycle")); treasury=mapping(settlement.get("treasury")); treasury_stats=mapping(mapping(treasury.get("stats")).get("gross"))
    auction_score,auction_state,auction_text,auction_metrics=auction_summary(auction)
    credit_score=number(credit.get("composite")); credit_score=credit_score if credit_score is not None else number(credit.get("composite_score"))
    eurodollar_score=number(eurodollar.get("composite_score")); frag_score=number(frag.get("score_0_100")); dollar_score=number(dollar.get("dollar_pressure")); bond_score=number(bond_equity.get("score")); heartbeat_score=number(heartbeat.get("score")); crisis_score=number(crisis.get("master_crisis_score")); defcon=number(crisis.get("defcon_level"))
    cycle_posture=str(synthesis.get("posture") or "UNKNOWN").upper(); cycle_raw=number(synthesis.get("score")); cycle_risk=clamp(50-cycle_raw) if cycle_raw is not None else None
    migration_state=str(migration.get("state") or "UNKNOWN").upper(); migration_score={"CALM":15,"UPSTREAM_BREWING":60,"MIGRATING":75,"BROAD_STRESS":90}.get(migration_state)
    liquidity_regime=str(liquidity.get("regime") or "UNKNOWN").upper(); liquidity_score={"EXPANDING":15,"NEUTRAL":40,"CONTRACTING":70}.get(liquidity_regime)
    fx_state=str(usd.get("risk_state") or fx.get("risk_state") or "UNKNOWN").upper(); fx_score={"CALM":15,"LOW":20,"WATCH":40,"ELEVATED":60,"HIGH":75,"SEVERE":90,"CRISIS":95}.get(fx_state)
    if fx_score is None and "RISK-OFF" in fx_state: fx_score=70
    elif fx_score is None and "RISK-ON" in fx_state: fx_score=20
    settlement_score=number(treasury.get("score")); settlement_regime=str(treasury.get("regime") or "UNKNOWN").upper()
    gate_composite=number(risk_gate.get("composite")); gate_score=clamp(50-gate_composite*25) if gate_composite is not None else None
    cards=[
        card(source_health,"risk_gate","Master risk gate",gate_score,risk_gate.get("posture"),"Authoritative master capital gate.",[{"label":"Composite","value":gate_composite},{"label":"Sizing multiplier","value":number(risk_gate.get("sizing_multiplier")),"unit":"x"}]),
        card(source_health,"crisis","Crisis composite coverage",crisis_score,f"DEFCON {int(defcon)}" if defcon is not None else "UNKNOWN","Coverage and cadence are audited before use.",[{"label":"Crisis score","value":crisis_score,"unit":"/100"},{"label":"Components available","value":crisis.get("components_available")}]),
        card(source_health,"credit_composite","ICE BofA + credit plumbing",credit_score,risk_level(credit_score),"Credit deterioration is the primary continuous cap modifier.",[{"label":"Credit composite","value":credit_score,"unit":"/100"}]),
        card(source_health,"eurodollar_stress","Eurodollar shortage",eurodollar_score,eurodollar.get("severity") or eurodollar.get("regime"),"Offshore-dollar and money-market stress.",[{"label":"Stress composite","value":eurodollar_score,"unit":"/100"},{"label":"Signals used","value":number(eurodollar.get("n_signals_used"))},{"label":"Failures","value":number(eurodollar.get("n_failures"))}]),
        card(source_health,"settlement_fails","U.S. Treasury settlement fails",settlement_score,settlement_regime,"Weekly US Treasury scope including TIPS; ex-TIPS plus TIPS only, never all-asset totals.",[{"label":"Fails to deliver","value":number(treasury.get("ftd_bn")),"unit":"USD bn par"},{"label":"Fails to receive","value":number(treasury.get("ftr_bn")),"unit":"USD bn par"},{"label":"Gross fails","value":number(treasury.get("gross_bn")),"unit":"USD bn par"},{"label":"Gross z-score","value":number(treasury_stats.get("z"))},{"label":"Gross percentile","value":number(treasury_stats.get("pctile")),"unit":"%"},{"label":"Observation as-of","value":treasury.get("as_of")}]),
        card(source_health,"dollar_radar","DXY + broad dollar",dollar_score,dollar.get("regime"),dollar.get("headline") or dollar.get("regime_note"),[{"label":"Dollar pressure","value":dollar_score,"unit":"/100"},{"label":"Broad USD 3m","value":number(usd.get("chg_3m")),"unit":"%"}]),
        card(source_health,"euro_fragmentation","BTP-Bund + Italy-Spain",frag_score,frag.get("regime"),euro.get("headline") or euro.get("read"),[{"label":"BTP-Bund","value":btp,"unit":"bp"},{"label":"Italy-Spain","value":round(ites,1) if ites is not None else None,"unit":"bp"},{"label":"Fragmentation","value":frag_score,"unit":"/100"}]),
        card(source_health,"auction_desk","Treasury auction demand",auction_score,auction_state,auction_text,auction_metrics),
        card(source_health,"bond_warroom","Bond market war room",max([v for v in (bond_score,heartbeat_score) if v is not None],default=None),heartbeat.get("regime") or bond_equity.get("state"),heartbeat.get("headline") or bond_equity.get("text"),[{"label":"Heartbeat","value":heartbeat_score,"unit":"/100"},{"label":"Equity transmission","value":bond_score,"unit":"/100"},{"label":"Shortage score","value":number(shortage.get("score")),"unit":"/100"}]),
        card(source_health,"fifx_vol","Rates, FX and equity volatility",migration_score,migration_state,migration.get("read"),[{"label":"MOVE","value":number(mapping(legs.get("fixed_income")).get("level"))},{"label":"FX realized vol","value":number(mapping(legs.get("fx")).get("level_pct")),"unit":"%"},{"label":"VIX","value":number(mapping(legs.get("equity")).get("level"))}]),
        card(source_health,"fx_intelligence","FX intelligence",fx_score,fx_state,fx.get("headline") or usd.get("read"),[{"label":"USD risk state","value":fx_state},{"label":"JPY vol 20d","value":number(jpy.get("vol_20d")),"unit":"%"},{"label":"EUR vol 20d","value":number(eur.get("vol_20d")),"unit":"%"}]),
        card(source_health,"global_liquidity","Global liquidity",liquidity_score,liquidity_regime,liquidity.get("regime_read"),[{"label":"G3 impulse 13w","value":number(liquidity.get("global_impulse_13w_pct")),"unit":"%"},{"label":"Global liquidity","value":number(mapping(liquidity.get("global_liquidity_index")).get("total_usd_trillions")),"unit":"$T"},{"label":"Fed net liquidity","value":number(mapping(liquidity.get("fed_net_liquidity")).get("value_usd_trillions")),"unit":"$T"}]),
        card(source_health,"cycle_clock","Market cycle",cycle_risk,cycle_posture,synthesis.get("bottom_line") or cycle.get("verdict"),[{"label":"Cycle phase","value":cycle_block.get("headline_phase") or cycle_block.get("phase")},{"label":"Posture score","value":cycle_raw}]),
    ]
    hard=[]; tight=[]
    if credit_score is not None: (hard if credit_score>=75 else tight if credit_score>=45 else []).append(f"Credit composite is {credit_score:.0f}/100")
    if eurodollar_score is not None: (hard if eurodollar_score>=70 else tight if eurodollar_score>=50 else []).append(f"Eurodollar stress is {eurodollar_score:.0f}/100")
    if frag_score is not None: (hard if frag_score>=80 else tight if frag_score>=60 else []).append(f"Euro fragmentation is {frag_score:.0f}/100")
    if settlement_regime == "CRISIS" or (settlement_score is not None and settlement_score>=80): hard.append(f"Treasury settlement fails are {settlement_regime} ({settlement_score:.0f}/100)")
    elif settlement_regime == "STRESS" or (settlement_score is not None and settlement_score>=60): tight.append(f"Treasury settlement fails are {settlement_regime} ({settlement_score:.0f}/100)")
    if dollar_score is not None and dollar_score>=70: tight.append(f"Dollar pressure is {dollar_score:.0f}/100")
    if auction_state=="BEARISH": tight.append("Treasury auction read is bearish for risk assets")
    if migration_state=="BROAD_STRESS": hard.append("Rates, FX and equity volatility are in broad stress")
    elif migration_state in {"MIGRATING","UPSTREAM_BREWING"}: tight.append(f"Cross-asset volatility state is {migration_state}")
    if liquidity_regime=="CONTRACTING": tight.append("Global liquidity is contracting")
    if cycle_posture=="RISK-OFF": tight.append("Cycle clock is risk-off")
    if str(bond_equity.get("state") or "").upper() in {"STRESS","CRISIS","SEVERE","DUMP RISK","FLIGHT TO SAFETY"}: hard.append(f"Bond-to-equity transmission is {bond_equity.get('state')}")
    if str(heartbeat.get("regime") or "").upper()=="ACUTE": hard.append("Bond-market heartbeat is acute")
    if fx_state in {"SEVERE","CRISIS"}: hard.append(f"FX risk state is {fx_state}")
    # Tighten-only state transition: neither diagnostics nor missing optional data may loosen the master gate.
    if policy["mode"] not in {"DATA_HOLD","DEFENSIVE"}:
        if hard: policy["mode"]="DEFENSIVE"; policy["allows_new_entries"]=False; policy["reasons"].append("Independent market-risk veto: "+"; ".join(hard[:3]))
        elif tight: policy["mode"]="SELECTIVE"; policy["reasons"].append("Exposure cap tightened: "+"; ".join(tight[:3]))
    base=number(policy.get("sizing_multiplier")); cap=round((base if base is not None else 0)*100)
    if credit_score is not None: cap=min(cap,round(max(0,100-credit_score)))
    if eurodollar_score is not None: cap=min(cap,round(max(0,115-eurodollar_score)))
    if frag_score is not None: cap=min(cap,round(max(0,120-frag_score)))
    if migration_score is not None: cap=min(cap,round(max(0,120-migration_score)))
    if settlement_score is not None: cap=min(cap,round(max(0,110-settlement_score)))
    if policy["mode"]=="DATA_HOLD": cap=0
    elif policy["mode"]=="DEFENSIVE": cap=min(cap,10)
    elif policy["mode"]=="SELECTIVE": cap=min(cap,50)
    policy["sizing_multiplier"]=min(base if base is not None else 0,cap/100); policy["exposure_cap_pct"]=cap
    decision="STAY IN CASH / SHORT-TERM TREASURIES" if policy["mode"] in {"DATA_HOLD","DEFENSIVE"} else "INVEST SELECTIVELY" if policy["allows_new_entries"] else "WAIT IN CASH / SHORT-TERM TREASURIES"
    # The display risk score is the maximum fresh observed lens, not an average and not the decision rule.
    observed=[c["score"] for c in cards if c["status"]=="FRESH" and c["score"] is not None]
    conflicts=[]
    if risk_gate.get("posture") in {"RISK_ON","NEUTRAL"} and cycle_posture=="RISK-OFF": conflicts.append({"label":"Risk-gate / cycle disagreement","detail":f"Master gate {risk_gate.get('posture')} versus cycle {cycle_posture}"})
    if risk_gate.get("posture") in {"RISK_ON","NEUTRAL"} and settlement_regime in {"STRESS","CRISIS"}: conflicts.append({"label":"Risk-gate / settlement disagreement","detail":f"Master gate {risk_gate.get('posture')} versus Treasury fails {settlement_regime}"})
    return {"schema_version":SCHEMA_VERSION,"capital_decision":decision,"mode":policy["mode"],"allows_new_entries":bool(policy["allows_new_entries"]),"exposure_cap_pct":cap,"risk_score":max(observed) if observed else None,"risk_score_method":"maximum fresh observed domain; display only, not authoritative policy","hard_vetoes":hard,"tighteners":tight,"conflicts":conflicts,"domains":cards,"method":"Rule-based independent lenses. The master gate is authoritative; fresh diagnostics only tighten/veto. Optional missing data never becomes neutral and no average controls capital."},policy


def build_output(registry: dict, feeds: dict[str,dict], metas: dict[str,dict], now: datetime | None=None, fusion: dict | None=None) -> dict:
    now=now or datetime.now(timezone.utc); health,active=audit_sources(registry,feeds,metas,now); policy=base_policy(active.get("risk_gate") or {},active.get("crisis") or {},health); board,policy=build_board(active,health,policy)
    # Fusion is context-only in v1. It can surface a fresh veto/tightener, but cannot override or loosen policy.
    fusion_context={"status":"NOT_SUBSCRIBED","summaries":[]}
    if isinstance(fusion,dict):
        fusion_context={"status":fusion.get("status") or "UNKNOWN","as_of":fusion.get("generated_at"),"summaries":mapping(fusion.get("subscriptions")).get("khalid_risk") or [],"vetoes":fusion.get("vetoes") or []}
        for veto in fusion_context.get("vetoes") or []:
            if isinstance(veto,dict) and veto.get("consumer") in {None,"khalid_risk"} and veto.get("active") is True and policy["mode"] not in {"DATA_HOLD","DEFENSIVE"}:
                policy["mode"]="DEFENSIVE"; policy["allows_new_entries"]=False; policy["exposure_cap_pct"]=min(policy["exposure_cap_pct"],10); policy["sizing_multiplier"]=min(policy["sizing_multiplier"],0.1); policy["reasons"].append("Governed fusion veto: "+text(veto.get("reason"))); board["hard_vetoes"].append("Governed fusion: "+text(veto.get("reason")))
    decision="STAY IN CASH / SHORT-TERM TREASURIES" if policy["mode"] in {"DATA_HOLD","DEFENSIVE"} else "INVEST SELECTIVELY" if policy["allows_new_entries"] else "WAIT IN CASH / SHORT-TERM TREASURIES"
    board.update({"capital_decision":decision,"mode":policy["mode"],"allows_new_entries":policy["allows_new_entries"],"exposure_cap_pct":policy["exposure_cap_pct"]})
    critical_bad=[h for h in health if h["critical"] and h["status"]!="FRESH"]
    status="DATA_HOLD" if policy["mode"]=="DATA_HOLD" else "DEGRADED" if any(h["status"]!="FRESH" for h in health) else "OK"
    counts={name:sum(h["status"]==name for h in health) for name in ("FRESH","STALE","MISSING","INVALID","UNKNOWN")}
    ages=[number(h.get("age_h")) for h in health if number(h.get("age_h")) is not None]
    coverage={
        "total":len(health),
        "fresh":counts["FRESH"],
        "stale":counts["STALE"],
        "missing":counts["MISSING"],
        "invalid":counts["INVALID"],
        "unknown":counts["UNKNOWN"],
        "ratio":round(counts["FRESH"]/len(health),4) if health else 0,
        "status":status,
    }
    freshness={
        "status":"FRESH" if status=="OK" else "DEGRADED" if status=="DEGRADED" else "DATA_HOLD",
        "oldest_age_h":round(max(ages),2) if ages else None,
    }
    settlement_health=next((h for h in health if h.get("name")=="settlement_fails"),{})
    treasury=mapping(mapping(active.get("settlement_fails")).get("treasury"))
    treasury_stats=mapping(treasury.get("stats"))
    treasury_fails={
        "status":settlement_health.get("status") or "MISSING",
        "scope":treasury.get("scope"),
        "scope_note":treasury.get("scope_note"),
        "as_of":treasury.get("as_of") or settlement_health.get("as_of"),
        "unit":treasury.get("unit"),
        "regime":treasury.get("regime"),
        "score":number(treasury.get("score")),
        "ftd_bn":number(treasury.get("ftd_bn")),
        "ftr_bn":number(treasury.get("ftr_bn")),
        "gross_bn":number(treasury.get("gross_bn")),
        "stats":{
            "ftd":mapping(treasury_stats.get("ftd")),
            "ftr":mapping(treasury_stats.get("ftr")),
            "gross":mapping(treasury_stats.get("gross")),
        },
        "completeness":mapping(treasury.get("completeness")),
    }
    reason=next((text(item) for item in policy.get("reasons") or [] if text(item)),None)
    plain_english=(
        f"{decision}. Khalid Risk permits at most {policy['exposure_cap_pct']}% exposure. "
        + (reason or "No additional tightening reason was reported.")
        + " Missing or stale critical evidence never counts as an all-clear."
    )
    payload={"engine":"justhodl-khalid-risk","schema_version":SCHEMA_VERSION,"version":SCHEMA_VERSION,"generated_at":now.astimezone(timezone.utc).isoformat(),"as_of":max((str(h["as_of"]) for h in health if h["status"]=="FRESH" and h.get("as_of")),default=None),"status":status,"policy":policy,"capital_decision":decision,"exposure_cap_pct":policy["exposure_cap_pct"],"risk_score":board["risk_score"],"plain_english":plain_english,"coverage":coverage,"freshness":freshness,"treasury_fails":treasury_fails,"domains":board["domains"],"hard_vetoes":board["hard_vetoes"],"tighteners":board["tighteners"],"conflicts":board["conflicts"],"source_health":health,"missing_inputs":[h for h in health if h["status"]!="FRESH"],"critical_failures":critical_bad,"reasons":policy["reasons"],"risk_board":board,"fusion_context":fusion_context,"methodology":{"authority":"Master risk gate establishes the loosest possible policy; every other rule can only tighten or veto.","aggregation":"No mega-average is authoritative. risk_score is only the maximum fresh observed domain for display.","criticality":registry.get("criticality_policy"),"settlement_fails":"Weekly observation freshness uses treasury.as_of with a 240-hour SLA. Fresh STRESS tightens; fresh CRISIS vetoes."}}
    validate_output(payload); return payload


def validate_output(payload: dict) -> None:
    required={"engine","schema_version","generated_at","as_of","status","policy","capital_decision","exposure_cap_pct","risk_score","plain_english","coverage","freshness","treasury_fails","domains","conflicts","source_health","reasons","methodology"}
    missing=required-set(payload)
    if missing: raise ValueError("missing output keys: "+", ".join(sorted(missing)))
    if payload.get("schema_version")!=SCHEMA_VERSION or payload.get("version")!=SCHEMA_VERSION: raise ValueError("schema/version mismatch")
    if payload.get("status") not in {"OK","DEGRADED","DATA_HOLD"}: raise ValueError("invalid status")
    policy=mapping(payload.get("policy"))
    if policy.get("mode") not in MODES: raise ValueError("invalid policy mode")
    if payload.get("capital_decision") not in DECISIONS or payload.get("capital_decision")!=mapping(payload.get("risk_board")).get("capital_decision"): raise ValueError("capital decision mismatch")
    cap=number(payload.get("exposure_cap_pct"))
    if cap is None or not 0<=cap<=100 or cap!=number(policy.get("exposure_cap_pct")): raise ValueError("exposure cap invalid or inconsistent")
    risk=number(payload.get("risk_score"))
    if payload.get("risk_score") is not None and (risk is None or not 0<=risk<=100): raise ValueError("risk score invalid")
    if not isinstance(payload.get("domains"),list) or not isinstance(payload.get("source_health"),list): raise ValueError("domains/source health must be lists")
    coverage=mapping(payload.get("coverage"))
    ratio=number(coverage.get("ratio"))
    if ratio is None or not 0<=ratio<=1: raise ValueError("coverage ratio invalid")
    treasury=mapping(payload.get("treasury_fails"))
    if treasury.get("status")=="FRESH":
        if treasury.get("scope")!="US_TREASURY_INCLUDING_TIPS" or treasury.get("unit")!="USD_bn_par": raise ValueError("Treasury display scope invalid")
        if any(number(treasury.get(key)) is None for key in ("ftd_bn","ftr_bn","gross_bn")): raise ValueError("Treasury display values invalid")
    json.dumps(payload,allow_nan=False)
