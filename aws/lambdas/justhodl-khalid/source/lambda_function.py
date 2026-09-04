"""Khalid: strict, cross-asset asymmetric-opportunity orchestrator.

Reads existing JustHodl engine artifacts from S3.  It does not replace those
engines or hide their disagreement.  It applies a hard long-term location
gate, corroboration requirements, macro vetoes, and a separate execution gate.

Outputs:
  data/khalid.json
  data/history/khalid.json
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3

from discovery import apply_lifecycle, build_opportunity_radar
from breadth import apply_breadth_confirmation
from scoring import contract_error, number, rank_candidates, score_candidate


BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/khalid.json"
HISTORY_KEY = "data/history/khalid.json"
CANDIDATE_LEDGER_KEY = "data/khalid-candidates.json"
S3 = boto3.client("s3")
MAX_FUTURE_SKEW_HOURS = 5 / 60
KHALID_RISK_MODES = {"DATA_HOLD", "DEFENSIVE", "SELECTIVE", "SELECTIVE_RISK_ON"}
KHALID_RISK_DECISIONS = {
    "STAY IN CASH / SHORT-TERM TREASURIES",
    "INVEST SELECTIVELY",
    "WAIT IN CASH / SHORT-TERM TREASURIES",
}
KHALID_RISK_SOURCE_STATUSES = {"FRESH", "STALE", "MISSING", "INVALID", "UNKNOWN"}
KHALID_RISK_CRITICAL_SOURCES = {
    "risk_gate",
    "crisis",
    "bond_warroom",
    "eurodollar_stress",
    "credit_composite",
}

REGISTRY_PATH = Path(__file__).with_name("input_registry.json")
SCHEMA_PATH = Path(__file__).with_name("output_schema.json")
REGISTRY_ROWS = json.loads(REGISTRY_PATH.read_text()).get("inputs", [])
REGISTRY_BY_ID = {row["id"]: row for row in REGISTRY_ROWS}
FEEDS = {
    row["id"]: (row["artifact"], float(row["max_age_hours"]), bool(row["required"]))
    for row in REGISTRY_ROWS
}


def validate_output(payload: dict) -> None:
    """Small dependency-free contract gate run before every authoritative write."""
    schema = json.loads(SCHEMA_PATH.read_text())
    missing = [key for key in schema["required"] if key not in payload]
    if missing:
        raise ValueError("missing output keys: " + ", ".join(missing))
    if payload["status"] not in schema["statuses"]:
        raise ValueError("invalid status")
    if payload.get("schema_version") != schema["schema_version"] or payload.get("version") != schema["schema_version"]:
        raise ValueError("schema/version mismatch")
    for key in ("score", "risk_score"):
        value = payload.get(key)
        if value is not None and not 0 <= float(value) <= 100:
            raise ValueError(f"{key} outside 0..100")
    if not 0 <= float(payload["confidence"]) <= 1:
        raise ValueError("confidence outside 0..1")
    allowed_actions = set(schema["actions"])
    allowed_stages = set(schema["discovery_stages"])
    radar = payload.get("opportunity_radar")
    if not isinstance(radar, list):
        raise ValueError("opportunity_radar must be a list")
    seen = set()
    for row in radar:
        lifecycle = row.get("lifecycle") or {}
        opportunity_id = lifecycle.get("opportunity_id")
        if not opportunity_id or opportunity_id in seen:
            raise ValueError("opportunity IDs must be present and unique")
        seen.add(opportunity_id)
        if row.get("action") not in allowed_actions:
            raise ValueError(f"invalid action for {opportunity_id}")
        if row.get("discovery_stage") not in allowed_stages:
            raise ValueError(f"invalid discovery stage for {opportunity_id}")
        if not 0 <= float(row.get("score")) <= 100:
            raise ValueError(f"score outside 0..100 for {opportunity_id}")
        if not 0 <= float(row.get("confidence")) <= 1:
            raise ValueError(f"confidence outside 0..1 for {opportunity_id}")
        if not 0 <= float(row.get("component_coverage")) <= 1:
            raise ValueError(f"coverage outside 0..1 for {opportunity_id}")
        stable_fields = {
            "industry", "sector", "category", "market_cap", "cap_bucket",
            "momentum", "criteria", "gates", "dump_risk", "risk_reward",
        }
        missing_row_fields = sorted(stable_fields - set(row))
        if missing_row_fields:
            raise ValueError(f"unstable row schema for {opportunity_id}: {missing_row_fields}")
        dump_risk = row.get("dump_risk")
        if not isinstance(dump_risk, dict):
            raise ValueError(f"dump_risk must be an object for {opportunity_id}")
        empirical = dump_risk.get("empirical_loss_pct")
        estimate = dump_risk.get("structural_estimate")
        if empirical is not None and estimate is not None:
            raise ValueError(f"empirical and structural dump risk cannot coexist for {opportunity_id}")
        if estimate is not None and (
            not isinstance(estimate, dict)
            or estimate.get("calibrated_probability") is not False
            or "NOT A PROBABILITY" not in str(estimate.get("label"))
        ):
            raise ValueError(f"mislabelled structural dump risk for {opportunity_id}")
        entry_state = (row.get("entry_trigger") or {}).get("state")
        if row.get("discovery_stage") == "ENTRY_READY":
            if row.get("action") != "READY_TO_SNIPE" or entry_state != "TRIGGERED":
                raise ValueError(f"ENTRY_READY without observed trigger for {opportunity_id}")
        if row.get("discovery_stage") == "EVIDENCE_HOLD":
            if row.get("action") != "TRACKING" or float(row.get("confidence")) != 0 or entry_state != "WAIT":
                raise ValueError(f"EVIDENCE_HOLD must suspend conviction and execution for {opportunity_id}")
    decision = payload.get("decision") or {}
    if decision.get("opportunities_tracked") != len(radar):
        raise ValueError("tracked count does not reconcile")
    board = payload.get("risk_board")
    if not isinstance(board, dict) or not isinstance(board.get("domains"), list):
        raise ValueError("risk_board must expose domains")
    if decision.get("capital_decision") != board.get("capital_decision"):
        raise ValueError("capital decision does not reconcile with risk board")
    if decision.get("exposure_cap_pct") != board.get("exposure_cap_pct"):
        raise ValueError("exposure cap does not reconcile with risk board")
    risk_artifact = payload.get("risk_artifact") or {}
    if (
        risk_artifact.get("artifact") != "data/khalid-risk.json"
        or risk_artifact.get("authoritative") is not True
        or risk_artifact.get("direct_risk_recomputed") is not False
    ):
        raise ValueError("authoritative Khalid Risk provenance is missing")
    high_count = sum(row["discovery_stage"] in {"ENTRY_READY", "HIGH_CONVICTION"} for row in radar)
    if decision.get("high_conviction_count") != high_count:
        raise ValueError("high-conviction count does not reconcile")
    json.dumps(payload, allow_nan=False)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _read(key: str) -> tuple[dict, dict]:
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        body = json.loads(obj["Body"].read())
        return body if isinstance(body, dict) else {}, {
            "key": key,
            "last_modified": _iso(obj["LastModified"]),
            "bytes": obj.get("ContentLength"),
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 - degradation is part of contract
        return {}, {"key": key, "last_modified": None, "bytes": None, "error": str(exc)[:240]}


def _age_h(iso_value: str | None, now: datetime) -> float | None:
    if not iso_value:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return round((now - dt.astimezone(timezone.utc)).total_seconds() / 3600, 2)
    except (TypeError, ValueError):
        return None


def _feed_timestamp(payload: dict, meta: dict) -> str | None:
    return (
        payload.get("generated_at")
        or payload.get("as_of")
        or meta.get("last_modified")
    )


def _risk_payload_error(name: str, payload: dict) -> str | None:
    """Apply semantic guards that cannot be expressed by the shallow registry contract."""
    if name == "khalid_risk":
        policy = payload.get("policy") if isinstance(payload.get("policy"), dict) else {}
        board = payload.get("risk_board") if isinstance(payload.get("risk_board"), dict) else {}
        status = payload.get("status")
        mode = policy.get("mode")
        allows = policy.get("allows_new_entries")
        decision = payload.get("capital_decision")
        cap = number(payload.get("exposure_cap_pct"))
        sizing = number(policy.get("sizing_multiplier"))
        health = payload.get("source_health")
        if payload.get("schema_version") != "1.0.0":
            return "khalid-risk schema_version must be 1.0.0"
        if status not in {"OK", "DEGRADED", "DATA_HOLD"}:
            return "khalid-risk status is invalid"
        if mode not in KHALID_RISK_MODES:
            return "khalid-risk policy mode is invalid"
        if decision not in KHALID_RISK_DECISIONS:
            return "khalid-risk capital decision is invalid"
        if not isinstance(allows, bool):
            return "khalid-risk allows_new_entries must be boolean"
        if cap is None or not 0 <= cap <= 100:
            return "khalid-risk exposure cap must be finite and within 0..100"
        if sizing is None or not 0 <= sizing <= 1:
            return "khalid-risk sizing multiplier must be finite and within 0..1"
        if cap != number(policy.get("exposure_cap_pct")) or cap != number(board.get("exposure_cap_pct")):
            return "khalid-risk exposure cap fields do not reconcile"
        if payload.get("capital_decision") != board.get("capital_decision"):
            return "khalid-risk capital decision fields do not reconcile"
        if mode != board.get("mode") or allows is not board.get("allows_new_entries"):
            return "khalid-risk policy and board state fields do not reconcile"
        if not isinstance(board.get("domains"), list):
            return "khalid-risk board domains must be a list"
        if not isinstance(health, list):
            return "khalid-risk source_health must be a list"
        for row in health:
            if (
                not isinstance(row, dict)
                or not isinstance(row.get("critical"), bool)
                or row.get("status") not in KHALID_RISK_SOURCE_STATUSES
            ):
                return "khalid-risk source_health row is invalid"
        health_by_name = {
            row.get("name"): row
            for row in health
            if isinstance(row.get("name"), str)
        }
        for source in sorted(KHALID_RISK_CRITICAL_SOURCES):
            row = health_by_name.get(source)
            if row is None:
                return f"khalid-risk critical source_health row is missing: {source}"
            if row.get("critical") is not True:
                return f"khalid-risk required source is not marked critical: {source}"
        nonfresh = [row for row in health if row["status"] != "FRESH"]
        critical_bad = [row for row in nonfresh if row["critical"]]
        if critical_bad and (status != "DATA_HOLD" or mode != "DATA_HOLD"):
            return "khalid-risk critical source failure must force DATA_HOLD"
        if mode == "DATA_HOLD" and not critical_bad:
            return "khalid-risk DATA_HOLD requires a critical source failure"
        if status == "OK" and nonfresh:
            return "khalid-risk OK status contradicts non-fresh source health"
        if status == "DEGRADED" and (not nonfresh or critical_bad):
            return "khalid-risk DEGRADED status contradicts source health"
        if (status == "DATA_HOLD") != (mode == "DATA_HOLD"):
            return "khalid-risk status and policy mode do not reconcile"
        state_contract = {
            "DATA_HOLD": (False, "STAY IN CASH / SHORT-TERM TREASURIES", 0),
            "DEFENSIVE": (False, "STAY IN CASH / SHORT-TERM TREASURIES", 10),
            "SELECTIVE": (True, "INVEST SELECTIVELY", 50),
            "SELECTIVE_RISK_ON": (True, "INVEST SELECTIVELY", 100),
        }
        expected_allows, expected_decision, max_cap = state_contract[mode]
        if allows is not expected_allows or decision != expected_decision or cap > max_cap:
            return "khalid-risk mode, decision, entry permission, and cap contradict"
        if sizing > cap / 100:
            return "khalid-risk sizing multiplier exceeds exposure cap"
        return None
    # Retained adapter guards support direct bond/FX context and backwards
    # compatible tests, but these feeds no longer participate in policy.
    if name in {"credit_composite", "eurodollar_stress"}:
        score = number(
            payload.get("composite")
            if name == "credit_composite" and payload.get("composite") is not None
            else payload.get("composite_score")
        )
        if score is None or not 0 <= score <= 100:
            return f"{name} composite score must be finite and within 0..100"
        return None
    if name == "bond_warroom":
        heartbeat = payload.get("heartbeat") if isinstance(payload.get("heartbeat"), dict) else {}
        equity = payload.get("equity_risk") if isinstance(payload.get("equity_risk"), dict) else {}
        shortage = (
            payload.get("eurodollar_shortage")
            if isinstance(payload.get("eurodollar_shortage"), dict)
            else {}
        )
        missing = []
        heartbeat_score = number(heartbeat.get("score"))
        equity_score = number(equity.get("score"))
        shortage_score = number(shortage.get("score"))
        if heartbeat_score is None or not 0 <= heartbeat_score <= 100 or not str(heartbeat.get("regime") or "").strip():
            missing.append("heartbeat.score/regime")
        if equity_score is None or not 0 <= equity_score <= 100 or not str(
            equity.get("state") or equity.get("level") or ""
        ).strip():
            missing.append("equity_risk.score/state")
        if shortage_score is None or not 0 <= shortage_score <= 100 or not str(shortage.get("state") or "").strip():
            missing.append("eurodollar_shortage.score/state")
        if not payload.get("panels"):
            missing.append("panels")
        return "bond risk fields are missing: " + ", ".join(missing) if missing else None
    if name != "crisis":
        return None
    components = payload.get("components")
    reported = number(payload.get("components_available"))
    defcon = number(payload.get("defcon_level"))
    if defcon is None or not 1 <= defcon <= 5:
        return "crisis defcon_level must be finite and within 1..5"
    if not isinstance(components, list) or not components:
        return "crisis component coverage is missing"
    available = [row for row in components if isinstance(row, dict) and row.get("available") is True]
    if reported is None or reported < max(1, len(components) * 0.75) or len(available) < len(components) * 0.75:
        return "crisis component coverage is below 75%"
    fresh_available = []
    stale = []
    for row in available:
        source = str(row.get("source") or "").lower()
        max_age_h = 192 if source in {"ciss_ea", "ciss"} else 72
        age_h = number(row.get("age_hours"))
        if age_h is None or age_h > max_age_h:
            stale.append((row, max_age_h))
        else:
            fresh_available.append(row)
    if len(fresh_available) < len(components) * 0.75:
        labels = ", ".join(
            f"{row.get('source') or row.get('label') or 'component'}>{max_age_h}h"
            for row, max_age_h in stale[:3]
        )
        return f"crisis fresh component coverage is below 75%: {labels}"
    return None


def _data_hold_policy(reason: str) -> tuple[dict, dict]:
    """Fail-closed fallback when the required risk artifact is unavailable."""
    policy = {
        "mode": "DATA_HOLD",
        "allows_new_entries": False,
        "risk_gate_posture": "UNKNOWN",
        "risk_gate_composite": None,
        "risk_gate_sizing_multiplier": None,
        "sizing_multiplier": 0.0,
        "crisis_defcon": None,
        "exposure_cap_pct": 0,
        "reasons": [reason],
        "default_shelter": {
            "primary": "CASH",
            "why": "Required authoritative Khalid Risk artifact is unavailable",
            "avoid": "New risk until the critical risk artifact clears its contract",
        },
    }
    board = {
        "schema_version": "1.0.0",
        "capital_decision": "STAY IN CASH / SHORT-TERM TREASURIES",
        "mode": "DATA_HOLD",
        "allows_new_entries": False,
        "exposure_cap_pct": 0,
        "risk_score": 100,
        "hard_vetoes": [reason],
        "tighteners": [],
        "conflicts": [],
        "domains": [],
        "method": "Fail closed: required data/khalid-risk.json is not fresh and valid.",
    }
    return policy, board


def _policy_from_risk_artifact(artifact: dict) -> tuple[dict, dict]:
    """Copy the governed policy/board without recomputing direct market risk."""
    if not isinstance(artifact, dict) or not artifact:
        return _data_hold_policy("Required data/khalid-risk.json is missing, invalid, or stale")
    policy = artifact.get("policy") if isinstance(artifact.get("policy"), dict) else {}
    board = artifact.get("risk_board") if isinstance(artifact.get("risk_board"), dict) else {}
    policy = {**policy, "reasons": list(policy.get("reasons") or [])}
    board = {
        **board,
        "domains": list(board.get("domains") or []),
        "hard_vetoes": list(board.get("hard_vetoes") or []),
        "tighteners": list(board.get("tighteners") or []),
        "conflicts": list(board.get("conflicts") or []),
        "authoritative_artifact": "data/khalid-risk.json",
        "authoritative_generated_at": artifact.get("generated_at"),
        "authoritative_capital_decision": artifact.get("capital_decision"),
        "authoritative_exposure_cap_pct": artifact.get("exposure_cap_pct"),
    }
    return policy, board


def _sync_consumer_tightening(policy: dict, board: dict, reasons_before: int) -> None:
    """Reflect any Khalid-only veto without ever loosening the risk artifact."""
    source_cap = number(board.get("authoritative_exposure_cap_pct"))
    current_cap = number(policy.get("exposure_cap_pct"))
    if current_cap is None:
        current_cap = 0
    if source_cap is not None:
        current_cap = min(current_cap, source_cap)
    if policy.get("mode") in {"DATA_HOLD", "DEFENSIVE"}:
        current_cap = min(current_cap, 10 if policy.get("mode") == "DEFENSIVE" else 0)
        decision = "STAY IN CASH / SHORT-TERM TREASURIES"
        policy["allows_new_entries"] = False
    else:
        decision = board.get("authoritative_capital_decision")
    policy["exposure_cap_pct"] = round(current_cap)
    policy["sizing_multiplier"] = min(number(policy.get("sizing_multiplier")) or 0, current_cap / 100)
    board.update({
        "mode": policy.get("mode"),
        "allows_new_entries": bool(policy.get("allows_new_entries")),
        "exposure_cap_pct": round(current_cap),
        "capital_decision": decision,
        "consumer_tighteners": list(policy.get("reasons") or [])[reasons_before:],
    })


def _index(rows: Any, *keys: str) -> dict[str, dict]:
    out = {}
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = None
        for key in keys:
            if row.get(key):
                value = row[key]
                break
        if value:
            out[str(value).upper()] = row
    return out


def _dict_rows(value: Any) -> list[dict]:
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def _dict_map(value: Any) -> dict[str, dict]:
    if not isinstance(value, dict):
        return {}
    return {str(key).upper(): row for key, row in value.items() if isinstance(row, dict)}


def _bond_context(bond: dict) -> dict:
    """Normalize the published bond-warroom shape for legacy summary panels."""
    heartbeat = bond.get("heartbeat") if isinstance(bond.get("heartbeat"), dict) else {}
    equity = bond.get("equity_risk") if isinstance(bond.get("equity_risk"), dict) else {}
    shortage = (
        bond.get("eurodollar_shortage")
        if isinstance(bond.get("eurodollar_shortage"), dict)
        else {}
    )
    return {
        "generated_at": bond.get("generated_at"),
        "summary": (
            equity.get("text")
            or heartbeat.get("headline")
            or shortage.get("text")
        ),
        "regime": (
            equity.get("state")
            or equity.get("level")
            or heartbeat.get("regime")
            or shortage.get("state")
        ),
    }


def _crypto_watch(ma: dict, confluence: dict, cycle: dict) -> list[dict]:
    rows = []
    risk_score = cycle.get("composite_score") or cycle.get("risk_score")
    regime = confluence.get("regime") or confluence.get("verdict") or confluence.get("state")
    for group, label in (
        ("fresh_breakdowns_below", "Below 200-day; no bottom confirmation"),
        ("retest_failed", "Failed 200-day retest"),
        ("fresh_breakouts_above", "Fresh reclaim; wait for a successful retest"),
    ):
        for row in _dict_rows(ma.get(group))[:10]:
            dist = row.get("dist_pct")
            rows.append({
                "ticker": row.get("ticker"),
                "name": row.get("ticker"),
                "asset_class": "CRYPTO",
                "action": "WATCH_RECLAIM" if group == "fresh_breakouts_above" else "REJECTED",
                "score": 35 if group == "fresh_breakouts_above" else 20,
                "confidence": 0.45,
                "timeframe": {
                    "thesis": "3M / 1M / weekly",
                    "entry": "daily confirmation, then 4h execution",
                    "rule": "No crypto entry from a one-day volume spike",
                },
                "price": row.get("price"),
                "technical": {
                    "vs_200d_pct": dist,
                    "vs_250d_pct": None,
                    "rsi": None,
                    "bb_width_percentile": None,
                    "higher_lows": None,
                    "bottom_confirmation": row.get("state"),
                },
                "valuation": {"score": None, "pe": None, "forward_pe": None, "peg": None, "ev_ebitda": None, "fcf_yield_pct": None},
                "dilution": {"yoy_pct": None, "active": None, "net_issuer": None, "net_buyback_yield_pct": None},
                "risk_reward": {"ratio": None, "pivot": row.get("ma200"), "stop": None, "target_1": None, "target_2": None, "empirical_dump_loss_pct": None, "asymmetry": None},
                "entry_trigger": {
                    "state": "WAIT",
                    "daily": "Reclaim and hold the 200-day average",
                    "four_hour": "Higher low after the daily reclaim",
                    "invalidation": "4h close back below the reclaimed 200-day level",
                },
                "evidence": {
                    "location": [{"label": "200-day distance", "value": dist, "source": "crypto-ma200", "detail": label}],
                    "accumulation": [], "flows": [], "catalysts": [], "valuation": [], "risk_reward": [],
                },
                "vetoes": ["No verified multi-week accumulation and flow convergence"],
                "cautions": [f"Crypto regime: {regime or 'unavailable'}", f"Cycle-risk score: {risk_score if risk_score is not None else 'unavailable'}"],
                "plain_english": f"{row.get('ticker')} is monitored near its 200-day trend, but Khalid will not chase or infer accumulation from a volume spike. A durable base, flow confirmation, and a successful retest are still required.",
                "deep_links": ["/crypto-confluence.html", "/crypto-liquidity.html", "/ma200-radar.html"],
            })
    return rows


def _katlin_rows(katlin: dict) -> list[dict]:
    """Translate Katlin's broad scan into Khalid's independently gated row contract."""
    rows = []
    for row in _dict_rows(katlin.get("picks")) + _dict_rows(katlin.get("watch")):
        if not row.get("ticker"):
            continue
        quality = row.get("quality") if isinstance(row.get("quality"), dict) else {}
        inflows = row.get("inflow_legs") if isinstance(row.get("inflow_legs"), dict) else {}
        plan = row.get("plan") if isinstance(row.get("plan"), dict) else {}
        pillars = row.get("pillars") if isinstance(row.get("pillars"), dict) else {}
        accum = row.get("accum_legs") if isinstance(row.get("accum_legs"), dict) else {}
        catalysts = _dict_rows(row.get("catalysts"))
        catalyst_text = " ".join(str(item.get("text") or "") for item in catalysts).lower()
        industry_flow = inflows.get("industry_flow") if isinstance(inflows.get("industry_flow"), dict) else {}
        rows.append({
            "_source": "katlin",
            "ticker": row.get("ticker"),
            "name": row.get("name"),
            "industry": row.get("industry"),
            "sector": row.get("sector"),
            "category": row.get("category"),
            "market_cap": row.get("market_cap") or row.get("market_cap_usd"),
            "cap_bucket": row.get("cap_bucket") or row.get("market_cap_bucket"),
            "momentum": row.get("momentum") if row.get("momentum") is not None else pillars.get("momentum"),
            "criteria": row.get("criteria") if row.get("criteria") is not None else row.get("criteria_met", []),
            "gates": row.get("gates") if row.get("gates") is not None else row.get("gate_results", []),
            "dump_risk_evidence": row.get("dump_risk_evidence") or row.get("dump_evidence") or [],
            "asset_class": str(row.get("asset_class") or "stock").upper(),
            "close": row.get("last"),
            "vs_sma200_pct": row.get("dist_sma200_pct"),
            "vs_sma250_pct": row.get("dist_sma250_pct"),
            "bb_width_pctile": row.get("bbw_pctile"),
            "rsi": row.get("rsi_w"),
            "volume_dryup": row.get("vol_ratio_20_120"),
            "higher_lows": row.get("higher_lows_w"),
            "obv_divergence": (number(accum.get("obv_div")) or 0) >= 60,
            "ad_divergence": (number(accum.get("ad_line")) or 0) >= 60,
            "adv_usd_20d": row.get("adv_usd"),
            "dilution_yoy_pct": quality.get("share_count_yoy_pct"),
            "net_buyback_yield_pct": quality.get("net_buyback_yield_pct"),
            "flow_score": pillars.get("inflows"),
            "inflow_major": bool(industry_flow.get("major")),
            "inst_net_usd": inflows.get("inst_net_usd"),
            "whale_net_usd": inflows.get("whale_net_usd"),
            "dark_pool_state": inflows.get("dark_pool_state"),
            "insider_cluster": bool(inflows.get("insider_cluster")),
            "catalyst_score": pillars.get("catalyst"),
            "has_contract_catalyst": "contract" in catalyst_text,
            "demand_accelerating": "backlog" in catalyst_text or "accelerat" in catalyst_text,
            "industry_boom_score": next(
                (item.get("value") for item in catalysts if item.get("kind") == "industry_boom"),
                None,
            ),
            "fwd_pe": quality.get("fwd_pe"),
            "peg": quality.get("peg"),
            "ev_ebitda": quality.get("ev_ebitda"),
            "fcf_yield_pct": quality.get("fcf_yield_pct"),
            "beneish_m": quality.get("beneish_m"),
            "asymmetry": row.get("asymmetry"),
            "confidence": (
                (number(row.get("conviction")) or 0) / 100.0
                if row.get("conviction") is not None else None
            ),
            "trade_plan": {
                "pivot": plan.get("entry"),
                "stop": plan.get("stop"),
                "target_2": plan.get("target_2") or plan.get("target_1"),
                "target": plan.get("target_1"),
            },
            "_katlin": {
                "tier": row.get("tier"),
                "composite": row.get("composite"),
                "conviction": row.get("conviction"),
                "structure_state": row.get("structure_state"),
                "double_bottom": (
                    row.get("double_bottom_w").get("state")
                    if isinstance(row.get("double_bottom_w"), dict) else None
                ),
                "trend_break": row.get("lt_trend_break"),
                "rsi_d": row.get("rsi_d"),
                "rsi_w": row.get("rsi_w"),
                "rsi_m": row.get("rsi_m"),
                "accumulation": pillars.get("accumulation"),
                "inflows": pillars.get("inflows"),
                "catalyst": pillars.get("catalyst"),
                "momentum": pillars.get("momentum"),
                "sniper": (
                    row.get("sniper").get("state")
                    if isinstance(row.get("sniper"), dict) else None
                ),
                "knife": row.get("knife"),
                "red_flags": quality.get("red_flags"),
                "why": row.get("why"),
                "catalysts": [str(item.get("text") or "") for item in catalysts[:4]],
                "accum_evidence": [
                    item for item in (row.get("accum_evidence") or [])[:4]
                ] if isinstance(row.get("accum_evidence"), list) else [],
                "inflow_evidence": [
                    item for item in (row.get("inflow_evidence") or [])[:4]
                ] if isinstance(row.get("inflow_evidence"), list) else [],
            },
        })
    return rows


def _katlin_posture(katlin: dict) -> dict:
    war_room = katlin.get("war_room") if isinstance(katlin.get("war_room"), dict) else {}
    legs = war_room.get("legs") if isinstance(war_room.get("legs"), list) else []
    vetoes = war_room.get("vetoes") if isinstance(war_room.get("vetoes"), list) else []
    return {
        "posture": war_room.get("posture"),
        "thermometer": war_room.get("thermometer"),
        "exposure_cap_pct": war_room.get("exposure_cap_pct"),
        "vetoes": vetoes,
        "brief": war_room.get("brief"),
        "n_legs": len(legs),
        "session": katlin.get("session"),
    }


def build_output(
    feeds: dict[str, dict],
    metas: dict[str, dict],
    now: datetime | None = None,
    prior_candidates: list[dict] | None = None,
) -> dict:
    now = now or datetime.now(timezone.utc)
    source_health = []
    gaps = []
    for name, (key, max_age, critical) in FEEDS.items():
        payload = feeds.get(name) or {}
        meta = metas.get(name) or {}
        spec = REGISTRY_BY_ID[name]
        ts = _feed_timestamp(payload, meta)
        age = _age_h(ts, now)
        adapter_error = contract_error(spec, payload) or _risk_payload_error(name, payload)
        if meta.get("error"):
            status = "MISSING"
            gaps.append(f"{key}: {meta['error']}")
        elif adapter_error:
            status = "INVALID"
            gaps.append(f"{key}: {adapter_error}")
        elif age is None:
            status = "UNKNOWN"
            gaps.append(f"{key}: no parseable timestamp")
        elif age < -MAX_FUTURE_SKEW_HOURS:
            status = "INVALID"
            gaps.append(f"{key}: timestamp is {abs(age):.1f}h in the future")
        elif age > max_age:
            status = "STALE"
            gaps.append(f"{key}: {age:.1f}h old, SLA {max_age}h")
        else:
            status = "FRESH"
        source_health.append({
            "name": name,
            "key": key,
            "status": status,
            "age_h": age,
            "max_age_h": max_age,
            "critical": critical,
            "as_of": ts,
            "s3_modified_at": meta.get("last_modified"),
            "producer": spec.get("producer"),
            "domain": spec.get("domain"),
            "fields_consumed": spec.get("fields_consumed") or [],
        })

    health_by_id = {row["name"]: row["status"] for row in source_health}
    active_feeds = {
        name: payload if health_by_id.get(name) == "FRESH" else {}
        for name, payload in feeds.items()
    }
    # The standalone Khalid Risk artifact is the only market-risk authority.
    # Direct bond/FX feeds that remain below are context for opportunity
    # features only and are never scored a second time into capital policy.
    risk_artifact = active_feeds.get("khalid_risk") or {}
    policy, risk_board = _policy_from_risk_artifact(risk_artifact)
    risk_reason_count = len(policy.get("reasons") or [])
    katlin = active_feeds.get("katlin") or {}
    katlin_war_room = _katlin_posture(katlin)
    if (
        katlin_war_room.get("posture") in {"CASH_OR_TBILLS", "DEFENSIVE"}
        and policy.get("allows_new_entries")
    ):
        policy["allows_new_entries"] = False
        detail = (
            "; ".join(str(value) for value in katlin_war_room["vetoes"][:3])
            if katlin_war_room["vetoes"] else "Katlin's cross-asset war room is defensive"
        )
        policy.setdefault("reasons", []).append(
            f"Katlin war room is {katlin_war_room['posture']} "
            f"(thermometer {katlin_war_room.get('thermometer')}, "
            f"cap {katlin_war_room.get('exposure_cap_pct')}%): {detail}"
        )
    _sync_consumer_tightening(policy, risk_board, risk_reason_count)
    fortress = active_feeds.get("fortress") or {}
    accumulation = active_feeds.get("accumulation") or {}
    best = active_feeds.get("best_setups") or {}
    floor = active_feeds.get("floor") or {}
    buyback = active_feeds.get("buyback") or {}
    option_feed = active_feeds.get("options") or {}

    bottoms = accumulation.get("bottoms") if isinstance(accumulation.get("bottoms"), dict) else {}
    bottom_rows = (
        _dict_rows(accumulation.get("confirmed_bottoms"))
        + _dict_rows(bottoms.get("stocks"))
        + _dict_rows(bottoms.get("etfs"))
    )
    bottom_map = _index(bottom_rows, "ticker", "symbol")
    best_map = _index(_dict_rows(best.get("top_setups")), "ticker", "symbol")
    floor_map = _dict_map(floor.get("tickers"))
    buyback_map = _dict_map(buyback.get("tickers"))
    option_map = _index(_dict_rows(option_feed.get("all_results")), "ticker", "symbol")

    candidates = []
    for row in _dict_rows(fortress.get("board")):
        ticker = str(row.get("ticker") or "").upper()
        candidates.append(score_candidate(
            row,
            asset_class="STOCK",
            risk_allows_entries=policy["allows_new_entries"],
            bottom_confirmation=bottom_map.get(ticker),
            best_setup=best_map.get(ticker),
            floor=(floor_map or {}).get(ticker),
            buyback=(buyback_map or {}).get(ticker),
            options=option_map.get(ticker),
        ))
    for row in _dict_rows(fortress.get("etfs")):
        ticker = str(row.get("ticker") or "").upper()
        candidates.append(score_candidate(
            row,
            asset_class="ETF",
            risk_allows_entries=policy["allows_new_entries"],
            bottom_confirmation=bottom_map.get(ticker),
            best_setup=best_map.get(ticker),
            options=option_map.get(ticker),
        ))

    # Katlin expands discovery; every translated row still passes Khalid's
    # independent scoring, macro veto, and observed-trigger execution rules.
    seen = {row["ticker"]: index for index, row in enumerate(candidates) if row.get("ticker")}
    fortress_rows = {
        str(row.get("ticker") or "").upper(): row
        for row in _dict_rows(fortress.get("board")) + _dict_rows(fortress.get("etfs"))
    }
    for katlin_row in _katlin_rows(katlin):
        ticker = str(katlin_row["ticker"]).upper()
        asset_class = katlin_row["asset_class"]
        if ticker in seen:
            merged = {
                **katlin_row,
                **{
                    key: value
                    for key, value in fortress_rows.get(ticker, {}).items()
                    if value is not None
                },
                "_source": "fortress+katlin",
                "_katlin": katlin_row["_katlin"],
            }
            candidates[seen[ticker]] = score_candidate(
                merged,
                asset_class=asset_class,
                risk_allows_entries=policy["allows_new_entries"],
                bottom_confirmation=bottom_map.get(ticker),
                best_setup=best_map.get(ticker),
                floor=floor_map.get(ticker),
                buyback=buyback_map.get(ticker),
                options=option_map.get(ticker),
            )
        else:
            seen[ticker] = len(candidates)
            candidates.append(score_candidate(
                katlin_row,
                asset_class=asset_class,
                risk_allows_entries=policy["allows_new_entries"],
                bottom_confirmation=bottom_map.get(ticker),
                best_setup=best_map.get(ticker),
                floor=floor_map.get(ticker),
                buyback=buyback_map.get(ticker),
                options=option_map.get(ticker),
            ))

    ranked = rank_candidates(candidates)
    selected = [x for x in ranked if x["action"] == "READY_TO_SNIPE"][:12]
    building = [x for x in ranked if x["action"] in {"ARMED", "BUILDING_BASE"}][:20]
    watch = [x for x in ranked if x["action"] == "WATCH_RECLAIM"][:12]
    rejected_examples = [x for x in ranked if x["action"] == "REJECTED"][:12]
    crypto_watch = [row for row in ranked if row["asset_class"] == "CRYPTO"][:15] + _crypto_watch(
        active_feeds.get("crypto_ma200") or {},
        active_feeds.get("crypto_confluence") or {},
        active_feeds.get("crypto_cycle_risk") or {},
    )

    allocator = active_feeds.get("allocator") or {}
    bond = active_feeds.get("bond_warroom") or {}
    bond_context = _bond_context(bond)
    total = len(candidates)
    n_katlin = sum("katlin" in str(row.get("source") or "") for row in candidates)
    required_bad = [x for x in source_health if x["critical"] and x["status"] != "FRESH"]
    fresh_count = sum(1 for x in source_health if x["status"] == "FRESH")
    top_eligible = selected or building or watch
    opportunity_radar = build_opportunity_radar(active_feeds, ranked, policy["mode"])
    opportunity_radar, breadth_clusters = apply_breadth_confirmation(opportunity_radar)
    opportunity_radar, candidate_ledger, opportunity_changes = apply_lifecycle(
        opportunity_radar,
        prior_candidates or [],
        _iso(now),
        {
            row["name"]
            for row in source_health
            if row["status"] != "FRESH"
        },
    )
    biggest_opportunities = sorted([
        row for row in opportunity_radar
        if row["discovery_stage"] in {"ENTRY_READY", "HIGH_CONVICTION", "UNDERAPPRECIATED", "RISK_BLOCKED"}
    ], key=lambda row: (-row["score"], -row["source_count"], row["asset_class"], row["ticker"]))[:15]
    queue_counts = {
        "discovery": len(opportunity_radar),
        "conviction": sum(row["discovery_stage"] in {"HIGH_CONVICTION", "ENTRY_READY", "RISK_BLOCKED"} for row in opportunity_radar),
        "entry": sum((row.get("entry_trigger") or {}).get("state") in {"ARMED", "TRIGGERED"} for row in opportunity_radar),
    }
    universe_by_asset_class = {
        asset_class: sum(row["asset_class"] == asset_class for row in opportunity_radar)
        for asset_class in sorted({row["asset_class"] for row in opportunity_radar})
    }
    stance_score = round(
        (biggest_opportunities[0]["score"] if biggest_opportunities else top_eligible[0]["score"]),
        1,
    ) if (biggest_opportunities or top_eligible) else None
    risk_score = {
        "DATA_HOLD": 100,
        "DEFENSIVE": 85,
        "SELECTIVE": 50,
        "SELECTIVE_RISK_ON": 25,
    }.get(policy["mode"], 75)
    if risk_board.get("risk_score") is not None:
        risk_score = max(risk_score, float(risk_board["risk_score"]))
    confidence_pool = top_eligible[:5] if top_eligible else biggest_opportunities[:5]
    confidence = round(
        (fresh_count / max(1, len(source_health))) * 0.45
        + ((sum(x.get("confidence", 0) for x in confidence_pool) / len(confidence_pool)) if confidence_pool else 0) * 0.55,
        3,
    )
    status = "NO_DATA" if total == 0 and not opportunity_radar else ("DEGRADED" if required_bad else "OK")
    stance = (
        "CASH_OR_TBILLS" if policy["mode"] in {"DATA_HOLD", "DEFENSIVE"}
        else "SELECTIVE_BUY" if selected
        else "WAIT_FOR_CONFIRMATION"
    )
    # Do not recompute capital permission from the candidate set. The
    # authoritative risk artifact (or a stricter Khalid-only veto) controls it.
    capital_decision = risk_board["capital_decision"]
    asset_views = [
        {"asset_class": "Stocks", "stance": "SELECTIVE" if selected else "TRACKING", "ready": sum(x["asset_class"] == "STOCK" for x in selected), "building": sum(x["asset_class"] == "STOCK" for x in opportunity_radar), "reason": "Value, inflection, catalysts and capital confirmation are discovered broadly; entry remains strict."},
        {"asset_class": "ETFs / Sectors", "stance": "TRACKING", "ready": sum(x["asset_class"] == "ETF" and x["action"] == "READY_TO_SNIPE" for x in opportunity_radar), "building": sum(x["asset_class"] == "ETF" for x in opportunity_radar), "reason": "Early sector emergence and cross-asset value are ranked before they become crowded."},
        {"asset_class": "Crypto", "stance": "TRACKING", "ready": sum(x["asset_class"] == "CRYPTO" and x["action"] == "READY_TO_SNIPE" for x in opportunity_radar), "building": sum(x["asset_class"] == "CRYPTO" for x in opportunity_radar), "reason": "Crypto can enter discovery, but needs durable structure and a retest before execution."},
        {"asset_class": "Bonds", "stance": bond_context["regime"] or "MONITOR", "ready": sum(x["asset_class"] == "BOND" and x["action"] == "READY_TO_SNIPE" for x in opportunity_radar), "building": sum(x["asset_class"] == "BOND" for x in opportunity_radar), "reason": bond_context["summary"] or "Rates, credit, carry and asymmetry are ranked alongside their role as risk controls."},
        {"asset_class": "Commodities / Countries", "stance": "TRACKING", "ready": sum(x["asset_class"] in {"COMMODITY", "COUNTRY"} and x["action"] == "READY_TO_SNIPE" for x in opportunity_radar), "building": sum(x["asset_class"] in {"COMMODITY", "COUNTRY"} for x in opportunity_radar), "reason": "Physical demand, relative strength, expected return and extension risk are monitored across markets."},
        {"asset_class": "Cash / T-bills", "stance": "PREFERRED" if not selected or policy["mode"] in {"DATA_HOLD", "DEFENSIVE"} else "RESERVE", "ready": 0, "building": 0, "reason": policy["default_shelter"]["why"]},
    ]
    domain_rows = [
        {"domain": "Risk regime", "score": 100 - risk_score, "direction": policy["mode"], "confidence": confidence, "status": "DEGRADED" if required_bad else "OK", "contributors": ["khalid_risk"]},
        {"domain": "Long-term location", "score": round(sum(x["score"] for x in top_eligible[:10]) / len(top_eligible[:10]), 1) if top_eligible else None, "direction": "BELOW_TREND_REQUIRED", "confidence": confidence, "status": "OK" if total else "NO_DATA", "contributors": ["fortress"]},
        {"domain": "Accumulation", "score": round(sum(min(100, len(x["evidence"]["accumulation"]) * 20) for x in top_eligible[:10]) / len(top_eligible[:10]), 1) if top_eligible else None, "direction": "CONFIRMING" if building or selected else "WEAK", "confidence": confidence, "status": "OK" if active_feeds.get("accumulation") else "DEGRADED", "contributors": ["fortress", "accumulation"]},
        {"domain": "Capital flows", "score": round(sum(min(100, len(x["evidence"]["flows"]) * 25) for x in top_eligible[:10]) / len(top_eligible[:10]), 1) if top_eligible else None, "direction": "SELECTIVE", "confidence": confidence, "status": "OK", "contributors": ["fortress", "options"]},
        {"domain": "Catalysts", "score": round(sum(min(100, len(x["evidence"]["catalysts"]) * 25) for x in top_eligible[:10]) / len(top_eligible[:10]), 1) if top_eligible else None, "direction": "SELECTIVE", "confidence": confidence, "status": "OK", "contributors": ["fortress", "buyback"]},
        {"domain": "Crypto cycle", "score": None, "direction": "WATCH_ONLY", "confidence": confidence, "status": "OK" if active_feeds.get("crypto_ma200") else "DEGRADED", "contributors": ["crypto_ma200", "crypto_confluence", "crypto_cycle_risk"]},
        {"domain": "Opportunity discovery", "score": round(sum(x["score"] for x in biggest_opportunities) / len(biggest_opportunities), 1) if biggest_opportunities else None, "direction": "BROAD_CROSS_ASSET", "confidence": confidence, "status": "OK" if opportunity_radar else "NO_DATA", "contributors": ["fortress", "universe_discovery", "asset_compass", "sector_emergence", "commodity_curves", "deal_scanner", "buyback_ranking", "insider_radar", "spinoff_desk", "industry_rotation", "capital_flow", "cftc_positioning", "crypto_emergence", "crypto_opportunities", "metals_miners", "inventory_drawdown", "global_sovereign", "fx_intelligence", "portwatch"]},
    ]
    contradictions = []
    for row in top_eligible[:12]:
        for note in row.get("cautions", [])[:2]:
            contradictions.append({"ticker": row["ticker"], "issue": note, "effect": "REDUCES_CONFIDENCE"})
    catalyst_rows = []
    for row in top_eligible[:12]:
        for item in row["evidence"]["catalysts"][:3]:
            catalyst_rows.append({"ticker": row["ticker"], **item})
    risks = [{"scope": "SYSTEM", "risk": reason, "severity": policy["mode"]} for reason in policy["reasons"]]
    risks.extend({"scope": "DATA", "risk": gap, "severity": "DEGRADED"} for gap in gaps[:12])
    risks.extend(
        {"scope": row["ticker"], "risk": veto, "severity": "VETO"}
        for row in rejected_examples[:8] for veto in row.get("vetoes", [])[:1]
    )
    output = {
        "schema_version": "3.0.0",
        "engine": "justhodl-khalid",
        "version": "3.0.0",
        "generated_at": _iso(now),
        "as_of": _iso(now),
        "status": status,
        "score": stance_score,
        "stance": stance,
        "risk_score": risk_score,
        "confidence": confidence,
        "coverage": {
            "required": sum(1 for x in source_health if x["critical"]),
            "fresh": fresh_count,
            "stale": sum(1 for x in source_health if x["status"] == "STALE"),
            "missing": sum(1 for x in source_health if x["status"] in {"MISSING", "UNKNOWN"}),
            "ratio": round(fresh_count / max(1, len(source_health)), 3),
            "evaluated": len(opportunity_radar),
            "tracked": len(opportunity_radar),
            "universe_by_asset_class": universe_by_asset_class,
            "by_domain": {
                domain: {
                    "fresh": sum(1 for x in source_health if x["domain"] == domain and x["status"] == "FRESH"),
                    "total": sum(1 for x in source_health if x["domain"] == domain),
                }
                for domain in sorted({x["domain"] for x in source_health})
            },
        },
        "domains": domain_rows,
        "asset_views": asset_views,
        "top_signals": biggest_opportunities or top_eligible[:12],
        "contradictions": contradictions[:20],
        "catalysts": catalyst_rows[:24],
        "risks": risks[:30],
        "inputs": source_health,
        "missing_inputs": [x for x in source_health if x["status"] != "FRESH"],
        "decision": {
            "posture": policy["mode"],
            "headline": (
                f"{len(selected)} setup{'s' if len(selected) != 1 else ''} cleared every gate"
                if selected else f"No entry trigger yet; tracking {len(opportunity_radar)} opportunities"
            ),
            "plain_english": (
                "Khalid found long-term value, accumulation, flow, catalyst, and reward/risk alignment. "
                "The names below are armed for confirmation, not market orders."
                if selected else
                f"Khalid is monitoring {len(opportunity_radar)} cross-asset opportunities across discovery and conviction. "
                "None has an observed daily/4h entry trigger, so cash or short-term Treasury bills remain the default."
            ),
            "selected_count": len(selected),
            "building_count": len(building),
            "universe_scored": total,
            "universe_from_katlin": n_katlin,
            "opportunities_tracked": len(opportunity_radar),
            "high_conviction_count": sum(x["discovery_stage"] in {"ENTRY_READY", "HIGH_CONVICTION"} for x in opportunity_radar),
            "capital_decision": capital_decision,
            "exposure_cap_pct": risk_board["exposure_cap_pct"],
            "shelter": policy["default_shelter"],
        },
        "risk_board": risk_board,
        "risk_artifact": {
            "artifact": "data/khalid-risk.json",
            "generated_at": risk_artifact.get("generated_at"),
            "status": risk_artifact.get("status"),
            "authoritative": True,
            "direct_risk_recomputed": False,
        },
        "breadth_clusters": breadth_clusters,
        "risk_control": {
            **policy,
            "katlin_war_room": katlin_war_room,
            "bond_market": {
                **bond_context,
                "note": "Bond-market evidence controls sizing and vetoes; it does not create an equity buy.",
            },
            "allocator": {
                "regime": allocator.get("regime_headline"),
                "cash_buffer_pct": allocator.get("cash_buffer_pct"),
                "recommended_weights_pct": allocator.get("recommended_weights_pct"),
            },
            "engine_fusion": {
                "artifact": "data/engine-fusion.json",
                "status": (active_feeds.get("engine_fusion") or {}).get("status"),
                "summaries": (
                    ((active_feeds.get("engine_fusion") or {}).get("subscriptions") or {}).get("khalid")
                    or []
                ),
                "role": "context/tighten-only; never overrides Khalid Risk vetoes",
            },
        },
        "selected": selected,
        "building_bases": building,
        "watch_reclaims": watch,
        "crypto_watch": crypto_watch[:15],
        "biggest_opportunities": biggest_opportunities,
        "opportunity_radar": opportunity_radar,
        "queues": {
            "discovery": {
                "eligible_count": queue_counts["discovery"],
                "returned_count": queue_counts["discovery"],
                "next_cursor": None,
            },
            "conviction": {
                "eligible_count": queue_counts["conviction"],
                "returned_count": queue_counts["conviction"],
                "next_cursor": None,
            },
            "entry": {
                "eligible_count": queue_counts["entry"],
                "returned_count": queue_counts["entry"],
                "next_cursor": None,
            },
        },
        "near_misses": sorted([
            row for row in opportunity_radar
            if row["discovery_stage"] in {"EARLY_SIGNAL", "UNDERAPPRECIATED"}
        ], key=lambda row: (-row["score"], row["ticker"]))[:25],
        "opportunity_changes": opportunity_changes,
        "rejected_examples": rejected_examples,
        "methodology": {
            "mandate": "Continuously find and track the market's largest underappreciated, asymmetric opportunities across asset classes, then wait for a defined-risk entry.",
            "long_horizon": "3M, monthly and weekly context establish the thesis. Discovery spans value, early trend, capital rotation, physical demand and unpriced catalysts.",
            "entry_horizon": "Daily and 4h are execution-only. A breakout, retest and higher low may arm an entry; they cannot rescue a weak thesis.",
            "hard_gates": [
                "Discovery and execution are separate: an idea may be tracked early, but only the execution lane can label it READY_TO_SNIPE",
                "For deep-base entries, price must be at or below the 200-day trend; below the 250-day trend is preferred",
                "Emerging-trend ideas may be tracked after a reclaim only when they are not extended; they are not automatically entry-ready",
                "A compressed base, supply dry-up and independent price/volume structure must agree",
                "RSI must be reset at 45 or below before a setup can be armed",
                "At least one capital-flow clue and one identifiable catalyst clue",
                "Minimum 2.0x reward/risk; 2.5x preferred for READY_TO_SNIPE",
                "Stock dilution coverage must be present and no severe dilution may be active",
                "No illiquidity, stale critical data, or macro risk veto",
            ],
            "score_weights_pct": {
                "location": 22,
                "accumulation": 22,
                "flows": 18,
                "catalysts": 16,
                "valuation": 10,
                "risk_reward_and_safety": 12,
            },
            "evidence_rules": [
                "Missing values remain null and reduce eligibility; they are never converted to zero evidence.",
                "Below the 200-day average is a discovery constraint, not evidence of value; deep distance receives a falling-knife penalty.",
                "A lower Bollinger-band touch and a squeeze are directionless until price confirms the reversal.",
                "Declining volume is a supply dry-up precondition, not accumulation proof by itself.",
                "Options blocks and dark-pool volume are context only because trade direction is not independently observable.",
                "ETF flow is separated from corporate issuance; buybacks and dilution are evaluated independently.",
                "A recent volume surge or price breakout is not accumulation and is rejected if the asset is extended.",
                "Reported institutional positions are delayed context and never a fast entry trigger.",
                "A candidate is stronger when independent engines agree; source count is explicit and one-source ideas receive a confidence haircut.",
                "The radar ranks opportunity, not guaranteed return. Every tracked thesis retains a falsifier or risk note.",
                "Every qualifying candidate remains reachable in the full radar; queues publish reconciled counts and never hide rows behind a fixed client slice.",
            ],
        },
        "source_health": source_health,
        "gaps": gaps,
        "health": {
            "ok_blocks": sum(1 for x in source_health if x["status"] == "FRESH"),
            "errors": gaps,
            "stale_feeds": [x["key"] for x in source_health if x["status"] == "STALE"],
        },
        "signal": {
            "engine_id": "justhodl-khalid",
            "domain": "cross_asset_opportunity",
            "as_of": _iso(now),
            "score": round((((selected[0]["score"] if selected else biggest_opportunities[0]["score"]) if biggest_opportunities or selected else 0) / 50.0) - 1.0, 3),
            "confidence": round(
                sum(x.get("confidence", 0) for x in (selected or biggest_opportunities[:5]))
                / len(selected or biggest_opportunities[:5]), 3
            ) if (selected or biggest_opportunities) else 0.0,
            "regime_context": [policy["mode"]],
            "lead_lag_days": 0,
            "state": "green" if selected and policy["allows_new_entries"] else ("amber" if opportunity_radar else "red"),
            "percentile_10y": None,
            "percentile_2008on": None,
            "contributes_to": ["opportunity_board", "risk_master"],
            "why": (
                f"{len(selected)} candidates passed every required gate"
                if selected else f"{len(opportunity_radar)} opportunities are tracked; none has a verified entry trigger"
            ),
        },
    }
    output["panels"] = {
        "overview": [{
            "stance": stance,
            "score": stance_score,
            "risk_score": risk_score,
            "confidence": confidence,
            "ready": len(selected),
            "building": len(building),
            "universe": total,
            "tracked": len(opportunity_radar),
            "high_conviction": sum(x["discovery_stage"] in {"ENTRY_READY", "HIGH_CONVICTION"} for x in opportunity_radar),
            "shelter": policy["default_shelter"]["primary"],
            "status": status,
        }],
        "domains": domain_rows,
        "assets": asset_views,
        "opportunities": [
            {
                "ticker": row["ticker"],
                "asset_class": row["asset_class"],
                "action": row["action"],
                "score": row["score"],
                "vs_200d_pct": row["technical"]["vs_200d_pct"],
                "rsi": row["technical"]["rsi"],
                "reward_risk": row["risk_reward"]["ratio"],
                "why": row["plain_english"],
            }
            for row in biggest_opportunities
        ],
        "risks": risks[:20],
        "catalysts": catalyst_rows[:20],
        "data_quality": source_health,
    }
    output["_candidate_ledger"] = candidate_ledger
    validate_output(output)
    return output


def _write(key: str, payload: dict) -> None:
    S3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload, separators=(",", ":"), allow_nan=False).encode(),
        ContentType="application/json",
        CacheControl="public,max-age=60",
    )


def lambda_handler(event, context):
    validation_only = isinstance(event, dict) and event.get("mode") == "validate_only"
    now = datetime.now(timezone.utc)
    feeds, metas = {}, {}
    for name, (key, _max_age, _critical) in FEEDS.items():
        feeds[name], metas[name] = _read(key)
    prior_ledger, _ = _read(CANDIDATE_LEDGER_KEY)
    output = build_output(feeds, metas, now, prior_ledger.get("candidates") or [])
    candidate_ledger = output.pop("_candidate_ledger")
    validate_output(output)
    if validation_only:
        encoded_size = len(json.dumps(output, separators=(",", ":"), allow_nan=False).encode())
        return {
            "statusCode": 200,
            "body": json.dumps({
                "ok": True,
                "validation_only": True,
                "schema_version": output["schema_version"],
                "status": output["status"],
                "opportunities_tracked": output["decision"]["opportunities_tracked"],
                "artifact_size_bytes": encoded_size,
            }),
        }
    _write(OUT_KEY, output)
    _write(CANDIDATE_LEDGER_KEY, candidate_ledger)

    history, _ = _read(HISTORY_KEY)
    points = history.get("points") if isinstance(history.get("points"), list) else []
    point = {
        "generated_at": output["generated_at"],
        "posture": output["decision"]["posture"],
        "selected_count": output["decision"]["selected_count"],
        "building_count": output["decision"]["building_count"],
        "risk_gate_posture": output["risk_control"]["risk_gate_posture"],
        "tracked_count": output["decision"]["opportunities_tracked"],
        "high_conviction_count": output["decision"]["high_conviction_count"],
        "top_ticker": output["biggest_opportunities"][0]["ticker"] if output["biggest_opportunities"] else None,
        "top_score": output["biggest_opportunities"][0]["score"] if output["biggest_opportunities"] else None,
    }
    if not points or points[-1].get("generated_at") != point["generated_at"]:
        points.append(point)
    _write(HISTORY_KEY, {
        "engine": "justhodl-khalid-history",
        "generated_at": output["generated_at"],
        "points": points[-720:],
    })
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "output": OUT_KEY,
            "selected": output["decision"]["selected_count"],
            "status": output["status"],
        }),
    }
