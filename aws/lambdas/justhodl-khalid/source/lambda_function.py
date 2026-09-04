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

from scoring import contract_error, rank_candidates, risk_policy, score_candidate


BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/khalid.json"
HISTORY_KEY = "data/history/khalid.json"
S3 = boto3.client("s3")

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
    for key in ("score", "risk_score"):
        value = payload.get(key)
        if value is not None and not 0 <= float(value) <= 100:
            raise ValueError(f"{key} outside 0..100")
    if not 0 <= float(payload["confidence"]) <= 1:
        raise ValueError("confidence outside 0..1")
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
        return round(max(0.0, (now - dt.astimezone(timezone.utc)).total_seconds() / 3600), 2)
    except (TypeError, ValueError):
        return None


def _feed_timestamp(payload: dict, meta: dict) -> str | None:
    return (
        payload.get("generated_at")
        or payload.get("as_of")
        or meta.get("last_modified")
    )


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


def _crypto_watch(ma: dict, confluence: dict, cycle: dict) -> list[dict]:
    rows = []
    risk_score = cycle.get("composite_score") or cycle.get("risk_score")
    regime = confluence.get("regime") or confluence.get("verdict") or confluence.get("state")
    for group, label in (
        ("fresh_breakdowns_below", "Below 200-day; no bottom confirmation"),
        ("retest_failed", "Failed 200-day retest"),
        ("fresh_breakouts_above", "Fresh reclaim; wait for a successful retest"),
    ):
        for row in (ma.get(group) or [])[:10]:
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



# ---- KATLIN bridge (2026-09-04): Khalid gates; Katlin hunts. Katlin (justhodl-katlin, data/katlin.json) scans every
# stock/ETF/crypto in the warehouse for the 200/250-day location, W/M/Q bottom structure, oversold RSI, Wyckoff
# accumulation, inflows, named catalysts and a 4h entry. Its picks are translated into the row vocabulary that
# score_candidate already reads, so Khalid's strict execution gate runs over Fortress AND Katlin without a second
# scoring rulebook. Fortress rows win on overlapping fields; Katlin fills what Fortress does not measure.
def _katlin_rows(katlin: dict) -> list[dict]:
    rows = []
    for r in list(katlin.get("picks") or []) + list(katlin.get("watch") or []):
        if not isinstance(r, dict) or not r.get("ticker"):
            continue
        q = r.get("quality") if isinstance(r.get("quality"), dict) else {}
        il = r.get("inflow_legs") if isinstance(r.get("inflow_legs"), dict) else {}
        pl = r.get("plan") if isinstance(r.get("plan"), dict) else {}
        pillars = r.get("pillars") if isinstance(r.get("pillars"), dict) else {}
        legs = r.get("accum_legs") if isinstance(r.get("accum_legs"), dict) else {}
        cats = [str(c.get("text") if isinstance(c, dict) else c) for c in (r.get("catalysts") or [])]
        cat_text = " ".join(cats).lower()
        indflow = il.get("industry_flow") if isinstance(il.get("industry_flow"), dict) else {}
        rows.append({
            "_source": "katlin",
            "ticker": r.get("ticker"),
            "name": r.get("name"),
            "industry": r.get("industry"),
            "asset_class": str(r.get("asset_class") or "stock").upper(),
            "close": r.get("last"),
            "vs_sma200_pct": r.get("dist_sma200_pct"),
            "vs_sma250_pct": r.get("dist_sma250_pct"),
            "bb_width_pctile": r.get("bbw_pctile"),
            "rsi": r.get("rsi_w"),
            "volume_dryup": r.get("vol_ratio_20_120"),
            "higher_lows": r.get("higher_lows_w"),
            "obv_divergence": (legs.get("obv_div") or 0) >= 60,
            "ad_divergence": (legs.get("ad_line") or 0) >= 60,
            "adv_usd_20d": r.get("adv_usd"),
            "dilution_yoy_pct": q.get("share_count_yoy_pct"),
            "net_buyback_yield_pct": q.get("net_buyback_yield_pct"),
            "flow_score": pillars.get("inflows"),
            "inflow_major": bool(indflow.get("major")),
            "inst_net_usd": il.get("inst_net_usd"),
            "whale_net_usd": il.get("whale_net_usd"),
            "dark_pool_state": il.get("dark_pool_state"),
            "insider_cluster": bool(il.get("insider_cluster")),
            "catalyst_score": pillars.get("catalyst"),
            "has_contract_catalyst": "contract" in cat_text,
            "demand_accelerating": "backlog" in cat_text or "accelerat" in cat_text,
            "industry_boom_score": next((c.get("value") for c in (r.get("catalysts") or []) if isinstance(c, dict) and c.get("kind") == "industry_boom"), None),
            "fwd_pe": q.get("fwd_pe"), "peg": q.get("peg"), "ev_ebitda": q.get("ev_ebitda"), "fcf_yield_pct": q.get("fcf_yield_pct"),
            "beneish_m": q.get("beneish_m"),
            "asymmetry": r.get("asymmetry"),
            "confidence": (r.get("conviction") or 0) / 100.0 if r.get("conviction") is not None else None,
            "trade_plan": {"pivot": pl.get("entry"), "stop": pl.get("stop"), "target_2": pl.get("target_2") or pl.get("target_1"), "target": pl.get("target_1")},
            "_katlin": {
                "tier": r.get("tier"), "composite": r.get("composite"), "conviction": r.get("conviction"),
                "structure_state": r.get("structure_state"), "double_bottom": (r.get("double_bottom_w") or {}).get("state") if isinstance(r.get("double_bottom_w"), dict) else None,
                "trend_break": r.get("lt_trend_break"), "rsi_d": r.get("rsi_d"), "rsi_w": r.get("rsi_w"), "rsi_m": r.get("rsi_m"),
                "accumulation": pillars.get("accumulation"), "inflows": pillars.get("inflows"), "catalyst": pillars.get("catalyst"), "momentum": pillars.get("momentum"),
                "sniper": (r.get("sniper") or {}).get("state") if isinstance(r.get("sniper"), dict) else None,
                "knife": r.get("knife"), "red_flags": q.get("red_flags"), "why": r.get("why"), "catalysts": cats[:4],
                "accum_evidence": (r.get("accum_evidence") or [])[:4], "inflow_evidence": (r.get("inflow_evidence") or [])[:4],
            },
        })
    return rows


def _katlin_posture(katlin: dict) -> dict:
    wr = katlin.get("war_room") if isinstance(katlin.get("war_room"), dict) else {}
    return {"posture": wr.get("posture"), "thermometer": wr.get("thermometer"), "exposure_cap_pct": wr.get("exposure_cap_pct"),
            "vetoes": wr.get("vetoes") or [], "brief": wr.get("brief"), "n_legs": len(wr.get("legs") or []), "session": katlin.get("session")}


def build_output(feeds: dict[str, dict], metas: dict[str, dict], now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    source_health = []
    gaps = []
    for name, (key, max_age, critical) in FEEDS.items():
        payload = feeds.get(name) or {}
        meta = metas.get(name) or {}
        spec = REGISTRY_BY_ID[name]
        ts = _feed_timestamp(payload, meta)
        age = _age_h(ts, now)
        adapter_error = contract_error(spec, payload)
        if meta.get("error"):
            status = "MISSING"
            gaps.append(f"{key}: {meta['error']}")
        elif adapter_error:
            status = "INVALID"
            gaps.append(f"{key}: {adapter_error}")
        elif age is None:
            status = "UNKNOWN"
            gaps.append(f"{key}: no parseable timestamp")
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

    policy = risk_policy(feeds.get("risk_gate") or {}, feeds.get("crisis") or {}, source_health)
    katlin = feeds.get("katlin") or {}
    katlin_wr = _katlin_posture(katlin)
    if katlin_wr.get("posture") in {"CASH_OR_TBILLS", "DEFENSIVE"} and policy.get("allows_new_entries"):
        policy["allows_new_entries"] = False
        policy.setdefault("reasons", []).append(
            f"Katlin war room is {katlin_wr['posture']} (thermometer {katlin_wr.get('thermometer')}, cap {katlin_wr.get('exposure_cap_pct')}%): "
            + "; ".join(str(v) for v in (katlin_wr.get("vetoes") or [])[:3]) if katlin_wr.get("vetoes") else
            f"Katlin war room is {katlin_wr['posture']} (thermometer {katlin_wr.get('thermometer')}, cap {katlin_wr.get('exposure_cap_pct')}%)")
    fortress = feeds.get("fortress") or {}
    accumulation = feeds.get("accumulation") or {}
    best = feeds.get("best_setups") or {}
    floor = feeds.get("floor") or {}
    buyback = feeds.get("buyback") or {}
    option_feed = feeds.get("options") or {}

    bottom_rows = (
        (accumulation.get("confirmed_bottoms") or [])
        + ((accumulation.get("bottoms") or {}).get("stocks") or [])
        + ((accumulation.get("bottoms") or {}).get("etfs") or [])
    )
    bottom_map = _index(bottom_rows, "ticker", "symbol")
    best_map = _index(best.get("top_setups") or [], "ticker", "symbol")
    floor_map = floor.get("tickers") if isinstance(floor.get("tickers"), dict) else {}
    buyback_map = buyback.get("tickers") if isinstance(buyback.get("tickers"), dict) else {}
    option_map = _index(option_feed.get("all_results") or [], "ticker", "symbol")

    candidates = []
    for row in (fortress.get("board") or []):
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
    for row in (fortress.get("etfs") or []):
        ticker = str(row.get("ticker") or "").upper()
        candidates.append(score_candidate(
            row,
            asset_class="ETF",
            risk_allows_entries=policy["allows_new_entries"],
            bottom_confirmation=bottom_map.get(ticker),
            best_setup=best_map.get(ticker),
            options=option_map.get(ticker),
        ))

    # union with Katlin's market-wide scan: same ticker -> Fortress fields win, Katlin fills the gaps; new tickers -> Katlin rows
    seen = {c["ticker"]: i for i, c in enumerate(candidates) if c.get("ticker")}
    fortress_rows = {str(r.get("ticker") or "").upper(): r for r in list(fortress.get("board") or []) + list(fortress.get("etfs") or []) if isinstance(r, dict)}
    for krow in _katlin_rows(katlin):
        t = str(krow["ticker"]).upper()
        cls = krow["asset_class"]
        if t in seen:
            merged = {**krow, **{k: v for k, v in fortress_rows.get(t, {}).items() if v is not None}, "_source": "fortress+katlin", "_katlin": krow["_katlin"]}
            candidates[seen[t]] = score_candidate(merged, asset_class=cls if cls != "CRYPTO" else "STOCK",
                                                  risk_allows_entries=policy["allows_new_entries"], bottom_confirmation=bottom_map.get(t),
                                                  best_setup=best_map.get(t), floor=(floor_map or {}).get(t), buyback=(buyback_map or {}).get(t), options=option_map.get(t))
        else:
            seen[t] = len(candidates)
            candidates.append(score_candidate(krow, asset_class=cls, risk_allows_entries=policy["allows_new_entries"], bottom_confirmation=bottom_map.get(t),
                                              best_setup=best_map.get(t), floor=(floor_map or {}).get(t), buyback=(buyback_map or {}).get(t), options=option_map.get(t)))
    ranked = rank_candidates(candidates)
    selected = [x for x in ranked if x["action"] == "READY_TO_SNIPE"][:12]
    building = [x for x in ranked if x["action"] == "BUILDING_BASE"][:20]
    watch = [x for x in ranked if x["action"] == "WATCH_RECLAIM"][:12]
    rejected_examples = [x for x in ranked if x["action"] == "REJECTED"][:12]
    crypto_watch = [x for x in ranked if x["asset_class"] == "CRYPTO"][:15] + _crypto_watch(
        feeds.get("crypto_ma200") or {},
        feeds.get("crypto_confluence") or {},
        feeds.get("crypto_cycle_risk") or {},
    )

    allocator = feeds.get("allocator") or {}
    bond = feeds.get("bond_warroom") or {}
    total = len(candidates)
    n_katlin = sum(1 for x in candidates if str(x.get("source") or "").find("katlin") >= 0)
    required_bad = [x for x in source_health if x["critical"] and x["status"] != "FRESH"]
    fresh_count = sum(1 for x in source_health if x["status"] == "FRESH")
    top_eligible = selected or building or watch
    stance_score = round(top_eligible[0]["score"], 1) if top_eligible else None
    risk_score = {
        "DATA_HOLD": 100,
        "DEFENSIVE": 85,
        "SELECTIVE": 50,
        "SELECTIVE_RISK_ON": 25,
    }.get(policy["mode"], 75)
    confidence = round(
        (fresh_count / max(1, len(source_health))) * 0.45
        + ((sum(x.get("confidence", 0) for x in top_eligible[:5]) / len(top_eligible[:5])) if top_eligible else 0) * 0.55,
        3,
    )
    status = "NO_DATA" if total == 0 else ("DEGRADED" if required_bad else "OK")
    stance = (
        "CASH_OR_TBILLS" if policy["mode"] in {"DATA_HOLD", "DEFENSIVE"}
        else "SELECTIVE_BUY" if selected
        else "WAIT_FOR_CONFIRMATION"
    )
    asset_views = [
        {"asset_class": "Stocks", "stance": "SELECTIVE" if selected else "WAIT", "ready": sum(x["asset_class"] == "STOCK" for x in selected), "building": sum(x["asset_class"] == "STOCK" for x in building), "reason": "Only below-trend, evidence-complete bases qualify."},
        {"asset_class": "ETFs", "stance": "SELECTIVE" if any(x["asset_class"] == "ETF" for x in selected) else "WAIT", "ready": sum(x["asset_class"] == "ETF" for x in selected), "building": sum(x["asset_class"] == "ETF" for x in building), "reason": "Fund flows must confirm a favorable long-term location."},
        {"asset_class": "Crypto", "stance": "WATCH", "ready": 0, "building": len(crypto_watch), "reason": "A 200-day reclaim needs a durable base, flow confirmation and retest."},
        {"asset_class": "Bonds", "stance": bond.get("regime") or bond.get("state") or "MONITOR", "ready": 0, "building": 0, "reason": bond.get("summary") or bond.get("headline") or "Rates, credit and auction stress govern sizing."},
        {"asset_class": "Cash / T-bills", "stance": "PREFERRED" if not selected or policy["mode"] in {"DATA_HOLD", "DEFENSIVE"} else "RESERVE", "ready": 0, "building": 0, "reason": policy["default_shelter"]["why"]},
    ]
    domain_rows = [
        {"domain": "Risk regime", "score": 100 - risk_score, "direction": policy["mode"], "confidence": confidence, "status": "DEGRADED" if required_bad else "OK", "contributors": ["risk_gate", "crisis", "bond_warroom"]},
        {"domain": "Long-term location", "score": round(sum(x["score"] for x in top_eligible[:10]) / len(top_eligible[:10]), 1) if top_eligible else None, "direction": "BELOW_TREND_REQUIRED", "confidence": confidence, "status": "OK" if total else "NO_DATA", "contributors": ["fortress"]},
        {"domain": "Accumulation", "score": round(sum(min(100, len(x["evidence"]["accumulation"]) * 20) for x in top_eligible[:10]) / len(top_eligible[:10]), 1) if top_eligible else None, "direction": "CONFIRMING" if building or selected else "WEAK", "confidence": confidence, "status": "OK" if feeds.get("accumulation") else "DEGRADED", "contributors": ["fortress", "accumulation"]},
        {"domain": "Capital flows", "score": round(sum(min(100, len(x["evidence"]["flows"]) * 25) for x in top_eligible[:10]) / len(top_eligible[:10]), 1) if top_eligible else None, "direction": "SELECTIVE", "confidence": confidence, "status": "OK", "contributors": ["fortress", "options"]},
        {"domain": "Catalysts", "score": round(sum(min(100, len(x["evidence"]["catalysts"]) * 25) for x in top_eligible[:10]) / len(top_eligible[:10]), 1) if top_eligible else None, "direction": "SELECTIVE", "confidence": confidence, "status": "OK", "contributors": ["fortress", "buyback"]},
        {"domain": "Crypto cycle", "score": None, "direction": "WATCH_ONLY", "confidence": confidence, "status": "OK" if feeds.get("crypto_ma200") else "DEGRADED", "contributors": ["crypto_ma200", "crypto_confluence", "crypto_cycle_risk"]},
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
        "schema_version": "1.0.0",
        "engine": "justhodl-khalid",
        "version": "1.0.0",
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
        "top_signals": top_eligible[:12],
        "contradictions": contradictions[:20],
        "catalysts": catalyst_rows[:24],
        "risks": risks[:30],
        "inputs": source_health,
        "missing_inputs": [x for x in source_health if x["status"] != "FRESH"],
        "decision": {
            "posture": policy["mode"],
            "headline": (
                f"{len(selected)} setup{'s' if len(selected) != 1 else ''} cleared every gate"
                if selected else "No asset cleared every gate; preserve optionality"
            ),
            "plain_english": (
                "Khalid found long-term value, accumulation, flow, catalyst, and reward/risk alignment. "
                "The names below are armed for confirmation, not market orders."
                if selected else
                "The engine found no setup with the required long-term location, independent accumulation, "
                "capital flow, catalyst, and reward/risk evidence. Cash or short-term Treasury bills remain the default."
            ),
            "selected_count": len(selected),
            "building_count": len(building),
            "universe_scored": total,
            "universe_from_katlin": n_katlin,
            "shelter": policy["default_shelter"],
        },
        "risk_control": {
            **policy,
            "bond_market": {
                "generated_at": bond.get("generated_at"),
                "summary": bond.get("summary") or bond.get("headline"),
                "regime": bond.get("regime") or bond.get("state"),
                "note": "Bond-market evidence controls sizing and vetoes; it does not create an equity buy.",
            },
            "katlin_war_room": katlin_wr,
            "allocator": {
                "regime": allocator.get("regime_headline"),
                "cash_buffer_pct": allocator.get("cash_buffer_pct"),
                "recommended_weights_pct": allocator.get("recommended_weights_pct"),
            },
        },
        "selected": selected,
        "building_bases": building,
        "watch_reclaims": watch,
        "crypto_watch": crypto_watch[:15],
        "rejected_examples": rejected_examples,
        "methodology": {
            "mandate": "Find large-upside, defined-downside opportunities before consensus without chasing.",
            "long_horizon": "3M, monthly and weekly context establish the thesis; 200/250-day location is mandatory.",
            "entry_horizon": "Daily and 4h are execution-only. A breakout, retest and higher low may arm an entry; they cannot rescue a weak thesis.",
            "hard_gates": [
                "At or below the 200-day trend; below the 250-day trend is preferred",
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
            "score": round(((selected[0]["score"] if selected else 0) / 50.0) - 1.0, 3),
            "confidence": round(
                sum(x.get("confidence", 0) for x in selected) / len(selected), 3
            ) if selected else 0.0,
            "regime_context": [policy["mode"]],
            "lead_lag_days": 0,
            "state": "green" if selected and policy["allows_new_entries"] else ("amber" if building else "red"),
            "percentile_10y": None,
            "percentile_2008on": None,
            "contributes_to": ["opportunity_board", "risk_master"],
            "why": (
                f"{len(selected)} candidates passed every required gate"
                if selected else "No candidate passed every required gate"
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
            for row in top_eligible[:12]
        ],
        "risks": risks[:20],
        "catalysts": catalyst_rows[:20],
        "data_quality": source_health,
    }
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
    now = datetime.now(timezone.utc)
    feeds, metas = {}, {}
    for name, (key, _max_age, _critical) in FEEDS.items():
        feeds[name], metas[name] = _read(key)
    output = build_output(feeds, metas, now)
    _write(OUT_KEY, output)

    history, _ = _read(HISTORY_KEY)
    points = history.get("points") if isinstance(history.get("points"), list) else []
    point = {
        "generated_at": output["generated_at"],
        "posture": output["decision"]["posture"],
        "selected_count": output["decision"]["selected_count"],
        "building_count": output["decision"]["building_count"],
        "risk_gate_posture": output["risk_control"]["risk_gate_posture"],
        "top_ticker": output["selected"][0]["ticker"] if output["selected"] else None,
        "top_score": output["selected"][0]["score"] if output["selected"] else None,
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
