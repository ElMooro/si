"""Institutional risk board for Khalid.

The board exposes independent market-risk lenses without silently averaging
them.  The existing risk gate remains authoritative; these diagnostics may
only tighten capital permission, never loosen it.
"""
from __future__ import annotations

import math
from typing import Any


def _num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _rows(value: Any) -> list[dict]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _text(value: Any, fallback: str = "Unavailable") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def _risk_level(score: float | None) -> str:
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


def _crisis_component_max_age(row: dict) -> float:
    source = str(row.get("source") or "").lower()
    return 192.0 if source in {"ciss_ea", "ciss"} else 72.0


def _health(source_health: list[dict], source_id: str) -> dict:
    return next((row for row in source_health if row.get("name") == source_id), {})


def _card(
    source_health: list[dict],
    source_id: str,
    domain: str,
    label: str,
    score: float | None,
    state: Any,
    summary: Any,
    metrics: list[dict],
    *,
    severity: str | None = None,
) -> dict:
    health = _health(source_health, source_id)
    return {
        "id": source_id,
        "domain": domain,
        "label": label,
        "score": round(score, 1) if score is not None else None,
        "severity": severity or _risk_level(score),
        "state": _text(state, "UNKNOWN"),
        "summary": _text(summary),
        "status": health.get("status", "UNKNOWN"),
        "age_h": health.get("age_h"),
        "as_of": health.get("as_of"),
        "artifact": health.get("key"),
        "metrics": [
            {
                "label": item.get("label"),
                "value": item.get("value"),
                "unit": item.get("unit"),
                "state": item.get("state"),
            }
            for item in metrics
            if item.get("value") is not None or item.get("state") is not None
        ],
    }


def _currency(currencies: Any, code: str) -> dict:
    if isinstance(currencies, dict):
        value = currencies.get(code) or currencies.get(code.lower())
        return value if isinstance(value, dict) else {}
    for row in _rows(currencies):
        if str(row.get("code") or "").upper() == code:
            return row
    return {}


def _bond_panel(panels: Any, key: str) -> dict:
    if not isinstance(panels, dict):
        return {}
    for rows in panels.values():
        for row in _rows(rows):
            if str(row.get("key") or "").upper() == key:
                return row
    return {}


def _auction_summary(auction: dict) -> tuple[float | None, str, str, list[dict]]:
    today = _dict(auction.get("today"))
    verdict = _dict(today.get("verdict"))
    auctions = _rows(today.get("auctions"))
    coupon_rows = [row for row in auctions if str(row.get("type") or "").lower() != "bill"]
    observed = coupon_rows or auctions
    demand = [_num(row.get("demand_score")) for row in observed]
    demand = [value for value in demand if value is not None]
    # Demand z-scores are centered at zero. Map weak demand to high risk.
    score = None
    if demand:
        score = max(0.0, min(100.0, 50.0 - (sum(demand) / len(demand)) * 25.0))
    risk_assets = str(verdict.get("risk_assets") or "unknown").upper()
    if risk_assets == "BEARISH":
        score = max(score or 0, 65.0)
    elif risk_assets == "BULLISH" and score is None:
        score = 25.0
    metrics = []
    if observed:
        latest = observed[0]
        metrics = [
            {"label": "Latest grade", "value": latest.get("grade")},
            {"label": "Bid-to-cover", "value": _num(latest.get("btc"))},
            {"label": "Indirect bidders", "value": _num(latest.get("indirect_pct")), "unit": "%"},
            {"label": "Dealer take", "value": _num(latest.get("pd_pct")), "unit": "%"},
            {"label": "Tail", "value": _num(latest.get("tail_bp")), "unit": "bp"},
        ]
    return score, risk_assets, _text(verdict.get("headline"), "No current auction verdict"), metrics


def build_risk_board(
    feeds: dict[str, dict],
    source_health: list[dict],
    policy: dict,
) -> tuple[dict, dict]:
    """Build the displayed board and tighten a copy of the capital policy."""
    policy = {**policy, "reasons": list(policy.get("reasons") or [])}
    risk_gate = _dict(feeds.get("risk_gate"))
    crisis = _dict(feeds.get("crisis"))
    bond = _dict(feeds.get("bond_warroom"))
    dollar = _dict(feeds.get("dollar_radar"))
    euro = _dict(feeds.get("euro_fragmentation"))
    auction = _dict(feeds.get("auction_desk"))
    eurodollar = _dict(feeds.get("eurodollar_stress"))
    fx = _dict(feeds.get("fx_intelligence"))
    vol = _dict(feeds.get("fifx_vol"))
    liquidity = _dict(feeds.get("global_liquidity"))
    cycle = _dict(feeds.get("cycle_clock"))
    credit = _dict(feeds.get("credit_composite"))

    heartbeat = _dict(bond.get("heartbeat"))
    bond_equity = _dict(bond.get("equity_risk"))
    shortage = _dict(bond.get("eurodollar_shortage"))
    euro_frag = _dict(euro.get("fragmentation"))
    countries = _dict(euro.get("countries"))
    italy = _dict(countries.get("IT"))
    spain = _dict(countries.get("ES"))
    btp_bund = _num(italy.get("spread_vs_bund_bp"))
    it_es = None
    if _num(italy.get("yield_10y_pct")) is not None and _num(spain.get("yield_10y_pct")) is not None:
        it_es = (_num(italy.get("yield_10y_pct")) - _num(spain.get("yield_10y_pct"))) * 100
    # The bond war-room publishes these exact series in panels.europe_spreads.
    # Keep them as a fallback when the dedicated fragmentation feed is absent.
    bond_btp = _bond_panel(bond.get("panels"), "BTP-BUND")
    bond_it_es = _bond_panel(bond.get("panels"), "IT-ES")
    if btp_bund is None:
        last = _num(bond_btp.get("last"))
        btp_bund = last * 100 if last is not None else None
    if it_es is None:
        last = _num(bond_it_es.get("last"))
        it_es = last * 100 if last is not None else None

    usd = _dict(fx.get("usd_regime"))
    jpy = _currency(fx.get("currencies"), "JPY")
    eur = _currency(fx.get("currencies"), "EUR")
    vol_legs = _dict(vol.get("legs"))
    fixed_income_vol = _dict(vol_legs.get("fixed_income"))
    fx_vol = _dict(vol_legs.get("fx"))
    equity_vol = _dict(vol_legs.get("equity"))
    migration = _dict(vol.get("migration"))
    synthesis = _dict(cycle.get("synthesis"))
    cycle_block = _dict(cycle.get("cycle"))
    global_liq = _dict(liquidity.get("global_liquidity_index"))
    fed_liq = _dict(liquidity.get("fed_net_liquidity"))

    auction_score, auction_state, auction_text, auction_metrics = _auction_summary(auction)
    dollar_score = _num(dollar.get("dollar_pressure"))
    credit_score = _num(credit.get("composite"))
    if credit_score is None:
        credit_score = _num(credit.get("composite_score"))
    eurodollar_score = _num(eurodollar.get("composite_score"))
    fragmentation_score = _num(euro_frag.get("score_0_100"))
    bond_score = _num(bond_equity.get("score"))
    heartbeat_score = _num(heartbeat.get("score"))
    crisis_score = _num(crisis.get("master_crisis_score"))
    defcon = _num(crisis.get("defcon_level"))
    crisis_components = _rows(crisis.get("components"))
    crisis_available = sum(row.get("available") is True for row in crisis_components)
    crisis_stale = sum(
        row.get("available") is True
        and (
            _num(row.get("age_hours")) is None
            or _num(row.get("age_hours")) > _crisis_component_max_age(row)
        )
        for row in crisis_components
    )
    cycle_posture = str(synthesis.get("posture") or "UNKNOWN").upper()
    cycle_score_raw = _num(synthesis.get("score"))
    cycle_risk_score = max(0.0, min(100.0, 50.0 - (cycle_score_raw or 0))) if cycle_score_raw is not None else None
    migration_state = str(migration.get("state") or "UNKNOWN").upper()
    migration_score = {
        "CALM": 15.0,
        "UPSTREAM_BREWING": 60.0,
        "MIGRATING": 75.0,
        "BROAD_STRESS": 90.0,
    }.get(migration_state)
    liquidity_regime = str(liquidity.get("regime") or "UNKNOWN").upper()
    liquidity_score = {
        "EXPANDING": 15.0,
        "NEUTRAL": 40.0,
        "CONTRACTING": 70.0,
    }.get(liquidity_regime)
    fx_risk_state = str(usd.get("risk_state") or fx.get("risk_state") or "UNKNOWN").upper()
    fx_score = {
        "CALM": 15.0,
        "LOW": 20.0,
        "WATCH": 40.0,
        "ELEVATED": 60.0,
        "HIGH": 75.0,
        "SEVERE": 90.0,
        "CRISIS": 95.0,
    }.get(fx_risk_state)
    if fx_score is None and "RISK-OFF" in fx_risk_state:
        fx_score = 70.0
    elif fx_score is None and "RISK-ON" in fx_risk_state:
        fx_score = 20.0

    cards = [
        _card(source_health, "risk_gate", "master", "Master risk gate",
              max(0.0, min(100.0, 50.0 - (_num(risk_gate.get("composite")) or 0) * 25)),
              risk_gate.get("posture"), "Authoritative cross-domain capital gate.", [
                  {"label": "Composite", "value": _num(risk_gate.get("composite"))},
                  {"label": "Sizing multiplier", "value": _num(risk_gate.get("sizing_multiplier")), "unit": "x"},
              ]), 
        _card(source_health, "crisis", "black_swan", "Crisis composite coverage",
              crisis_score, f"DEFCON {int(defcon)}" if defcon is not None else "UNKNOWN",
              "Component coverage and age are audited before the crisis composite can control capital.", [
                  {"label": "Crisis score", "value": crisis_score, "unit": "/100"},
                  {"label": "Components available", "value": crisis.get("components_available")},
                  {"label": "Available rows", "value": crisis_available},
                  {"label": "Stale component rows", "value": crisis_stale},
              ]),
        _card(source_health, "credit_composite", "credit", "ICE BofA + credit plumbing",
              credit_score, _risk_level(credit_score),
              "Credit spreads, dealer positioning, settlement and funding stress.", [
                  {"label": "Credit composite", "value": credit_score, "unit": "/100"},
              ]),
        _card(source_health, "eurodollar_stress", "funding", "Eurodollar shortage",
              eurodollar_score, eurodollar.get("severity") or eurodollar.get("regime"),
              "Offshore-dollar and money-market stress, including ICE BofA OAS inputs.", [
                  {"label": "Stress composite", "value": eurodollar_score, "unit": "/100"},
                  {"label": "Signals used", "value": _num(eurodollar.get("n_signals_used"))},
                  {"label": "Failures", "value": _num(eurodollar.get("n_failures"))},
              ]),
        _card(source_health, "dollar_radar", "dollar", "DXY + broad dollar",
              dollar_score, dollar.get("regime"), dollar.get("headline") or dollar.get("regime_note"), [
                  {"label": "Dollar pressure", "value": dollar_score, "unit": "/100"},
                  {"label": "FX risk state", "value": usd.get("risk_state")},
                  {"label": "Broad USD 3m", "value": _num(usd.get("chg_3m")), "unit": "%"},
              ]),
        _card(source_health, "euro_fragmentation", "sovereign", "BTP-Bund + Italy-Spain",
              fragmentation_score, euro_frag.get("regime"), euro.get("headline") or euro_frag.get("read"), [
                  {"label": "BTP-Bund", "value": btp_bund, "unit": "bp"},
                  {"label": "Italy 10Y - Spain 10Y", "value": round(it_es, 1) if it_es is not None else None, "unit": "bp"},
                  {"label": "Fragmentation", "value": fragmentation_score, "unit": "/100"},
              ]),
        _card(source_health, "auction_desk", "treasury", "Treasury auction demand",
              auction_score, auction_state, auction_text, auction_metrics),
        _card(source_health, "bond_warroom", "rates", "Bond market war room",
              max(value for value in (bond_score, heartbeat_score) if value is not None)
              if any(value is not None for value in (bond_score, heartbeat_score)) else None,
              heartbeat.get("regime") or bond_equity.get("state") or bond_equity.get("level"),
              heartbeat.get("headline") or bond_equity.get("text") or shortage.get("text"), [
                  {"label": "Heartbeat", "value": heartbeat_score, "unit": "/100"},
                  {"label": "Equity transmission", "value": bond_score, "unit": "/100"},
                  {"label": "Shortage score", "value": _num(shortage.get("score")), "unit": "/100"},
                  {"label": "BTP-Bund", "value": btp_bund, "unit": "bp"},
                  {"label": "Italy-Spain", "value": it_es, "unit": "bp"},
              ]),
        _card(source_health, "fifx_vol", "volatility", "Rates, FX and equity volatility",
              migration_score, migration_state, migration.get("read"), [
                  {"label": "MOVE", "value": _num(fixed_income_vol.get("level"))},
                  {"label": "FX realized vol", "value": _num(fx_vol.get("level_pct")), "unit": "%"},
                  {"label": "VIX", "value": _num(equity_vol.get("level"))},
                  {"label": "JPY vol 20d", "value": _num(jpy.get("vol_20d")), "unit": "%"},
                  {"label": "EUR vol 20d", "value": _num(eur.get("vol_20d")), "unit": "%"},
              ]),
        _card(source_health, "fx_intelligence", "fx", "FX intelligence",
              fx_score, fx_risk_state, fx.get("headline") or usd.get("read"), [
                  {"label": "USD risk state", "value": fx_risk_state},
                  {"label": "Broad USD 3m", "value": _num(usd.get("chg_3m")), "unit": "%"},
                  {"label": "JPY vol 20d", "value": _num(jpy.get("vol_20d")), "unit": "%"},
                  {"label": "EUR vol 20d", "value": _num(eur.get("vol_20d")), "unit": "%"},
              ]),
        _card(source_health, "global_liquidity", "liquidity", "Global liquidity",
              liquidity_score, liquidity_regime, liquidity.get("regime_read"), [
                  {"label": "G3 impulse 13w", "value": _num(liquidity.get("global_impulse_13w_pct")), "unit": "%"},
                  {"label": "Global liquidity", "value": _num(global_liq.get("total_usd_trillions")), "unit": "$T"},
                  {"label": "Fed net liquidity", "value": _num(fed_liq.get("value_usd_trillions")), "unit": "$T"},
              ]),
        _card(source_health, "cycle_clock", "cycle", "Market cycle",
              cycle_risk_score, cycle_posture, synthesis.get("bottom_line") or cycle.get("verdict"), [
                  {"label": "Cycle phase", "value": cycle_block.get("headline_phase") or cycle_block.get("phase")},
                  {"label": "Posture score", "value": cycle_score_raw},
                  {"label": "Risk-off signals", "value": _num(synthesis.get("n_risk_off"))},
                  {"label": "Risk-on signals", "value": _num(synthesis.get("n_risk_on"))},
              ]),
    ]

    fresh_cards = [card for card in cards if card["status"] == "FRESH" and card["score"] is not None]
    board_score = (
        round(sum(card["score"] for card in fresh_cards) / len(fresh_cards), 1)
        if fresh_cards else None
    )
    hard = []
    selective = []
    if credit_score is not None:
        (hard if credit_score >= 75 else selective if credit_score >= 45 else []).append(
            f"Credit composite is {credit_score:.0f}/100"
        )
    if eurodollar_score is not None:
        (hard if eurodollar_score >= 70 else selective if eurodollar_score >= 50 else []).append(
            f"Eurodollar stress is {eurodollar_score:.0f}/100"
        )
    if fragmentation_score is not None:
        (hard if fragmentation_score >= 80 else selective if fragmentation_score >= 60 else []).append(
            f"Euro fragmentation is {fragmentation_score:.0f}/100"
        )
    if dollar_score is not None and dollar_score >= 70:
        selective.append(f"Dollar pressure is {dollar_score:.0f}/100")
    if auction_state == "BEARISH":
        selective.append("Treasury auction read is bearish for risk assets")
    if migration_state == "BROAD_STRESS":
        hard.append("Rates, FX and equity volatility are in broad stress")
    elif migration_state in {"MIGRATING", "UPSTREAM_BREWING"}:
        selective.append(f"Cross-asset volatility state is {migration_state}")
    if liquidity_regime == "CONTRACTING":
        selective.append("Global liquidity is contracting")
    if cycle_posture == "RISK-OFF":
        selective.append("Cycle clock is risk-off")
    if str(bond_equity.get("state") or "").upper() in {
        "STRESS", "CRISIS", "SEVERE", "DUMP RISK", "FLIGHT TO SAFETY"
    }:
        hard.append(f"Bond-to-equity transmission is {bond_equity.get('state')}")
    if str(heartbeat.get("regime") or "").upper() == "ACUTE":
        hard.append("Bond-market heartbeat is acute")
    if fx_risk_state in {"SEVERE", "CRISIS"}:
        hard.append(f"FX risk state is {fx_risk_state}")

    # Tighten only. The master gate can never be overruled by these diagnostics.
    if policy.get("mode") not in {"DATA_HOLD", "DEFENSIVE"}:
        if hard:
            policy["mode"] = "DEFENSIVE"
            policy["allows_new_entries"] = False
            policy["reasons"].append("Independent market-risk veto: " + "; ".join(hard[:3]))
        elif selective:
            policy["mode"] = "SELECTIVE"
            policy["reasons"].append("Exposure cap tightened: " + "; ".join(selective[:3]))

    base_cap = _num(policy.get("sizing_multiplier"))
    exposure_cap = round((base_cap if base_cap is not None else 0.0) * 100)
    # Continuous modifiers only reduce the gate's cap. Credit is primary:
    # deterioration begins reducing exposure before it becomes a hard veto.
    if credit_score is not None:
        exposure_cap = min(exposure_cap, round(max(0.0, 100.0 - credit_score)))
    if eurodollar_score is not None:
        exposure_cap = min(exposure_cap, round(max(0.0, 115.0 - eurodollar_score)))
    if fragmentation_score is not None:
        exposure_cap = min(exposure_cap, round(max(0.0, 120.0 - fragmentation_score)))
    if migration_score is not None:
        exposure_cap = min(exposure_cap, round(max(0.0, 120.0 - migration_score)))
    if policy.get("mode") == "DATA_HOLD":
        exposure_cap = 0
    elif policy.get("mode") == "DEFENSIVE":
        exposure_cap = min(exposure_cap, 10)
    elif policy.get("mode") == "SELECTIVE":
        exposure_cap = min(exposure_cap, 50)
    policy["sizing_multiplier"] = min(
        base_cap if base_cap is not None else 0.0,
        exposure_cap / 100.0,
    )
    policy["exposure_cap_pct"] = exposure_cap

    decision = (
        "STAY IN CASH / SHORT-TERM TREASURIES"
        if policy.get("mode") in {"DATA_HOLD", "DEFENSIVE"}
        else "INVEST SELECTIVELY"
        if policy.get("allows_new_entries")
        else "WAIT IN CASH / SHORT-TERM TREASURIES"
    )
    return {
        "schema_version": "3.0.0",
        "capital_decision": decision,
        "mode": policy.get("mode"),
        "allows_new_entries": bool(policy.get("allows_new_entries")),
        "exposure_cap_pct": exposure_cap,
        "risk_score": board_score,
        "hard_vetoes": hard,
        "tighteners": selective,
        "conflicts": [
            {
                "label": "Risk-gate / cycle disagreement",
                "detail": f"Master gate {risk_gate.get('posture')} versus cycle {cycle_posture}",
            }
        ] if (
            risk_gate.get("posture") in {"RISK_ON", "NEUTRAL"}
            and cycle_posture == "RISK-OFF"
        ) else [],
        "domains": cards,
        "method": (
            "Independent lenses remain visible. The master risk gate is authoritative; "
            "credit, funding, sovereign, auction, volatility, liquidity and cycle signals "
            "may only tighten exposure or veto entries, never loosen the gate."
        ),
    }, policy
