"""Display-only evidence. No I/O, policy decisions, sizing or new signal fitting."""
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json

SCHEMA = "khalid-qualification.v1"
BACKEND_VERSION = "khalid-existing-readiness.v1"
REQUESTED_VERSION = "khalid-requested-strategy.unresolved.v1"
# Publication display freshness only; never changes existing action/risk policy.
DISPLAY_TTL_HOURS = 26

BACKEND_RULES = (
    ("location", "Price at/below the existing 200-day average", "Existing selected EMA200, otherwise SMA200, distance <= 0 percent.", "percent"),
    ("compression", "Bollinger compression", "Existing bb_width_pctile <= 20; the upstream estimator is not redefined here.", "percentile"),
    ("supply", "Supply dry-up", "Existing volume_dryup <= 0.8; upstream reference window is not redefined here.", "ratio"),
    ("structure", "Existing structural confirmation", "At least two higher lows OR truthy OBV/AD divergence OR bottom phase ACCUMULATION OR flag LIKELY_BOTTOM.", "boolean"),
    ("legacy_flow", "Existing flow-clue gate", "At least one existing flows evidence row. These legacy clues are not verified ETF creations/redemptions or signed trades.", "count"),
    ("catalyst", "Existing catalyst-clue gate", "At least one existing catalysts evidence row.", "count"),
    ("rsi", "Existing RSI reset", "Selected bottom-model RSI, otherwise row RSI/rsi_14, <= 45.", "index"),
    ("dilution", "Existing dilution coverage", "Non-stock OR dilution observed OR net_issuer flag observed OR net buyback yield observed. The existing veto gate separately checks severity.", "boolean"),
    ("confidence", "Existing confidence floor", "Existing candidate confidence (including its existing fallback) >= 0.60; this is not a calibrated probability.", "fraction"),
    ("vetoes", "Existing vetoes", "No existing vetoes: location, observed ADV below $1M, modeled R/R below 2, dilution >= 10%, or net issuer. Missing ADV retains existing behavior.", "count"),
    ("reward_risk", "Existing entry geometry", "Valid planned stop < entry < target and (target-entry)/(entry-stop) >= 2.5.", "ratio"),
    ("risk_permission", "Existing risk permission", "Existing risk_allows_entries flag is true; this evidence never grants permission.", "boolean"),
    ("trigger", "Observed entry trigger", "entry_triggered is True OR daily_4h_triggered is True. Without this, an otherwise qualified entry remains ARMED.", "boolean"),
)

# Unconfirmed definitions deliberately remain unresolved. No old browser threshold
# is silently promoted to an approved strategy requirement.
REQUESTED_RULES = (
    ("universe", "Eligible universe and exclusions", "Point-in-time index membership, taxonomy, biotechnology and cap exclusions require an agreed definition."),
    ("ma250", "Below the 250-day", "Average type, session calendar and required/preference role are unconfirmed."),
    ("offhigh", "At least 50% off the high", "High anchor, window and corporate-action treatment are unconfirmed."),
    ("rsi", "RSI washed out", "Threshold, estimator, timeframe and warm-up are unconfirmed."),
    ("spread", "Very tight price spread", "Window and threshold are unconfirmed; OHLC range is not a quoted bid/ask spread."),
    ("bb", "Tight Bollinger bands", "Estimator, window and percentile threshold are unconfirmed."),
    ("volatility", "Very low volatility", "Estimator, reference window and threshold are unconfirmed."),
    ("volume", "Shrinking volume", "Volume normalization, window and threshold are unconfirmed."),
    ("flat", "Flat moving average", "Average, window and slope threshold are unconfirmed."),
    ("support_3m", "On 3-month support", "3m is unresolved: 63 native sessions, rolling calendar months and quarterly bars are different definitions."),
    ("higher_low", "Higher low", "Pivot detection, windows and minimum separation are unconfirmed."),
    ("capitulation", "Selling climax or capitulation", "Return/volume windows and thresholds are unconfirmed. Close location means (close-low)/(high-low) for nonzero range, not price percent above low."),
    ("demand", "Demand showing", "Price/volume proxy is not signed buying; accepted evidence and threshold are unconfirmed."),
    ("valuation", "Cheap valuation", "Industry comparables, point-in-time fundamentals and required/preference role are unconfirmed."),
    ("industry", "Industry ETF near its 200-day", "ETF mapping, average and tolerance are unconfirmed."),
    ("true_flow", "Verified fund flows", "Exact fund identity, creation/redemption measure, window and publication lag are unconfirmed. Legacy flow scores and holdings are not proof."),
    ("resilience", "Resilience versus exact S&P benchmark", "Exact benchmark instrument, return treatment and horizon are unconfirmed. SPY is not silently substituted for the S&P index."),
    ("crypto_relative", "ETH/BTC relative turn", "Eligible crypto-linked taxonomy, leader, lag and relative-return definition are unconfirmed."),
    ("ma300", "Below the 300-day preference", "Average, session calendar and optional/required role are unconfirmed."),
    ("double_bottom", "Double bottom preference", "Pivot tolerance, confirmation and optional/required role are unconfirmed."),
    ("campaign", "Accumulation campaign preference", "Accepted model and optional/required role are unconfirmed."),
    ("catalyst", "Catalyst preference", "Point-in-time evidence and optional/required role are unconfirmed."),
    ("momentum", "Momentum turning up preference", "Estimator, timeframe and optional/required role are unconfirmed."),
)


def backend_observation(ticker, asset_class, action, facts, input_sources):
    """Record already-computed native gate outcomes, without re-evaluating them."""
    criteria = []
    for key, label, definition, unit in BACKEND_RULES:
        passed, value = facts[key]
        criteria.append({"id": key, "status": "UNAVAILABLE" if value is None else "PASS" if passed else "FAIL", "value": value})
    return {"schema_version": BACKEND_VERSION, "ticker": ticker, "asset_class": asset_class,
            "reported_action": action, "criteria": criteria, "input_sources": sorted(set(input_sources)),
            "source_attribution": "Producer inputs are identified; exact per-field lineage through merged rows is not retained."}


def publish_qualification(output, scored_rows):
    """Project final radar actions and native gates; mutate only one new field."""
    generated = output["generated_at"]
    lookup = {(r["asset_class"], r["ticker"]): r for r in scored_rows}
    rows = []
    for index, row in enumerate(output.get("opportunity_radar") or []):
        key = (row.get("asset_class"), row.get("ticker"))
        scored = lookup.get(key) or {}
        native = deepcopy(scored.get("backend_gate_observation"))
        clocks = {"evaluated_at": generated, "effective_at": None, "available_at": None,
                  "clock_note": "Evaluation/publication time is known. Historical per-field effective/availability clocks are not retained."}
        sources = [{k: h.get(k) for k in ("name", "key", "status", "as_of", "s3_modified_at", "max_age_h", "critical")}
                   for h in output.get("source_health", [])
                   if h.get("critical") or h.get("name") in (native or {}).get("input_sources", [])]
        fresh = (bool(sources) and all(h["status"] == "FRESH" and h["as_of"] for h in sources)
                 and set((native or {}).get("input_sources", [])).issubset({h["name"] for h in sources}))
        if native:
            native["scoring_action"] = native.pop("reported_action")
        else:
            native = {"schema_version": BACKEND_VERSION, "ticker": key[1], "asset_class": key[0],
                      "scoring_action": None, "criteria": [], "input_sources": [],
                      "source_attribution": "No native execution inputs retained for this discovery-only row."}
        native.update({"reported_action": row.get("action"),
                       "status": ("PASS" if row.get("action") == "READY_TO_SNIPE" else "FAIL") if scored and fresh else "UNAVAILABLE",
                       "criterion_definitions_ref": "backend_definitions",
                       "criterion_clocks": "Use this contract's clocks; source freshness is separate from historical availability",
                       "authority": "Existing published backend readiness only; no new capital authority",
                       "clocks": deepcopy(clocks), "sources": sources,
                       "reason": "Final discovery/lifecycle action is authoritative; gate observations precede those controls." if scored and fresh else "Execution qualification or required source freshness is unavailable; published action is retained for audit only."})
        requested = {"schema_version": REQUESTED_VERSION, "ticker": key[1], "asset_class": key[0],
                     "status": "UNRESOLVED", "complete": False,
                     "authority": "Requested strategy evidence only; cannot change existing readiness or capital permission",
                     "clocks": deepcopy(clocks),
                     "criteria_ref": "requested_criteria"}
        rows.append({"ticker": key[1], "asset_class": key[0], "name": row.get("name"),
                     "source_path": "opportunity_radar/" + str(index),
                     "existing_backend_qualification": native, "requested_strategy_qualification": requested})
    packet = {"schema_version": SCHEMA, "generated_at": generated,
              "expires_at": (datetime.fromisoformat(generated.replace("Z", "+00:00")) + timedelta(hours=DISPLAY_TTL_HOURS)).isoformat(),
              "freshness_scope": "26-hour publication display limit only; source statuses/clocks remain separate, no risk-policy change",
              "historical_validation": "UNAVAILABLE: these contracts do not establish backtest validity or calibrated performance",
              "backend_definitions": [{"id": k, "label": label, "definition": definition, "unit": unit,
                                       "definition_version": BACKEND_VERSION, "applicability": "EXISTING_ENTRY_GATE",
                                       "provenance": "scoring.py:score_candidate; recorded native gate, not independently validated market evidence"}
                                      for k, label, definition, unit in BACKEND_RULES],
              "requested_criteria": [{"id": k, "label": label, "definition": reason,
                                      "definition_version": REQUESTED_VERSION, "applicability": "UNRESOLVED",
                                      "status": "UNRESOLVED", "value": None, "unit": None,
                                      "clocks": {"evaluated_at": generated, "effective_at": None, "available_at": None,
                                                 "clock_note": "No approved definition or qualifying measurement; publication time is not historical availability."},
                                      "provenance": "Unconfirmed requested specification; no measurement substituted"}
                                     for k, label, reason in REQUESTED_RULES],
              "rows": rows}
    packet["revision"] = hashlib.sha256(json.dumps(packet, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    for row in rows:
        row["artifact_revision"] = packet["revision"]
    output["qualification_evidence"] = packet
