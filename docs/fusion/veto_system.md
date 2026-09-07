# Veto system

Vetoes travel ON the signal: an adapter for a RISK/MACRO/CATALYST engine sets `metadata.veto = {type: HARD|SOFT,
severity 0-1, reason}` from the engine's own thresholds. Fusion aggregates them per entity x horizon
(`hard_vetoes`, `soft_vetoes`, each with engine, signal_type, signal_id, data_asof, horizon, invalidation).

| Source | Trigger | Type | Effect |
|---|---|---|---|
| crisis_composite | master_crisis_score >= 80 (existing fusion-policy hard_veto) or DEFCON <= 2 | HARD | capital_decision BLOCKED |
| crisis_composite | master_crisis_score >= 65 (existing risk_off threshold) | SOFT | size_modifier |
| risk_gate | posture SEVERE (sizing 0.20; brain rule "never touch stocks when plumbing is shaky") | HARD | BLOCKED |
| risk_gate | posture RISK_OFF (sizing 0.45) | SOFT | size_modifier |
| liquidity_credit_engine | stress composite >= 60 (ELEVATED) | SOFT | size_modifier |
| tail_risk | index tail_stress >= 70 | SOFT | size_modifier (that ETF and market context) |
| dealer_gex | NEGATIVE / STRONG_NEGATIVE gamma (dealers short gamma) | SOFT | size_modifier |
| registry | a CRITICAL engine (risk_gate, crisis_composite) has no live signal | HARD (`CAPITAL_DECISION_BLOCKED: ...`, last_valid published) | BLOCKED for every entity |
| registry | a CRITICAL engine is STALE | reported as IMPORTANT failure | confidence/coverage only |

`size_modifier = product(max(0.2, 1 - 0.5 x severity))` over soft vetoes; `capital_decision` is BLOCKED / REDUCED /
OPEN. Modifiers that *raise* weight (seasonality, sector leadership...) are Release 4+ and are never vetoes.
`FUSION_VETO_ENABLED=false` publishes the veto list but sets no capital decision (used only for research replays).
A veto clears exactly when the underlying signal clears (tested: hard veto fired -> cleared).
