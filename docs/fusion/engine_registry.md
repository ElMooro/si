# Engine registry

`config/engine-registry.v1.json` (schema `ENGINE-REGISTRY-1.0`) is the persisted metadata for every engine the fusion
layer reads. It is validated on load (`jh_registry.validate_registry`); a bad entry fails the Lambda at cold start
instead of silently dropping an engine. The bridge mirrors it to `data/jhsignal/state/registry.json` on every run.

Per engine: engine_id, engine_name, engine_family (MACRO/FLOW/FUNDAMENTAL/CATALYST/MARKET/RISK), description, owner,
producer (Lambda), artifact (S3 key under data/), timestamp_paths, version, status (active/shadow/disabled),
criticality (NONCRITICAL/IMPORTANT/CRITICAL), default_horizon, expected_update_frequency, freshness_ttl_seconds,
half_life_days, entities_supported, dependencies, historical_backtest_available, calibration_available,
hard_veto_capable, soft_veto_capable, signal_schema_version, adapter, trust_key (name used by engine-trust /
signal-scorecard / signal-orthogonality), signal_types -> {cluster, category}.

## Release-1 pilot (17 engines, all six families)
| Family | engine_id | Producer / artifact | Criticality | Horizon | Half-life |
|---|---|---|---|---|---|
| MACRO | regime_composite | justhodl-regime-composite `data/regime-composite.json` | IMPORTANT | SWING | 5d |
| MACRO | liquidity_credit_engine (LCE) | justhodl-liquidity-credit-engine | IMPORTANT | INTERMEDIATE | 14d |
| MACRO | global_business_cycle | justhodl-global-business-cycle | NONCRITICAL | INTERMEDIATE | 30d |
| RISK | risk_gate | justhodl-risk-gate | **CRITICAL** | SWING | 3d |
| RISK | crisis_composite | justhodl-crisis-composite | **CRITICAL** | TACTICAL | 1d |
| RISK | tail_risk | justhodl-tail-risk | IMPORTANT | TACTICAL | 2d |
| FLOW | insider_radar | justhodl-insider-radar | NONCRITICAL | SWING | 21d |
| FLOW | institutional_13f_flows | justhodl-13f-positions `data/13f-flows-by-ticker.json` | NONCRITICAL | INTERMEDIATE | 45d |
| FLOW | etf_flows | justhodl-etf-flows | NONCRITICAL | SWING | 7d |
| FLOW | dark_pool | justhodl-dark-pool | NONCRITICAL | SWING | 14d |
| FLOW | short_interest | justhodl-short-interest | NONCRITICAL | SWING | 14d |
| FUNDAMENTAL | estimate_revisions | justhodl-estimate-revisions | NONCRITICAL | INTERMEDIATE | 30d |
| MARKET | momentum_leaders | justhodl-momentum-leaders | NONCRITICAL | SWING | 10d |
| MARKET | fortress | justhodl-fortress | NONCRITICAL | SWING | 15d |
| MARKET (+CATALYST type) | katlin | justhodl-katlin | NONCRITICAL | INTERMEDIATE | 20d |
| CATALYST | catalyst | justhodl-catalyst | NONCRITICAL | SWING | 10d |
| CATALYST | dealer_gex | justhodl-dealer-gex | NONCRITICAL | TACTICAL | 2d |

Adding an engine = one registry entry + one adapter class (see `aws/shared/jh_adapters.py`) + a fixture in
`aws/lambdas/justhodl-jhsignal-bridge/tests/jh_fixtures.py`. `adapter_for` refuses an adapter whose signal_type the
registry does not declare. Runtime disable without deploy: `FUSION_DISABLED_ENGINES` in the SSM flags document.
