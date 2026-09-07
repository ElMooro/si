# Gap analysis -- proposed fusion capabilities vs what the repository already has

Classes: **EXISTS** (use as is) / **PARTIAL** / **MISSING** / **SHOULD_REUSE** / **SHOULD_EXTEND** / **SHOULD_REPLACE**.
Release column = where the gap is closed (R1 shipped in ops 5212).

| # | Capability (spec phase) | Class | Evidence in repo | Release |
|---|---|---|---|---|
| 1 | Universal signal schema JHSIGNAL-1.0 | MISSING -> shipped | fusion-schema.v1 is macro packets; DDB rows are grading rows | R1 `schemas/jhsignal-1.0.json`, `aws/shared/jhsignal.py` |
| 2 | Engine registry (persisted metadata) | PARTIAL / SHOULD_EXTEND | fusion-registry.v1 (10), engine-contracts (learned), engine-manifest | R1 `config/engine-registry.v1.json` (17 engines, 6 families) |
| 3 | Signal adapters | MISSING -> shipped | engines expose bespoke JSON | R1 `aws/shared/jh_adapters.py` (17) |
| 4 | Intelligence bus | EXISTS / SHOULD_REUSE | `justhodl-system-events`, coordinator, `system_events.py` | R1 routes for `jhsignal.*` |
| 5 | Signal registry / current state | MISSING -> shipped | DDB tables are ledgers | R1 DDB `justhodl-jhsignal-state` + `data/jhsignal/state/latest.json` |
| 5 | S3 archive of the signal stream | MISSING -> shipped | engines keep own history keys | R1 `data/jhsignal/archive/` (append-only gz NDJSON) |
| 6 | Freshness + exponential decay | PARTIAL -> shipped | binary staleness (contracts, feed-registry) | R1 half-life per engine, FRESH/STALE/EXPIRED |
| 7 | Entity master with canonical ids | PARTIAL | symdir, finviz-universe, census; ticker-only ids | R1 `type:SYMBOL` ids + canonicalisation; full master R3 |
| 8 | Entity + exposure graph | PARTIAL | impact_mapper graph/betas, IND_ETF, stock-exposure-lookup | R3 (relational table `entity_relationships`) |
| 9 | Signal propagation | MISSING | -- | R3 (affects[] already carried on every signal) |
| 10 | Engine trigger router | PARTIAL / SHOULD_EXTEND | coordinator ROUTES, event-driven invokes | R3 (configurable rules) |
| 11 | Hypothesis engine | MISSING | conviction-engine subjects are the closest analogue | R3 |
| 12 | Independence clustering | PARTIAL -> shipped | conviction-engine family decorrelation; fusion-v1 dedupe | R1 evidence clusters in registry, diminishing confirmations |
| 13 | Correlation matrix | EXISTS / SHOULD_REUSE | `justhodl-signal-orthogonality` | R1 consumed when present (engine names aligned by trust_key) |
| 14 | Basic fusion engine (multi-horizon, six families) | PARTIAL -> shipped | subject-level conviction; macro engine-fusion | R1 `aws/shared/jh_fusion_core.py`, `justhodl-jh-fusion` (shadow) |
| 15 | Preserve contradictions (bull/bear kept) | PARTIAL -> shipped | fusion-v1 `disagreements` | R1 bullish/bearish evidence, family scores |
| 16 | Contradiction engine | MISSING -> shipped | -- | R1 0-100 score + LOW/MODERATE/HIGH/EXTREME |
| 17 | Hard / soft veto system | PARTIAL -> shipped | risk-gate posture, fusion-policy thresholds, katlin vetoes | R1 `metadata.veto` on RISK signals; capital_decision |
| 18 | Market regime vector + labels | EXISTS / SHOULD_REUSE | regime-composite (7 dims), macro-regime, cross-asset-regime, GBC | R1 regime axis v1; R4 full vector |
| 19 | Regime-conditioned reliability | EXISTS / SHOULD_REUSE | `justhodl-regime-conditional-trust`, engine-trust | R4 wire into fusion |
| 20 | Engine performance database | EXISTS | scorecard, outcome-checker, calibration-fleet-history, halflife | R5 expose by regime/sector/cap |
| 21 | Alpha decay detection | EXISTS | `justhodl-alpha-decay`, signal-halflife, scorecard deprecation | R5 feed reliability |
| 22-23 | Confluence discovery / interaction alpha | PARTIAL | `justhodl-alpha-confluence`, `sequence-alpha-detector`, research-backtest | R5 (offline batch) |
| 24 | Historical analog engine (macro) | EXISTS | `justhodl-historical-analogs` (Euclidean), positioning-analog | R6 add Mahalanobis/cosine, expose to fusion |
| 25 | Asset-specific analogs | MISSING | -- | R6 |
| 26 | Signal velocity / acceleration | PARTIAL -> shipped | fortress/katlin `changes` blocks | R1 per-entity history + 1d/5d/20d deltas |
| 27 | What-changed attribution | PARTIAL -> shipped | fortress/katlin changes | R1 contribution deltas per contributor |
| 28 | Confidence model separate from conviction | MISSING -> shipped | conviction "confidence band" only | R1 components published |
| 29 | Fusion coverage | MISSING -> shipped | fusion-v1 `coverage` (macro) | R1 expected families per entity type |
| 30 | Critical dependency system | PARTIAL -> shipped | contract-gate, katlin hard vetoes | R1 CRITICAL engines -> CAPITAL_DECISION_BLOCKED |
| 31 | Fusion ledger | MISSING -> shipped | -- | R1 `data/jh-fusion/ledger/` |
| 32 | Asymmetry engine | PARTIAL | fortress asymmetry, katlin plans, magnitude-distributions | R6 (never fabricate scenario probabilities) |
| 33-34 | Opportunity score / company vs trade quality | PARTIAL | why.html JH score, master-ranker | R6 |
| 35-36 | Portfolio fusion / stress | PARTIAL | basket VaR, stress ladder, proven-portfolio | R7 |
| 37-40 | Second-order discovery / cascade / narrative / crowding | PARTIAL | industry-boom, industry-rotation, narratives engines | R7+ |
| 41 | Sizing integration | EXISTS (untouched) | sizing-engine, position-sizer-v2, risk-gate | R7 after shadow comparison |
| 42 | AI CIO layer | PARTIAL | llm_router, grounded notes | R8 (consumes `data/jh-fusion.json`) |
| 43-45 | Command desk / screener / API | PARTIAL | home.js feed renderer reads any `data/*` feed | R2 (`data/jh-fusion.json` is already renderable; endpoints R2) |
| 46 | Database model | PARTIAL -> shipped | -- | R1 signals + engines; hypotheses/relationships R3 |
| 47 | Observability | PARTIAL -> shipped | sentry-lite, monitors | R1 `JustHodl/Fusion` metrics + structured logs |
| 48 | Tests | PARTIAL -> shipped | per-Lambda pytest pattern | R1 59 tests |
| 49 | Replay / backtest mode | PARTIAL -> shipped | engine backtests | R1 pure fusion core + archive; harness R5 |
| 50 | Feature flags | MISSING -> shipped | SSM used ad hoc | R1 `config/jh-fusion-flags.json` + SSM `/justhodl/fusion/flags` |
| 51 | Shadow mode | MISSING -> shipped | -- | R1 default ON |
| 52 | Security | EXISTS | shared role, secrets in SSM, no creds in code | unchanged |
| 53-54 | Cost control / loop protection | PARTIAL -> shipped | coordinator dedupe | R1 envelope depth cap, idempotent fusion trigger |

## Do-not-duplicate decisions
- `justhodl-conviction-engine`, `justhodl-sizing-engine`, `justhodl-engine-fusion`, `justhodl-risk-gate`
  stay authoritative. Fusion v1 runs beside them in shadow mode and reads them as inputs (risk-gate) or leaves
  them untouched (sizing).
- No new EventBridge rule, no new bus: the coordinator gains routes.
- No new database technology: DynamoDB (already used) for state, S3 under `data/` for everything a page reads.
