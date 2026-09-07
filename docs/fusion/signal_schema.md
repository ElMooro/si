# JHSIGNAL-1.0

Canonical file: `schemas/jhsignal-1.0.json` (JSON Schema draft-07). Native twin: `aws/shared/jhsignal.py`
(`validate`, `assert_valid`, `make_signal`). Both are exercised by
`aws/lambdas/justhodl-jhsignal-bridge/tests/test_jhsignal_and_adapters.py::TestSchema`, which cross-checks that every
tampered signal is rejected by both validators.

## Fields
| Field | Type / vocabulary | Notes |
|---|---|---|
| schema_version | `"JHSIGNAL-1.0"` | |
| signal_id | uuid4 | new per publication; `idempotency_key()` identifies the same fact republished |
| engine_id, engine_version | `^[a-z0-9][a-z0-9_]+$`, string | registry ids (`insider_radar`, not the Lambda name) |
| entity_id, entity_type, ticker | `type:SYMBOL`; type in equity/etf/crypto/commodity/fx/country/sector/industry/index/central_bank/market/theme/bond | entity_type must equal the id prefix |
| observed_at, data_asof, published_at | ISO-8601 UTC | data_asof <= observed_at <= published_at (5 min skew); no future timestamps |
| category | macro, liquidity, credit, risk, smart_money, institutional_flow, options_positioning, fundamental_growth, quality, valuation, price_confirmation, catalyst, sentiment, geopolitics, positioning, cycle | |
| signal_type | snake_case | must be declared in the registry for the engine |
| direction | strong_bearish, bearish, slightly_bearish, neutral, slightly_bullish, bullish, strong_bullish | derived from score: neutral < 0.15, slightly < 0.45, plain < 0.8, strong |
| direction_numeric, score | [-1, 1] | score is the normalised strength; sign must agree with direction |
| confidence | [0, 1] | measured breadth, never a constant pretending to be knowledge |
| magnitude, percentile | number/null, [0,100]/null | raw size (USD, %, gex bn) and universe percentile when the engine has one |
| horizon, horizon_min_days, horizon_max_days | TACTICAL 0-5, SWING 5-90, INTERMEDIATE 90-365, STRUCTURAL 365+ | |
| half_life_days, freshness_ttl_seconds | > 0, >= 60 | decay `exp(-ln2 * age/half_life)`; FRESH within TTL, STALE to 2x TTL, EXPIRED after |
| supports, affects[], dependencies | ids; `{entity_id, relationship, strength}` | affects is stored now, propagated in Release 3 |
| invalidation | `{type: text|level|state|time, description}` | required, human-readable |
| evidence[] | `{field, value, unit?, source?}` | the raw numbers the score came from |
| quality | source_reliability, data_completeness, calculation_quality in [0,1] | |
| metadata | object | producer, artifact, asof_basis (engine | s3_last_modified), confidence_basis, `veto` for RISK signals |

## Strictness
- Unknown fields, coerced types, out-of-range numbers, sign disagreements, unparseable or future timestamps are all
  rejected (`JHSignalError` lists every problem).
- `make_signal` derives the direction from the score, so the two cannot disagree; it refuses a missing score or
  confidence instead of defaulting them.
