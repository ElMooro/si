# ops 4325 -- the whole fleet, on the table

**Status:** success  
**Duration:** 794.7s  
**Finished:** 2026-08-03T18:34:44+00:00  

## Log
- `18:21:39` async sweep fired; polling for CHANGED artifact (prev generated_at=2026-07-07T22:54:49.092952+00:00)
- `18:34:44` ⚠ time-budget truncation: partial sweep (honest, disclosed)
- `18:34:44` ✅ SCANNED 7238 artifacts in 780.0s -- OK 1113 · WARN 6123 · FAIL 1 · parse-fail 0
- `18:34:44` by class: {"STALE": 6075, "FROZEN": 1, "ZERO_SCOPE": 503}
## worst offenders (non-stale first)

- `18:34:44` data/_preview/master-ranker.json [FAIL, 836h]
- `18:34:44`     STALE: artifact 836h old (limit 48h)
- `18:34:44`     FROZEN: engine touched 191h ago but artifact 836h old
- `18:34:44` data/archive/convergence-radar/20260601_1643.json [WARN, 1513h]
- `18:34:44`     STALE: artifact 1513h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 34 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_1650.json [WARN, 1513h]
- `18:34:44`     STALE: artifact 1513h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 34 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_1700.json [WARN, 1513h]
- `18:34:44`     STALE: artifact 1513h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 34 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_1719.json [WARN, 1513h]
- `18:34:44`     STALE: artifact 1513h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 48 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_1730.json [WARN, 1512h]
- `18:34:44`     STALE: artifact 1512h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 48 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_1800.json [WARN, 1512h]
- `18:34:44`     STALE: artifact 1512h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 48 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_1830.json [WARN, 1511h]
- `18:34:44`     STALE: artifact 1511h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 46 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_1900.json [WARN, 1511h]
- `18:34:44`     STALE: artifact 1511h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 48 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_1930.json [WARN, 1510h]
- `18:34:44`     STALE: artifact 1510h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 46 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_2000.json [WARN, 1510h]
- `18:34:44`     STALE: artifact 1510h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 46 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_2030.json [WARN, 1509h]
- `18:34:44`     STALE: artifact 1509h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 46 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_2100.json [WARN, 1509h]
- `18:34:44`     STALE: artifact 1509h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 46 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_2110.json [WARN, 1509h]
- `18:34:44`     STALE: artifact 1509h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 46 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_2130.json [WARN, 1508h]
- `18:34:44`     STALE: artifact 1508h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 46 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_2200.json [WARN, 1508h]
- `18:34:44`     STALE: artifact 1508h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 50 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_2230.json [WARN, 1507h]
- `18:34:44`     STALE: artifact 1507h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 46 rows — scope smell
- `18:34:44` data/archive/convergence-radar/20260601_2300.json [WARN, 1507h]
- `18:34:44`     STALE: artifact 1507h old (limit 336h)
- `18:34:44`     ZERO_SCOPE: n_short is 0 across all 46 rows — scope smell
## cross-artifact price contradictions (>12%)

- `18:34:44` ✅ OPS 4325 PASS -- the fleet now audits itself daily; today's findings are the work queue
