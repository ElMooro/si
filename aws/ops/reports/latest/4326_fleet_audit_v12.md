# ops 4326 -- the whole fleet, on the table

**Status:** failure  
**Duration:** 793.2s  
**Finished:** 2026-08-03T19:24:34+00:00  

## Error

```
SystemExit: 1
```

## Log
- `19:11:21` async sweep fired; polling for CHANGED artifact (prev generated_at=2026-08-03T18:34:40.705506+00:00)
- `19:24:34` archives age-exempt: 37327
- `19:24:34` ✅ SCANNED 7697 artifacts in 780.0s -- OK 4603 · WARN 3075 · FAIL 13 · parse-fail 1
- `19:24:34` by class: {"STALE": 3080, "ZERO_SCOPE": 6, "FROZEN": 10, "UNITS": 4, "PARSE": 1}
## worst offenders (non-stale first)

- `19:24:34` data/credit-stress.json [FAIL, 69h]
- `19:24:34`     STALE: artifact 69h old (limit 48h)
- `19:24:34`     FROZEN: engine touched 22h ago but artifact 69h old
- `19:24:34`     UNITS: 2 negative prices
- `19:24:34` data/bond-trace.json [FAIL, 69h]
- `19:24:34`     STALE: artifact 69h old (limit 48h)
- `19:24:34`     FROZEN: engine touched 22h ago but artifact 69h old
- `19:24:34` data/crisis-knowledge-base.json [FAIL, 61h]
- `19:24:34`     STALE: artifact 61h old (limit 48h)
- `19:24:34`     FROZEN: engine touched 22h ago but artifact 61h old
- `19:24:34` data/cross-asset-rv.json [FAIL, 68h]
- `19:24:34`     STALE: artifact 68h old (limit 48h)
- `19:24:34`     FROZEN: engine touched 22h ago but artifact 68h old
- `19:24:34` data/event-study.json [FAIL, 56h]
- `19:24:34`     STALE: artifact 56h old (limit 48h)
- `19:24:34`     FROZEN: engine touched 22h ago but artifact 56h old
- `19:24:34` data/global-macro.json [FAIL, 69h]
- `19:24:34`     STALE: artifact 69h old (limit 48h)
- `19:24:34`     FROZEN: engine touched 22h ago but artifact 69h old
- `19:24:34` data/historical-analogs.json [FAIL, 56h]
- `19:24:34`     STALE: artifact 56h old (limit 48h)
- `19:24:34`     FROZEN: engine touched 22h ago but artifact 56h old
- `19:24:34` data/implied-prob.json [FAIL, 73h]
- `19:24:34`     STALE: artifact 73h old (limit 48h)
- `19:24:34`     FROZEN: engine touched 22h ago but artifact 73h old
- `19:24:34` data/insider-clusters.json [WARN, 56h]
- `19:24:34`     STALE: artifact 56h old (limit 48h)
- `19:24:34`     ZERO_SCOPE: shares_outstanding is 0 across all 19 rows — scope smell
- `19:24:34` data/interpretations/yield-curve.json [FAIL, 481h]
- `19:24:34`     STALE: artifact 481h old (limit 48h)
- `19:24:34`     FROZEN: engine touched 22h ago but artifact 481h old
- `19:24:34` data/liquidity-flow.json [FAIL, 69h]
- `19:24:34`     STALE: artifact 69h old (limit 48h)
- `19:24:34`     FROZEN: engine touched 22h ago but artifact 69h old
- `19:24:34` data/ai-rerating-radar.json [WARN, 4h]
- `19:24:34`     ZERO_SCOPE: short_squeeze is 0 across all 227 rows — scope smell
- `19:24:34` data/best-setups.json [WARN, 0h]
- `19:24:34`     ZERO_SCOPE: buildout_threat is 0 across all 90 rows — scope smell
- `19:24:34` data/crisis-composite.json [FAIL, 0h]
- `19:24:34`     UNITS: 1 negative prices
- `19:24:34` data/llm-cost.json [WARN, 0h]
- `19:24:34`     ZERO_SCOPE: out_tok is 0 across all 15 rows — scope smell
- `19:24:34` data/macro-nowcast.json [FAIL, 0h]
- `19:24:34`     UNITS: 9 negative prices
- `19:24:34` data/magic-formula.json [FAIL, 20h]
- `19:24:34`     UNITS: rank_earnings_yield: 129/210 outside [-5.0,60.0], e.g. 144
- `19:24:34` data/opportunities.json [WARN, 5h]
- `19:24:34`     ZERO_SCOPE: expected_to_outgrow_industry is 0 across all 192 rows — scope smell
## cross-artifact price contradictions (>12%)

- `19:24:34` ✗   truncated even after archive exemption
