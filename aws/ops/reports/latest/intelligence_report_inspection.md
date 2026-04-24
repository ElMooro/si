# intelligence-report.json — degraded or fine?

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-04-24T23:58:16+00:00  

## Data

| age_min | ml_fields_empty | ml_fields_populated | size_kb |
|---|---|---|---|
| 52.6 | 10 | 0 | 2.7 |

## Log
- `23:58:16`   Size: 2,785 bytes (52.6 min old)
- `23:58:16`   Top-level keys: ['action_required', 'data_sources', 'dxy', 'forecast', 'generated_at', 'headline', 'headline_detail', 'metrics_table', 'ml_intelligence', 'phase', 'phase_color', 'plumbing_flags', 'portfolio', 'regime', 'risks', 'scores', 'signals', 'stock_signals', 'swap_spreads', 'timestamp', 'version', 'yield_curve']
- `23:58:16` 
## Fields that come from predictions.json via justhodl-intelligence

- `23:58:16`   ✗ executive_summary         EMPTY/MISSING — Strategic narrative from ML
- `23:58:16`   ✗ ml_liquidity              EMPTY/MISSING — ML's liquidity analysis
- `23:58:16`   ✗ ml_risk                   EMPTY/MISSING — ML's risk decomposition
- `23:58:16`   ✗ carry_trade               EMPTY/MISSING — ML's carry analysis
- `23:58:16`   ✗ sector_rotation           EMPTY/MISSING — ML's sector picks
- `23:58:16`   ✗ trade_recommendations     EMPTY/MISSING — ML's trade ideas
- `23:58:16`   ✗ market_snapshot           EMPTY/MISSING — ML market overview
- `23:58:16`   ✗ us_equities               EMPTY/MISSING — ML US equity outlook
- `23:58:16`   ✗ global_markets            EMPTY/MISSING — ML global outlook
- `23:58:16`   ✗ agents_online             EMPTY/MISSING — ML agent count
## Fields signal-logger extracts (ml_risk, carry_risk)

- `23:58:16`   data.scores: {
  "khalid_index": 0,
  "crisis_distance": 60,
  "plumbing_stress": 0,
  "ml_risk_score": 0,
  "carry_risk_score": 0,
  "vix": null,
  "move": null
}
- `23:58:16` 
  data.phase: 'PRE-CRISIS' (signal-logger uses this for market_phase signal)
## Summary

- `23:58:16`   ML-dependent fields populated: 0/10
- `23:58:16`   ML-dependent fields empty:     10/10
- `23:58:16` 
- `23:58:16` ⚠   ⚠ MOST ML fields are empty — predictions.json staleness IS impacting
- `23:58:16` ⚠     intelligence-report.json. Retire is the wrong call. Need to fix the
- `23:58:16` ⚠     pipeline.
- `23:58:16` Done
