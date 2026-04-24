# Verify schema v2 on fresh signals + backwards compat

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-04-24T23:43:51+00:00  

## Data

| fresh_signals | regime_capture_pct | v2_coverage_pct |
|---|---|---|
| 25 | 100.0 | 100.0 |

## Log
## A. Schema v2 fields on signals from last 10 min

- `23:43:51`   Fresh signals scanned: 25
- `23:43:51`   schema_version='2':     25/25 (100%)
- `23:43:51`   horizon_days_primary:   25/25
- `23:43:51`   regime_at_log:          25/25
- `23:43:51`   khalid_score_at_log:    25/25
- `23:43:51`   baseline_price:         25/25 (carryover from prev fix)
- `23:43:51` 
  Optional fields (expected mostly None for now):
- `23:43:51`     predicted_magnitude_pct: 0/25
- `23:43:51`     predicted_target_price:  0/25
- `23:43:51`     rationale:               0/25
- `23:43:51`     supporting_signals:      0/25
- `23:43:51` ✅   Schema v2 landing on 100% of new signals
## B. Sample 3 fresh signals to inspect shape

- `23:43:51` 
  signal_type=screener_top_pick:
- `23:43:51`     schema_version                 = 2
- `23:43:51`     predicted_direction            = OUTPERFORM
- `23:43:51`     baseline_price                 = 450.3187
- `23:43:51`     horizon_days_primary           = 90
- `23:43:51`     regime_at_log                  = BEAR
- `23:43:51`     khalid_score_at_log            = 43
- `23:43:51`     predicted_magnitude_pct        = None
- `23:43:51`     predicted_target_price         = None
- `23:43:51` 
  signal_type=screener_top_pick:
- `23:43:51`     schema_version                 = 2
- `23:43:51`     predicted_direction            = OUTPERFORM
- `23:43:51`     baseline_price                 = 520.59
- `23:43:51`     horizon_days_primary           = 90
- `23:43:51`     regime_at_log                  = BEAR
- `23:43:51`     khalid_score_at_log            = 43
- `23:43:51`     predicted_magnitude_pct        = None
- `23:43:51`     predicted_target_price         = None
- `23:43:51` 
  signal_type=khalid_index:
- `23:43:51`     schema_version                 = 2
- `23:43:51`     predicted_direction            = NEUTRAL
- `23:43:51`     baseline_price                 = 713.94
- `23:43:51`     horizon_days_primary           = 30
- `23:43:51`     regime_at_log                  = BEAR
- `23:43:51`     khalid_score_at_log            = 43
- `23:43:51`     predicted_magnitude_pct        = None
- `23:43:51`     predicted_target_price         = None
## C. Old signals (24h+ ago) still readable — backwards compat

- `23:43:51`   Old signals (24-48h ago, sampled): 0
- `23:43:51`   Implicit v1 (no schema_version): 0
- `23:43:51`   Explicit v2:                     0
- `23:43:51` Done
