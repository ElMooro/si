# Verify enriched signals — rationale and magnitude populated

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-04-24T23:47:46+00:00  

## Data

| enriched_types_landing | fresh_signals | signal_types | types_with_any_rationale |
|---|---|---|---|
| 0 | 50 | 11 | 4 |

## Log
- `23:47:46`   Signals from last 10 min: 50
## Coverage by signal_type

- `23:47:46`   signal_type               count   rat   mag   tgt
- `23:47:46`   screener_top_pick            30     0     0     0
- `23:47:46`   khalid_index                  2     1     0     0
- `23:47:46`   carry_risk                    2     0     0     0
- `23:47:46`   edge_regime                   2     0     0     0
- `23:47:46`   crypto_fear_greed             2     1     0     0
- `23:47:46`   crypto_risk_score             2     0     0     0
- `23:47:46`   market_phase                  2     0     0     0
- `23:47:46`   ml_risk                       2     0     0     0
- `23:47:46`   momentum_uso                  2     1     1     1
- `23:47:46`   plumbing_stress               2     1     0     0
- `23:47:46`   edge_composite                2     0     0     0
## Sample rationales

- `23:47:46` 
  khalid_index:
- `23:47:46`     baseline:  713.94
- `23:47:46`     magnitude: None
- `23:47:46`     target:    None
- `23:47:46`     rationale: Khalid Index 43 = MODERATE (unknown regime)
- `23:47:46` 
  crypto_fear_greed:
- `23:47:46`     baseline:  77452
- `23:47:46`     magnitude: None
- `23:47:46`     target:    None
- `23:47:46`     rationale: Fear & Greed 39 (Fear) — contrarian NEUTRAL signal
- `23:47:46` 
  momentum_uso:
- `23:47:46`     baseline:  132.4
- `23:47:46`     magnitude: -1.72
- `23:47:46`     target:    130.12272
- `23:47:46`     rationale: USO momentum: -1.72% recent change → DOWN 1.7% over 1-7d
- `23:47:46` 
  plumbing_stress:
- `23:47:46`     baseline:  713.94
- `23:47:46`     magnitude: None
- `23:47:46`     target:    None
- `23:47:46`     rationale: Plumbing stress 25 = MODERATE (ELEVATED); red_flags=6
## Specifically expected to have rationale (the 7 enriched + variants)

- `23:47:46`   - momentum_gld                   (not in this batch — fires conditionally)
- `23:47:46`   - momentum_spy                   (not in this batch — fires conditionally)
- `23:47:46`   ⚠ momentum_uso                   1/2 have rationale
- `23:47:46`   ⚠ khalid_index                   1/2 have rationale
- `23:47:46`   ⚠ crypto_fear_greed              1/2 have rationale
- `23:47:46`   - btc_mvrv                       (not in this batch — fires conditionally)
- `23:47:46`   ⚠ plumbing_stress                1/2 have rationale
- `23:47:46`   - cape_ratio                     (not in this batch — fires conditionally)
- `23:47:46`   - buffett_indicator              (not in this batch — fires conditionally)
- `23:47:46` Done
