# Audit baseline_price coverage in justhodl-signals

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-04-24T23:23:17+00:00  

## Data

| coverage_pct | total_sampled | with_baseline |
|---|---|---|
| 53.8 | 500 | 269 |

## Log
## A. Sample 500 signals, count baseline_price coverage

- `23:23:17`   Scanned 500 signal records
- `23:23:17` 
  Signal types — baseline_price coverage:
- `23:23:17`     ✓ screener_top_pick              269/269 (100%)
- `23:23:17`     ✗ carry_risk                     0/29 (0%)
- `23:23:17`     ✗ edge_regime                    0/25 (0%)
- `23:23:17`     ✗ plumbing_stress                0/25 (0%)
- `23:23:17`     ✗ ml_risk                        0/24 (0%)
- `23:23:17`     ✗ market_phase                   0/20 (0%)
- `23:23:17`     ✗ edge_composite                 0/20 (0%)
- `23:23:17`     ✗ crypto_risk_score              0/19 (0%)
- `23:23:17`     ✗ khalid_index                   0/18 (0%)
- `23:23:17`     ✗ crypto_fear_greed              0/18 (0%)
- `23:23:17`     ✗ momentum_uso                   0/18 (0%)
- `23:23:17`     ✗ momentum_gld                   0/12 (0%)
- `23:23:17`     ✗ momentum_spy                   0/3 (0%)
- `23:23:17` 
  Sample WITH baseline:
- `23:23:17`     screener_top_pick: bp=128.59, against=SATS
- `23:23:17`     screener_top_pick: bp=243.29, against=COHR
- `23:23:17`     screener_top_pick: bp=297.35, against=CVNA
- `23:23:17` 
  Sample WITHOUT baseline:
- `23:23:17`     edge_regime: against=SPY, predicted=NEUTRAL
- `23:23:17`     market_phase: against=SPY, predicted=DOWN
- `23:23:17`     edge_composite: against=SPY, predicted=NEUTRAL
- `23:23:17` 
  Overall: 269/500 (54%) signals have baseline_price
- `23:23:17` ✅   Coverage decent — different bug
- `23:23:17` Done
