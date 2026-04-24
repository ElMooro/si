# Verify newly-logged signals have baseline_price

**Status:** success  
**Duration:** 0.1s  
**Finished:** 2026-04-24T23:27:07+00:00  

## Data

| signal_types_present | types_with_full_coverage | types_with_no_baseline | types_with_partial |
|---|---|---|---|
| 11 | 11 | 0 | 0 |

## Log
- `23:27:07`   Filtering by logged_epoch >= 1777069627 (last 60 min)
- `23:27:07`   Found 25 fresh signals logged in last 60 min
## Coverage by signal_type

- `23:27:07`   signal_type                      count  has_bp  has_bench
- `23:27:07`   ✓ screener_top_pick                   15    15/15 (100%)   15
- `23:27:07`   ✓ market_phase                         1     1/1  (100%)    0
- `23:27:07`   ✓ ml_risk                              1     1/1  (100%)    0
- `23:27:07`   ✓ plumbing_stress                      1     1/1  (100%)    0
- `23:27:07`   ✓ crypto_risk_score                    1     1/1  (100%)    0
- `23:27:07`   ✓ edge_regime                          1     1/1  (100%)    0
- `23:27:07`   ✓ carry_risk                           1     1/1  (100%)    0
- `23:27:07`   ✓ crypto_fear_greed                    1     1/1  (100%)    0
- `23:27:07`   ✓ momentum_uso                         1     1/1  (100%)    0
- `23:27:07`   ✓ edge_composite                       1     1/1  (100%)    0
- `23:27:07`   ✓ khalid_index                         1     1/1  (100%)    0
## Sample signals (one per type)

- `23:27:07`   screener_top_pick: against=SATS pred=OUTPERFORM bp=116.9806 bench=SPY bbp=713.94
- `23:27:07`   market_phase: against=SPY pred=DOWN bp=713.94 bench=None bbp=None
- `23:27:07`   ml_risk: against=SPY pred=UP bp=713.94 bench=None bbp=None
- `23:27:07`   plumbing_stress: against=SPY pred=UP bp=713.94 bench=None bbp=None
- `23:27:07`   crypto_risk_score: against=BTC-USD pred=NEUTRAL bp=77380 bench=None bbp=None
- `23:27:07`   edge_regime: against=SPY pred=DOWN bp=713.94 bench=None bbp=None
- `23:27:07` ✅   ✅ Fix working — 11/11 signal types have full baseline coverage
- `23:27:07` Done
