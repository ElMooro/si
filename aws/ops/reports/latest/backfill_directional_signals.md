# 1) Scan all unscored outcomes (correct=None, non-legacy)

**Status:** success  
**Duration:** 71.8s  
**Finished:** 2026-05-04T22:10:38+00:00  

## Log
- `22:09:27`   total unscored: 420 (across 6 pages)
- `22:09:27`   unscored by signal_type:
- `22:09:27`     plumbing_stress                 n=55
- `22:09:27`     ml_risk                         n=55
- `22:09:27`     khalid_index                    n=55
- `22:09:27`     crypto_fear_greed               n=43
- `22:09:27`     crypto_risk_score               n=43
- `22:09:27`     edge_regime                     n=30
- `22:09:27`     market_phase                    n=30
- `22:09:27`     carry_risk                      n=30
- `22:09:27`     edge_composite                  n=25
- `22:09:27`     momentum_uso                    n=21
- `22:09:27`     momentum_gld                    n=17
- `22:09:27`     momentum_spy                    n=11
- `22:09:27`     momentum_tlt                    n=5
# 2) Pull source signals + group by ticker

- `22:09:27`   unique signal_ids: 404
- `22:09:28`   signals loaded: 404
- `22:09:28`   outcomes grouped by ticker:
- `22:09:28`     SPY              n=291
- `22:09:28`     BTC-USD          n=86
- `22:09:28`     USO              n=21
- `22:09:28`     GLD              n=17
- `22:09:28`     TLT              n=5
- `22:09:28`   skipped — no source signal: 0
- `22:09:28`   skipped — no measure_against ticker: 0
# 3) Fetch historical price maps per ticker

- `22:09:29`   SPY              84 bars  range=2026-01-02 → 2026-05-04
- `22:09:29`   BTC-USD          124 bars  range=2026-01-01 → 2026-05-04
- `22:09:30`   USO              84 bars  range=2026-01-02 → 2026-05-04
- `22:09:30`   GLD              84 bars  range=2026-01-02 → 2026-05-04
- `22:09:31`   TLT              84 bars  range=2026-01-02 → 2026-05-04
# 4) Backfill signals (baseline_price) + rescore outcomes

- `22:10:38` 
- `22:10:38`   ✓ signals patched (baseline_price set): 420
- `22:10:38`   ✓ outcomes rescored: 420
- `22:10:38`   ⚠ skipped (no close in range): 0
- `22:10:38`   ⚠ skipped (no check price): 0
- `22:10:38`   ℹ baselines already set: 0
# 5) Per-signal accuracy of backfilled outcomes

- `22:10:38`     carry_risk                      rescored=  30  correct= 30  wrong=  0  acc=100.0%
- `22:10:38`     crypto_fear_greed               rescored=  43  correct= 38  wrong=  5  acc= 88.4%
- `22:10:38`     crypto_risk_score               rescored=  43  correct=  5  wrong= 38  acc= 11.6%
- `22:10:38`     edge_composite                  rescored=  25  correct=  0  wrong= 25  acc=  0.0%
- `22:10:38`     edge_regime                     rescored=  30  correct=  0  wrong= 30  acc=  0.0%
- `22:10:38`     khalid_index                    rescored=  55  correct=  0  wrong= 55  acc=  0.0%
- `22:10:38`     market_phase                    rescored=  30  correct=  0  wrong= 30  acc=  0.0%
- `22:10:38`     ml_risk                         rescored=  55  correct= 55  wrong=  0  acc=100.0%
- `22:10:38`     momentum_gld                    rescored=  17  correct=  5  wrong= 12  acc= 29.4%
- `22:10:38`     momentum_spy                    rescored=  11  correct= 10  wrong=  1  acc= 90.9%
- `22:10:38`     momentum_tlt                    rescored=   5  correct=  0  wrong=  5  acc=  0.0%
- `22:10:38`     momentum_uso                    rescored=  21  correct= 13  wrong=  8  acc= 61.9%
- `22:10:38`     plumbing_stress                 rescored=  55  correct= 43  wrong= 12  acc= 78.2%
