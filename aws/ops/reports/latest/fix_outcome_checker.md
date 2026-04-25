# Fix outcome-checker — skip signals without baseline_price

**Status:** success  
**Duration:** 31.8s  
**Finished:** 2026-04-25T09:42:19+00:00  

## Data

| signals_total | signals_with_baseline | signals_without_baseline | unscoreable_marked |
|---|---|---|---|
| 4829 | 2950 | 1879 | 318 |

## Log
## 1. Signal inventory

- `09:41:50`   Scanned 4829 signals
- `09:41:50`   Breakdown:
- `09:41:50`     status=pending baseline=yes                   2040
- `09:41:50`     status=partial baseline=yes                   910
- `09:41:50`     status=complete baseline=no                   865
- `09:41:50`     status=partial baseline=no                    696
- `09:41:50`     status=pending baseline=no                    318
- `09:41:50` 
  Signal types WITH baseline_price (post-fix, scoreable):
- `09:41:50`      2860  screener_top_pick
- `09:41:50`         9  edge_composite
- `09:41:50`         9  momentum_uso
- `09:41:50`         9  edge_regime
- `09:41:50`         9  khalid_index
- `09:41:50`         9  ml_risk
- `09:41:50`         9  market_phase
- `09:41:50`         9  crypto_risk_score
- `09:41:50`         9  carry_risk
- `09:41:50`         9  plumbing_stress
- `09:41:50`         9  crypto_fear_greed
- `09:41:50` 
  Signal types WITHOUT baseline_price (pre-fix, unscoreable):
- `09:41:50`       179  edge_regime
- `09:41:50`       179  market_phase
- `09:41:50`       179  edge_composite
- `09:41:50`       179  carry_risk
- `09:41:50`       179  khalid_index
- `09:41:50`       179  crypto_fear_greed
- `09:41:50`       179  ml_risk
- `09:41:50`       179  crypto_risk_score
- `09:41:50`       179  plumbing_stress
- `09:41:50`       111  momentum_uso
- `09:41:50`        75  momentum_gld
- `09:41:50`        51  momentum_spy
- `09:41:50`        28  momentum_tlt
- `09:41:50`         3  momentum_uup
## 2. Patch outcome-checker source

- `09:41:50` ✅   Patched lambda_function.py (+1034B)
- `09:41:50` ✅   Syntax OK
## 3. Re-deploy outcome-checker

- `09:41:54` ✅   Re-deployed (15144B)
## 4. Invoke outcome-checker (will mark no-baseline signals as unscoreable)

- `09:42:18` ✅   Invoked in 21.7s: processed=70
## 5. Post-fix status breakdown

- `09:42:19`   Total signals: 4829
- `09:42:19`   New distribution:
- `09:42:19`     status=pending baseline=yes                   2010
- `09:42:19`     status=partial baseline=yes                   940
- `09:42:19`     status=complete baseline=no                   889
- `09:42:19`     status=partial baseline=no                    672
- `09:42:19`     status=unscoreable baseline=no                318
- `09:42:19` Done
