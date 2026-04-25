# Verify IBIT populated by natural cron + re-run divergence

**Status:** success  
**Duration:** 26.8s  
**Finished:** 2026-04-25T18:57:03+00:00  

## Data

| btc_pairs_fixed | crypto_etfs_populated | etha_bars | gbtc_bars | ibit_bars | n_div_extreme | n_div_processed |
|---|---|---|---|---|---|---|
| 2 | 5/5 | 120 | 120 | 120 | 3 | 12 |

## Log
## A. IBIT/GBTC/ETHA in data/report.json

- `18:56:36`   data/report.json: 1,750,808B, age 1.5min
- `18:56:36`   Total stocks: 193
- `18:56:36` ✅     IBIT   120 bars, latest_close=$44.02
- `18:56:36` ✅     GBTC   120 bars, latest_close=$60.37
- `18:56:36` ✅     ETHA   120 bars, latest_close=$17.52
- `18:56:36` ✅     FBTC   120 bars, latest_close=$67.61
- `18:56:36` ✅     ARKB   120 bars, latest_close=$25.77
## B. Re-invoke divergence scanner (async, no rate limit risk)

- `18:56:37` ✅   Async invoked (StatusCode=202)
- `18:56:37`   Waiting 25s for scanner to complete...
## C. Verify BTC pairs in divergence/current.json

- `18:57:02`   divergence/current.json age: 0.4min
- `18:57:02` ✅     Gold vs BTC               z=-0.58  R²=0.01
- `18:57:02`       → GLD appears CHEAP vs IBIT
- `18:57:02` ✅     BTC vs Nasdaq             z=-0.37  R²=0.45
- `18:57:02`       → IBIT appears CHEAP vs QQQ
- `18:57:02` 
  Total processed: 12/12  At >2σ extreme: 3
- `18:57:02` 
  Top 5 divergences today:
- `18:57:02`     Nasdaq vs 10Y Yield            z=+2.73 ← EXTREME
- `18:57:02`     TIP vs IEF                     z=+2.71 ← EXTREME
- `18:57:02`     XLV vs SPY                     z=-2.52 ← EXTREME
- `18:57:02`     Small Caps vs 2s10s Curve      z=+1.49
- `18:57:02`     XLP vs XLY (defensive/cyclical) z=-1.02
## D. Confirm morning-intelligence has bond_regime in extract_metrics

- `18:57:02`   morning-intelligence: sha=rJ+PMLAVXeixpbnU... last_modified=2026-04-25T18:45:06
## E. Risk-sizer still producing differentiated sizes

- `18:57:03`   Sizes: 3.39% — 5.15%, spread 1.76%
- `18:57:03` ✅   ✅ Differentiation persistent
- `18:57:03` Done
