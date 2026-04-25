# Loop 1 readiness — is calibration meaningful yet?

**Status:** success  
**Duration:** 2.9s  
**Finished:** 2026-04-25T19:03:41+00:00  

## Data

| outcomes_correct | outcomes_unscored | outcomes_wrong | signals_at_30_plus | weights_diverged |
|---|---|---|---|---|
| 0 | 4377 | 0 | 0 | 11 |

## Log
## A. justhodl-outcomes — count by status

- `19:03:40`   Total outcomes: 4377
- `19:03:40`   ✅ correct=True:  0
- `19:03:40`   ❌ correct=False: 0
- `19:03:40`   ⏳ correct=None:  4377 (waiting for day_7 check)
- `19:03:40` 
  By signal_type (15 types):
- `19:03:40`     momentum_tlt                   scored=0/78 (none yet)
- `19:03:40`     khalid_index                   scored=0/334 (none yet)
- `19:03:40`     crypto_fear_greed              scored=0/442 (none yet)
- `19:03:40`     crypto_risk_score              scored=0/442 (none yet)
- `19:03:40`     ml_risk                        scored=0/334 (none yet)
- `19:03:40`     plumbing_stress                scored=0/334 (none yet)
- `19:03:40`     momentum_gld                   scored=0/201 (none yet)
- `19:03:40`     edge_composite                 scored=0/274 (none yet)
- `19:03:40`     market_phase                   scored=0/182 (none yet)
- `19:03:40`     screener_top_pick              scored=0/940 (none yet)
- `19:03:40`     carry_risk                     scored=0/182 (none yet)
- `19:03:40`     momentum_spy                   scored=0/141 (none yet)
- `19:03:40`     edge_regime                    scored=0/182 (none yet)
- `19:03:40`     momentum_uup                   scored=0/9 (none yet)
- `19:03:40`     momentum_uso                   scored=0/302 (none yet)
- `19:03:40` 
  Still warming up — no signal type has ≥30 scored outcomes yet
## B. SSM /justhodl/calibration/weights

- `19:03:40`   Weights stored: 12 entries
- `19:03:40`   Weights at 1.0 (default): 1
- `19:03:40`   Weights diverged from 1.0: 11
- `19:03:40` ✅ 
  ✅ Calibrator is producing real weights:
- `19:03:40`     cftc_bitcoin                   weight=0.750
- `19:03:40`     cftc_crude                     weight=0.700
- `19:03:40`     cftc_gold                      weight=0.800
- `19:03:40`     cftc_spx                       weight=0.800
- `19:03:40`     crypto_btc_signal              weight=0.700
- `19:03:40`     crypto_eth_signal              weight=0.650
- `19:03:40`     crypto_fear_greed              weight=0.310
- `19:03:40`     crypto_risk_score              weight=0.310
- `19:03:40`     edge_regime                    weight=0.750
- `19:03:40`     screener_top_pick              weight=0.850
- `19:03:40`     valuation_composite            weight=0.800
## C. SSM /justhodl/calibration/accuracy

- `19:03:40`   Accuracy data: 2 entries
- `19:03:40`     crypto_fear_greed              {'accuracy': 0.0, 'n': 369, 'avg_return': None}
- `19:03:40`     crypto_risk_score              {'accuracy': 0.0, 'n': 369, 'avg_return': None}
## D. intelligence-report.json — is_meaningful flag

- `19:03:40`   is_meaningful: False
- `19:03:40`   n_signals: 4
- `19:03:40`   calibrated_composite: 28.25
- `19:03:40`   raw_composite: 28.25
- `19:03:40`   Still standby (uniform weights)
## E. reports/scorecard.json — what does the badge show?

- `19:03:41`   is_meaningful: False
- `19:03:41`   n_calibrated_signals: 0
- `19:03:41`   n_signals_with_outcomes: 2
- `19:03:41`   🟡 Badge would render YELLOW — awaiting data
## F. learning/improvement_log.json (Loop 3 prompt iterator)

- `19:03:41`   Total log entries: 1
- `19:03:41`     skip_no_data                   1
- `19:03:41`   Iterator running, awaiting scored data
## G. portfolio/pnl-history.json — Loop 2 PnL accumulation

- `19:03:41`   Snapshots: 1
- `19:03:41`   First: 2026-04-25 bh=0.0% khalid=0.0%
- `19:03:41`   Last:  2026-04-25 bh=0.0% khalid=0.0%
## H. investor-debate/_index.json — Loop 4 watchlist debate

- `19:03:41`   No debate output yet — first run is nightly 03:00 UTC
## VERDICT — Loop 1 status

- `19:03:41`   🟡 STILL WARMING UP
- `19:03:41`   Need ≥30 scored outcomes per signal type
- `19:03:41`   Calibrator runs Sundays 9:00 UTC; outcome-checker Sun 8:00 UTC
- `19:03:41`   Earliest signal logged 2026-03-12 (44 days ago) — eligible
- `19:03:41`   for day_7 scoring. Next calibrator run will progress the count.
- `19:03:41` Done
