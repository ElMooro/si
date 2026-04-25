# Loop 1 readiness — post-legacy-cleanup verification

**Status:** success  
**Duration:** 2.7s  
**Finished:** 2026-04-25T20:56:30+00:00  

## Data

| n_correct | n_legacy | n_real_correct_none | n_total | n_wrong |
|---|---|---|---|---|
| 0 | 4410 | 0 | 4410 | 0 |

## Log
## A. justhodl-outcomes — counts by status + legacy

- `20:56:29`   Total outcomes: 4410
- `20:56:29`   ✅ correct=True:  0
- `20:56:29`   ❌ correct=False: 0
- `20:56:29`   ⏳ correct=None:  4410
- `20:56:29`      of which is_legacy=true:  4410 (with TTL: 4410)
- `20:56:29`      of which untagged:        0
- `20:56:29` ✅   ✅ All correct=None outcomes are legacy-tagged + TTL-scheduled
## B. SSM /justhodl/calibration/weights

- `20:56:29`   Weights stored: 25 entries
- `20:56:29`     btc_mvrv                       weight=0.700
- `20:56:29`     buffett_indicator              weight=0.750
- `20:56:29`     cape_ratio                     weight=0.750
- `20:56:29`     carry_risk                     weight=0.650
- `20:56:29`     cftc_bitcoin                   weight=0.750
- `20:56:29`     cftc_crude                     weight=0.700
- `20:56:29`     cftc_gold                      weight=0.800
- `20:56:29`     cftc_spx                       weight=0.800
- `20:56:29`     crypto_btc_signal              weight=0.700
- `20:56:29`     crypto_eth_signal              weight=0.650
- `20:56:29`     crypto_fear_greed              weight=0.550
- `20:56:29`     crypto_risk_score              weight=0.550
- `20:56:29`     edge_composite                 weight=0.700
- `20:56:29`     edge_regime                    weight=0.750
- `20:56:29`     khalid_index                   weight=1.000 ← default-1.0
- `20:56:29`     market_phase                   weight=0.750
- `20:56:29`     ml_risk                        weight=0.650
- `20:56:29`     momentum_gld                   weight=0.550
- `20:56:29`     momentum_spy                   weight=0.550
- `20:56:29`     momentum_uso                   weight=0.550
- `20:56:29`     plumbing_stress                weight=0.700
- `20:56:29`     screener_buy                   weight=0.650
- `20:56:29`     screener_sell                  weight=0.650
- `20:56:29`     screener_top_pick              weight=0.850
- `20:56:29`     valuation_composite            weight=0.800
## C. SSM /justhodl/calibration/accuracy

- `20:56:29`   Accuracy entries: 0
## D. reports/scorecard.json — badge state

- `20:56:30`   is_meaningful: False
- `20:56:30`   n_calibrated_signals: 0
- `20:56:30`   n_signals_with_outcomes: 2
- `20:56:30`   🟡 Badge YELLOW — awaiting data (correct state)
## E. Next scheduled runs

- `20:56:30`   Now: 2026-04-25T20:56:30.156532+00:00
- `20:56:30`   Next outcome-checker (Sun 08:00 UTC): 2026-04-26T08:00:00+00:00
- `20:56:30`   Next calibrator (Sun 09:00 UTC):     2026-04-26T09:00:00+00:00
- `20:56:30`   
- `20:56:30`   First outcomes from fixed signals:
- `20:56:30`     day_3 score:  ~2026-04-27 (covers signals from 2026-04-24)
- `20:56:30`     day_7 score:  ~2026-05-01-04 (the meaningful window)
- `20:56:30`   → ~2026-05-04 calibrator run = first meaningful weights
## VERDICT

- `20:56:30`   🟡 STILL WARMING UP — for the RIGHT reason now
- `20:56:30`   All correct=None records are legacy-tagged + TTL-scheduled
- `20:56:30`   Loop 1 will go LIVE as new signals get scored
- `20:56:30`   Earliest 🟢: 2026-05-04 (~9 days from now)
- `20:56:30` Done
