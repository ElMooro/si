# Where is baseline_price getting stripped?

**Status:** success  
**Duration:** 3.0s  
**Finished:** 2026-04-25T19:10:33+00:00  

## Data

| n_signals | n_types | strip_evidence_types |
|---|---|---|
| 4854 | 15 | 11 |

## Log
## 1. Scan all signals, group by (type, status), check baseline

- `19:10:32`   Total signals scanned: 4854
- `19:10:32` 
  baseline_price coverage by (type, status):

- `19:10:32`     signal_type                    status           n   with_bp     %
- `19:10:32`     carry_risk                     complete        60         0    0% ⚠
- `19:10:32`     carry_risk                     partial         62         0    0% ⚠
- `19:10:32`     carry_risk                     pending         10        10  100%
- `19:10:32`     carry_risk                     unscoreable     57         0    0% ⚠
- `19:10:32`     crypto_fear_greed              complete       124         0    0% ⚠
- `19:10:32`     crypto_fear_greed              partial         42         0    0% ⚠
- `19:10:32`     crypto_fear_greed              pending         10        10  100%
- `19:10:32`     crypto_fear_greed              unscoreable     13         0    0% ⚠
- `19:10:32`     crypto_risk_score              complete       124         0    0% ⚠
- `19:10:32`     crypto_risk_score              partial         42         0    0% ⚠
- `19:10:32`     crypto_risk_score              pending         10        10  100%
- `19:10:32`     crypto_risk_score              unscoreable     13         0    0% ⚠
- `19:10:32`     edge_composite                 complete       124         0    0% ⚠
- `19:10:32`     edge_composite                 partial         26         0    0% ⚠
- `19:10:32`     edge_composite                 pending         10        10  100%
- `19:10:32`     edge_composite                 unscoreable     29         0    0% ⚠
- `19:10:32`     edge_regime                    complete        60         0    0% ⚠
- `19:10:32`     edge_regime                    partial         62         0    0% ⚠
- `19:10:32`     edge_regime                    pending         10        10  100%
- `19:10:32`     edge_regime                    unscoreable     57         0    0% ⚠
- `19:10:32`     khalid_index                   complete        60         0    0% ⚠
- `19:10:32`     khalid_index                   partial         90         0    0% ⚠
- `19:10:32`     khalid_index                   pending         10        10  100%
- `19:10:32`     khalid_index                   unscoreable     29         0    0% ⚠
- `19:10:32`     market_phase                   partial        122         0    0% ⚠
- `19:10:32`     market_phase                   pending         10        10  100%
- `19:10:32`     market_phase                   unscoreable     57         0    0% ⚠
- `19:10:32`     ml_risk                        complete        60         0    0% ⚠
- `19:10:32`     ml_risk                        partial         90         0    0% ⚠
- `19:10:32`     ml_risk                        pending         10        10  100%
- `19:10:32`     ml_risk                        unscoreable     29         0    0% ⚠
- `19:10:32`     momentum_gld                   complete        59         0    0% ⚠
- `19:10:32`     momentum_gld                   partial         14         0    0% ⚠
- `19:10:32`     momentum_gld                   unscoreable      2         0    0% ⚠
- `19:10:32`     momentum_spy                   complete        42         0    0% ⚠
- `19:10:32`     momentum_spy                   partial          9         0    0% ⚠
- `19:10:32`     momentum_tlt                   complete        22         0    0% ⚠
- `19:10:32`     momentum_tlt                   partial          6         0    0% ⚠
- `19:10:32`     momentum_uso                   complete        91         0    0% ⚠
- `19:10:32`     momentum_uso                   partial         17         0    0% ⚠
- `19:10:32`     momentum_uso                   pending         10        10  100%
- `19:10:32`     momentum_uso                   unscoreable      3         0    0% ⚠
- `19:10:32`     momentum_uup                   complete         3         0    0% ⚠
- `19:10:32`     plumbing_stress                complete        60         0    0% ⚠
- `19:10:32`     plumbing_stress                partial         90         0    0% ⚠
- `19:10:32`     plumbing_stress                pending         10        10  100%
- `19:10:32`     plumbing_stress                unscoreable     29         0    0% ⚠
- `19:10:32`     screener_top_pick              partial        940       940  100%
- `19:10:32`     screener_top_pick              pending       1935      1935  100%
## 2. Verdict — where does baseline_price get lost?

- `19:10:32`   carry_risk                     pending= 100% partial=   0% complete=   0%
- `19:10:32`   crypto_fear_greed              pending= 100% partial=   0% complete=   0%
- `19:10:32`   crypto_risk_score              pending= 100% partial=   0% complete=   0%
- `19:10:32`   edge_composite                 pending= 100% partial=   0% complete=   0%
- `19:10:32`   edge_regime                    pending= 100% partial=   0% complete=   0%
- `19:10:32`   khalid_index                   pending= 100% partial=   0% complete=   0%
- `19:10:32`   market_phase                   pending= 100% partial=   0% complete=   0%
- `19:10:32`   ml_risk                        pending= 100% partial=   0% complete=   0%
- `19:10:32`   momentum_gld                   pending=   0% partial=   0% complete=   0%
- `19:10:32`   momentum_spy                   pending=   0% partial=   0% complete=   0%
- `19:10:32`   momentum_tlt                   pending=   0% partial=   0% complete=   0%
- `19:10:32`   momentum_uso                   pending= 100% partial=   0% complete=   0%
- `19:10:32`   momentum_uup                   pending=   0% partial=   0% complete=   0%
- `19:10:32`   plumbing_stress                pending= 100% partial=   0% complete=   0%
- `19:10:32`   screener_top_pick              pending= 100% partial= 100% complete=   0%
- `19:10:32` ⚠ 
  ⚠ 11 types where pending has baseline but complete doesn\'t:
- `19:10:32` ⚠     carry_risk: pending 100%, complete 0%
- `19:10:32` ⚠     crypto_fear_greed: pending 100%, complete 0%
- `19:10:32` ⚠     crypto_risk_score: pending 100%, complete 0%
- `19:10:32` ⚠     edge_composite: pending 100%, complete 0%
- `19:10:32` ⚠     edge_regime: pending 100%, complete 0%
- `19:10:32` ⚠     khalid_index: pending 100%, complete 0%
- `19:10:32` ⚠     market_phase: pending 100%, complete 0%
- `19:10:32` ⚠     ml_risk: pending 100%, complete 0%
- `19:10:32` ⚠     momentum_uso: pending 100%, complete 0%
- `19:10:32` ⚠     plumbing_stress: pending 100%, complete 0%
- `19:10:32` ⚠     screener_top_pick: pending 100%, complete 0%
## 3. Walk through 3 signals of same type at 3 statuses

- `19:10:32`   Type: carry_risk

- `19:10:32`   ── status=pending (newest of 10) ──
- `19:10:32`     signal_id            = a7ae5d26-e9cf-4eec-b023-501a5dad695e
- `19:10:32`     captured_at          = None
- `19:10:32`     baseline_price       = 713.94
- `19:10:32`     predicted_direction  = UP
- `19:10:32`     ticker               = None
- `19:10:32`     measure_against      = SPY
- `19:10:32`     outcomes             0 entries: []
- `19:10:32`     status               = pending
- `19:10:32`   ── status=partial (newest of 62) ──
- `19:10:32`     signal_id            = bad918b9-0fc9-4df9-b147-511ee2615d13
- `19:10:32`     captured_at          = None
- `19:10:32`     baseline_price       = None
- `19:10:32`     predicted_direction  = UP
- `19:10:32`     ticker               = None
- `19:10:32`     measure_against      = SPY
- `19:10:32`     outcomes             1 entries: ['day_14']
- `19:10:32`     status               = partial
- `19:10:32`   ── status=complete (newest of 60) ──
- `19:10:32`     signal_id            = a3a1eb2f-4234-427e-8bc5-7d7793bfa649
- `19:10:32`     captured_at          = None
- `19:10:32`     baseline_price       = None
- `19:10:32`     predicted_direction  = UP
- `19:10:32`     ticker               = None
- `19:10:32`     measure_against      = SPY
- `19:10:32`     outcomes             2 entries: ['day_14', 'day_30']
- `19:10:32`     status               = complete
## 4. One outcome with correct=None — what signal_id?

- `19:10:32` 
  outcome: fdfa64fe-acef-44f9-809b-be177bbd27e8_day
- `19:10:32`     correct            = None
- `19:10:32`     predicted_dir      = DOWN
- `19:10:32`     outcome            = {'price_at_check': 86.71, 'actual_direction': 'UNKNOWN', 'return_pct': 0.0, 'che
- `19:10:32`     checked_at         = 2026-04-24T23:17:41.907006+00:00
- `19:10:32`     signal_value       = -1.71%
- `19:10:32`   source signal fdfa64fe-acef-44f9-809b-be177b:
- `19:10:32`     baseline_price = None
- `19:10:32`     captured_at    = None
- `19:10:32`     status         = complete
- `19:10:32` 
  outcome: c2447ef6-3f05-43ee-9ebc-3dc49ad425ab_day
- `19:10:32`     correct            = None
- `19:10:32`     predicted_dir      = NEUTRAL
- `19:10:32`     outcome            = {'price_at_check': 713.94, 'actual_direction': 'UNKNOWN', 'return_pct': 0.0, 'ch
- `19:10:32`     checked_at         = 2026-04-24T23:17:41.907006+00:00
- `19:10:32`     signal_value       = MODERATE
- `19:10:33`   source signal c2447ef6-3f05-43ee-9ebc-3dc49a:
- `19:10:33`     baseline_price = None
- `19:10:33`     captured_at    = None
- `19:10:33`     status         = complete
- `19:10:33` 
  outcome: 035715a5-ba9e-44ad-8ebd-aa9ab0d6e6b9_day
- `19:10:33`     correct            = None
- `19:10:33`     predicted_dir      = UP
- `19:10:33`     outcome            = {'price_at_check': 77397.8, 'actual_direction': 'UNKNOWN', 'return_pct': 0.0, 'c
- `19:10:33`     checked_at         = 2026-04-24T23:10:19.571920+00:00
- `19:10:33`     signal_value       = FEAR
- `19:10:33`   source signal 035715a5-ba9e-44ad-8ebd-aa9ab0:
- `19:10:33`     baseline_price = None
- `19:10:33`     captured_at    = None
- `19:10:33`     status         = partial
- `19:10:33` Done
