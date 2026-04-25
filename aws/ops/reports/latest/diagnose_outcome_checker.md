# Diagnose outcome-checker — why are 4,307 outcomes unscored?

**Status:** success  
**Duration:** 3.5s  
**Finished:** 2026-04-25T02:50:42+00:00  

## Data

| last_7d_errors | last_7d_invocations | outcomes_scored | outcomes_total | outcomes_unscored |
|---|---|---|---|---|
| 0 | 3 | 0 | 4307 | 4307 |

## Log
## 1. EventBridge rules for outcome-checker

- `02:50:39`   Rules targeting justhodl-outcome-checker: 3
- `02:50:39`     justhodl-outcome-checker-daily                     state=ENABLED    schedule=cron(30 22 ? * MON-FRI *)
- `02:50:39`     justhodl-outcome-checker-monthly                   state=ENABLED    schedule=cron(0 8 1 * ? *)
- `02:50:39`     justhodl-outcome-checker-weekly                    state=ENABLED    schedule=cron(0 8 ? * SUN *)
## 2. CloudWatch metrics (last 7 days)

- `02:50:40`   Last 7d: 3 invocations, 0 errors
- `02:50:40`   Daily breakdown:
- `02:50:40`     2026-04-19: inv=1 err=0
- `02:50:40`     2026-04-24: inv=2 err=0
## 3. Most recent CloudWatch log stream

- `02:50:40`   Stream: 2026/04/24/[$LATEST]a680555be7b64d078b5d3030698e91a7
- `02:50:40`   Last event: 2026-04-24 23:19:37.354000+00:00
- `02:50:40`   Last 40 log lines:
- `02:50:40`     [CHECKER] momentum_uso [day_3] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] momentum_uso [day_7] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `02:50:40`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `02:50:40`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `02:50:40`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `02:50:40`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `02:50:40`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `02:50:40`     [CHECKER] plumbing_stress [day_14] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] plumbing_stress [day_7] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] khalid_index [day_14] → ❌ WRONG (predicted NEUTRAL, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] khalid_index [day_7] → ❌ WRONG (predicted NEUTRAL, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `02:50:40`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `02:50:40`     [CHECKER] edge_regime [day_14] → ❌ WRONG (predicted NEUTRAL, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] carry_risk [day_14] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] carry_risk [day_30] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] market_phase [day_14] → ❌ WRONG (predicted DOWN, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] market_phase [day_30] → ❌ WRONG (predicted DOWN, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] carry_risk [day_14] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] carry_risk [day_30] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] momentum_tlt [day_1] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] momentum_tlt [day_3] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] momentum_tlt [day_7] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `02:50:40`     [CHECKER] market_phase [day_14] → ❌ WRONG (predicted DOWN, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] market_phase [day_30] → ❌ WRONG (predicted DOWN, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] market_phase [day_14] → ❌ WRONG (predicted DOWN, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] edge_composite [day_14] → ❌ WRONG (predicted NEUTRAL, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] edge_composite [day_7] → ❌ WRONG (predicted NEUTRAL, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] carry_risk [day_14] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] carry_risk [day_30] → ❌ WRONG (predicted UP, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] edge_regime [day_14] → ❌ WRONG (predicted NEUTRAL, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] edge_regime [day_30] → ❌ WRONG (predicted NEUTRAL, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] screener_top_pick [day_30] → ❌ WRONG (predicted OUTPERFORM, got  0.00%)
- `02:50:40`     [CHECKER] plumbing_stress [day_14] → ❌ WRONG (predicted NEUTRAL, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] plumbing_stress [day_7] → ❌ WRONG (predicted NEUTRAL, got UNKNOWN 0.00%)
- `02:50:40`     [CHECKER] Processed 2139 signals
- `02:50:40`     END RequestId: 10741f83-e686-41dc-b680-ebd2c281c332
- `02:50:40`     REPORT RequestId: 10741f83-e686-41dc-b680-ebd2c281c332	Duration: 115424.72 ms	Billed Duration: 116037 ms	Memory Size: 256 MB	Max Memory Used: 135 MB	Init Duration: 611.47 ms
## 4. Outcomes table inspection

- `02:50:41`   10 sampled outcomes:
- `02:50:41`     signal_type=momentum_tlt              window=day_1    correct=None   outcome={'price_at_check': 86.71, 'actual_direct checked_at=2026-04-24T23:17:41
- `02:50:41`     signal_type=khalid_index              window=day_14   correct=None   outcome={'price_at_check': 713.94, 'actual_direc checked_at=2026-04-24T23:17:41
- `02:50:41`     signal_type=crypto_fear_greed         window=day_3    correct=None   outcome={'price_at_check': 77397.8, 'actual_dire checked_at=2026-04-24T23:10:19
- `02:50:41`     signal_type=crypto_risk_score         window=day_14   correct=None   outcome={'price_at_check': 71737.39, 'actual_dir checked_at=2026-04-12T08:00:12
- `02:50:41`     signal_type=khalid_index              window=day_14   correct=None   outcome={'price_at_check': 713.94, 'actual_direc checked_at=2026-04-24T23:17:41
- `02:50:41`     signal_type=ml_risk                   window=day_14   correct=None   outcome={'price_at_check': 713.94, 'actual_direc checked_at=2026-04-24T23:17:41
- `02:50:41`     signal_type=khalid_index              window=day_7    correct=None   outcome={'price_at_check': 713.94, 'actual_direc checked_at=2026-04-24T23:17:41
- `02:50:41`     signal_type=crypto_risk_score         window=day_7    correct=None   outcome={'price_at_check': 75238.8, 'actual_dire checked_at=2026-04-19T08:00:12
- `02:50:41`     signal_type=crypto_fear_greed         window=day_3    correct=None   outcome={'price_at_check': 66805.4, 'actual_dire checked_at=2026-04-05T08:00:12
- `02:50:41`     signal_type=plumbing_stress           window=day_14   correct=None   outcome={'price_at_check': 713.94, 'actual_direc checked_at=2026-04-24T23:17:41
- `02:50:41` 
  Full scan for stats…
- `02:50:42`   Total outcomes scanned: 4307
- `02:50:42`   Distribution of 'correct' values:
- `02:50:42`     None            4307
- `02:50:42`   Distribution of window_key:
- `02:50:42`     day_30          1258
- `02:50:42`     day_7           1109
- `02:50:42`     day_14          1098
- `02:50:42`     day_3           579
- `02:50:42`     day_1           263
## 5. Are unscored outcomes overdue for scoring?

- `02:50:42` 
  Unscored outcome fdfa64fe-acef-44f9-809b-be177bbd27e8_day_1
- `02:50:42`     signal_id: fdfa64fe-acef-44f9-809b-be177bbd27e8
- `02:50:42`     window_key: day_1
- `02:50:42`     logged_at: 2026-03-20T21:10:13.805378+00:00
- `02:50:42`     checked_at: 2026-04-24T23:17:41.907006+00:00
- `02:50:42`     parent signal check_timestamps: {'day_1': '2026-03-21T21:10:13.805378+00:00', 'day_3': '2026-03-23T21:10:13.805378+00:00', 'day_7': '2026-03-27T21:10:13.805378+00:00'}
- `02:50:42`     ⚠  OVERDUE by 34 days! check_timestamp was 2026-03-21T21:10:13.805378+00:00
- `02:50:42` 
  Unscored outcome c2447ef6-3f05-43ee-9ebc-3dc49ad425ab_day_14
- `02:50:42`     signal_id: c2447ef6-3f05-43ee-9ebc-3dc49ad425ab
- `02:50:42`     window_key: day_14
- `02:50:42`     logged_at: 2026-03-22T09:10:13.390116+00:00
- `02:50:42`     checked_at: 2026-04-24T23:17:41.907006+00:00
- `02:50:42`     parent signal check_timestamps: {'day_14': '2026-04-05T09:10:13.390116+00:00', 'day_7': '2026-03-29T09:10:13.390116+00:00', 'day_30': '2026-04-21T09:10:13.390116+00:00'}
- `02:50:42`     ⚠  OVERDUE by 19 days! check_timestamp was 2026-04-05T09:10:13.390116+00:00
- `02:50:42` 
  Unscored outcome 035715a5-ba9e-44ad-8ebd-aa9ab0d6e6b9_day_3
- `02:50:42`     signal_id: 035715a5-ba9e-44ad-8ebd-aa9ab0d6e6b9
- `02:50:42`     window_key: day_3
- `02:50:42`     logged_at: 2026-04-19T03:10:13.721712+00:00
- `02:50:42`     checked_at: 2026-04-24T23:10:19.571920+00:00
- `02:50:42`     parent signal check_timestamps: {'day_14': '2026-05-03T03:10:13.721712+00:00', 'day_3': '2026-04-22T03:10:13.721712+00:00', 'day_7': '2026-04-26T03:10:13.721712+00:00'}
- `02:50:42`     ⚠  OVERDUE by 2 days! check_timestamp was 2026-04-22T03:10:13.721712+00:00
## 6. Summary

- `02:50:42`   - Outcomes total: 4307
- `02:50:42`   - Scored (correct in [True, False]): 0
- `02:50:42`   - Unscored (correct=None): 4307
- `02:50:42`   - Last-7d inv: 3, errors: 0
- `02:50:42` Done
