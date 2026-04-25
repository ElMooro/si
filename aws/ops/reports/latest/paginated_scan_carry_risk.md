# Was carry_risk missing or did scan paginate out before finding it?

**Status:** success  
**Duration:** 18.5s  
**Finished:** 2026-04-25T00:24:58+00:00  

## Data

| carry_risk_in_last_10min | carry_risk_score_now | carry_risk_total_records | ml_risk_score_now |
|---|---|---|---|
| 3 | 25 | 186 | 60 |

## Log
## A. Current intelligence-report.json scores

- `00:24:40`   Age: 10.7 min
- `00:24:40`   scores: {
  "khalid_index": 43,
  "crisis_distance": 60,
  "plumbing_stress": 25,
  "ml_risk_score": 60,
  "carry_risk_score": 25,
  "vix": 19.31,
  "move": null
}
## B. Synchronous signal-logger invoke

- `00:24:57`   Status: 200, body: {"statusCode": 200, "body": "{\"logged\": 25}"}
## C. Latest signal-logger log output

- `00:24:57`   Stream: 2026/04/25/[$LATEST]8dce17e54c82482b9a023a2576eb96d8
- `00:24:58`     [LOG] ml_risk=60.0 NEUTRAL conf=0.20 baseline=$713.94
- `00:24:58`     [LOG] carry_risk=25.0 UP conf=0.50 baseline=$713.94
- `00:24:58`     [LOG] ml_risk=60.0 NEUTRAL conf=0.20 baseline=$713.94
- `00:24:58`     [LOG] carry_risk=25.0 UP conf=0.50 baseline=$713.94
- `00:24:58`     [LOG] ml_risk=60.0 NEUTRAL conf=0.20 baseline=$713.94
- `00:24:58`     [LOG] carry_risk=25.0 UP conf=0.50 baseline=$713.94
## D. Paginated scan — total carry_risk records in table

- `00:24:58`   Total carry_risk records: 186 (across 3 pages)
- `00:24:58`   In last 10 min: 3
- `00:24:58` 
  Sample carry_risk records (most recent first):
- `00:24:58`     logged_at=2026-04-14T15:10:14.046710+00:00, signal_value=0.0
- `00:24:58`     logged_at=2026-04-03T15:10:14.155500+00:00, signal_value=0.0
- `00:24:58`     logged_at=2026-03-24T21:10:13.914057+00:00, signal_value=0.0
- `00:24:58`     logged_at=2026-03-19T15:10:13.902655+00:00, signal_value=0.0
- `00:24:58`     logged_at=2026-03-14T09:10:13.952186+00:00, signal_value=0.0
- `00:24:58` Done
