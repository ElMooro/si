# Build justhodl-reports-builder Lambda + scorecard.json

**Status:** success  
**Duration:** 19.4s  
**Finished:** 2026-04-25T02:23:52+00:00  

## Data

| eb_rule | lambda_deployed | scorecard_key |
|---|---|---|
| justhodl-reports-builder-hourly | justhodl-reports-builder | reports/scorecard.json |

## Log
## 1. Probe DynamoDB schemas

- `02:23:32` 
  justhodl-signals: total~4779 items
- `02:23:32`   Sample item keys: ['accuracy_scores', 'baseline_price', 'benchmark', 'check_timestamps', 'check_windows', 'confidence', 'logged_at', 'logged_epoch', 'measure_against', 'metadata', 'outcomes', 'predicted_direction', 'signal_id', 'signal_type', 'signal_value', 'status', 'ttl']
- `02:23:32`   Sample item (first 2 fields):
- `02:23:32`     benchmark: "SPY"
- `02:23:32`     signal_type: "screener_top_pick"
- `02:23:32`     metadata: {"rank": 3.0, "piotroski": 1.0}
- `02:23:32`     logged_at: "2026-04-12T21:10:14.163979+00:00"
- `02:23:32`     check_windows: ["30", "60", "90"]
- `02:23:32`     predicted_direction: "OUTPERFORM"
- `02:23:32`     status: "pending"
- `02:23:32`     check_timestamps: {"day_90": "2026-07-11T21:10:14.163979+00:00", "day_60": "2026-06-11T21:10:14.163979+00:00", "day_30
- `02:23:33` 
  justhodl-outcomes: total~4307 items
- `02:23:33`   Sample item keys: ['checked_at', 'correct', 'logged_at', 'outcome', 'outcome_id', 'predicted_dir', 'signal_id', 'signal_type', 'signal_value', 'ttl', 'window_key']
- `02:23:33`   Sample item (first 2 fields):
- `02:23:33`     checked_at: "2026-04-24T23:17:41.907006+00:00"
- `02:23:33`     signal_type: "momentum_tlt"
- `02:23:33`     logged_at: "2026-03-20T21:10:13.805378+00:00"
- `02:23:33`     signal_id: "fdfa64fe-acef-44f9-809b-be177bbd27e8"
- `02:23:33`     predicted_dir: "DOWN"
- `02:23:33`     signal_value: "-1.71%"
- `02:23:33`     correct: null
- `02:23:33`     outcome_id: "fdfa64fe-acef-44f9-809b-be177bbd27e8_day_1"
## 2. Read existing SSM calibration data

- `02:23:33`   /justhodl/calibration/weights: parsed JSON, 284B
- `02:23:33`   /justhodl/calibration/accuracy: parsed JSON, 140B
- `02:23:33`   /justhodl/calibration/report: parsed JSON, 2881B
- `02:23:33`   Sample weights: [('crypto_risk_score', 0.3098), ('crypto_fear_greed', 0.3098), ('khalid_index', 1.0)]
- `02:23:33`   Sample accuracy: [('crypto_risk_score', {'accuracy': 0.0, 'n': 369, 'avg_return': None}), ('crypto_fear_greed', {'accuracy': 0.0, 'n': 369, 'avg_return': None})]
## 3. Write Lambda code

- `02:23:33` ✅   Wrote: aws/lambdas/justhodl-reports-builder/source/lambda_function.py (247 LOC)
- `02:23:33` ✅   Syntax OK
## 4. Deploy Lambda

- `02:23:37` ✅   Created new Lambda (8749B)
## 5. EventBridge schedule

- `02:23:40` ✅   put_rule justhodl-reports-builder-hourly
- `02:23:40` ✅   put_targets done
- `02:23:40` ✅   add_permission for EB invoke
## 6. Initial invoke

- `02:23:51` ✅   Invoked: {'ok': True, 'scorecard_rows': 15, 'timeline_points': 2, 'signals_seen': 4779, 'outcomes_seen': 4307}
## 7. Verify scorecard.json in S3

- `02:23:52` ✅   scorecard.json: 4,202B  modified 2026-04-25 02:23:52+00:00
- `02:23:52`   meta: signals=4779 outcomes=4307 scored=0
- `02:23:52`   scorecard rows: 15
- `02:23:52`   timeline points: 2
- `02:23:52`   Top signal by sample size: screener_top_pick (910 predictions, 0% hit rate)
- `02:23:52` Done
