# Phase 9.2 — create justhodl-regime-anomaly Lambda

**Status:** success  
**Duration:** 22.6s  
**Finished:** 2026-04-26T15:10:11+00:00  

## Log
## 1. Pre-flight

- `15:09:48`   ✅ justhodl-regime-anomaly does not exist — safe to create
## 2. Build zip

- `15:09:48`   zip: 8951B
## 3. Create justhodl-regime-anomaly

- `15:09:52`   ✅ created and Active
## 4. Test-invoke

- `15:10:05`   ✅ OK (13.4s)
- `15:10:05`   payload: {"statusCode": 200, "body": "{\"status\": \"ok\", \"elapsed_sec\": 12.57, \"ka_index_n_obs\": 200, \"hmm_state\": \"CRISIS\", \"n_anomalies\": 0, \"anomaly_score\": 0, \"s3_key\": \"data/regime-anomaly.json\"}"}
## 5. Verify s3://.../data/regime-anomaly.json

- `15:10:10`   ✅ written: 1675B  age=5s
- `15:10:10`   HMM training_n: 200
- `15:10:10`   HMM is_warming_up: False
- `15:10:10`   HMM current state: CRISIS
- `15:10:10`   HMM probabilities: {'EXPANSION': 0.0, 'LATE_CYCLE': 0.0, 'CONTRACTION': 0.0, 'CRISIS': 1.0}
- `15:10:10`   Anomaly n_anomalies: 0
- `15:10:10`   Anomaly score: 0
- `15:10:10`   Training window: {'ka_index_observations': 200, 'signal_count': 9, 'earliest': '2026-04-06T20:05:42.171158+00:00', 'latest': '2026-04-26T13:00:21.825915+00:00'}
## 6. Create EventBridge rule justhodl-regime-anomaly-refresh (rate(1 day))

- `15:10:11`   ✅ rule created
- `15:10:11`   ✅ EventBridge invoke perm granted
- `15:10:11`   ✅ rule targets justhodl-regime-anomaly
## FINAL

- `15:10:11`   Lambda: justhodl-regime-anomaly
- `15:10:11`   S3 output: data/regime-anomaly.json
- `15:10:11`   Schedule: rate(1 day) via justhodl-regime-anomaly-refresh
- `15:10:11`   Next: build /regime.html frontend
- `15:10:11` Done
