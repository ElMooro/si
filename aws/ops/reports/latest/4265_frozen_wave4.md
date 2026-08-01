# ops 4265 -- frozen-writer wave 4

**Status:** success  
**Duration:** 99.3s  
**Finished:** 2026-08-01T23:51:19+00:00  

## Log
## 1. contract corrections (manifest key_overrides)

- `23:49:40` ✅ manifest updated: 4 key_overrides, 1 retirement
## 2. signal-halflife -- honest-empty semantics

- `23:50:36` invoked: {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": "{\"ok\": true, \"n_engines\": 334, \"n_outcome
- `23:50:37` ✅ signal-halflife.json FRESH (0.0 min) -- n_outcomes=101000 status=OK
## 3. congress party map -- self-refreshing cache

- `23:51:11` invoked: {"statusCode": 200, "body": "{\"ok\": true, \"n_quiver\": 1000, \"n_house\": 500, \"n_senate\": 500, \"n_tickers\": 181, \"n_clusters\": 25, \"n_bipartisan\": 4
- `23:51:12` log: [political] S3 party map is 62d old -- forcing live refresh (stale copy kept as fallback)
- `23:51:12` log: [political] S3 cache miss — trying live fetch
## 4. monitor re-read under the corrected contract

- `23:51:19` ✅ data/polygon-related-graph.json CLEAN under corrected SLA
- `23:51:19` ✅ data/factor-data-cache.json CLEAN under corrected SLA
- `23:51:19` fleet: 2 stale / 52 tracked
## 5. calibration family -- disclosed, requeued

- `23:51:19` SSM weights present: 8115 chars -- {"screener_top_pick": 0.8597, "correlation_break": 0.3552, "ml_risk": 0.3751, "e
## RESULT

- `23:51:19` ✗   party map still stale (89679 min) with no blocked-source evidence
