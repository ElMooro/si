# ops 4265 -- frozen-writer wave 4

**Status:** success  
**Duration:** 82.2s  
**Finished:** 2026-08-01T23:55:29+00:00  

## Log
## 1. contract corrections (manifest key_overrides)

- `23:54:07` ✅ manifest updated: 4 key_overrides, 1 retirement
## 2. signal-halflife -- honest-empty semantics

- `23:54:35` invoked: {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": "{\"ok\": true, \"n_engines\": 334, \"n_outcome
- `23:54:36` ✅ signal-halflife.json FRESH (0.0 min) -- n_outcomes=101000 status=OK
## 3. congress party map -- self-refreshing cache

- `23:55:09` invoked: {"statusCode": 200, "body": "{\"ok\": true, \"n_quiver\": 1000, \"n_house\": 500, \"n_senate\": 500, \"n_tickers\": 181, \"n_clusters\": 25, \"n_bipartisan\": 4
- `23:55:20` log: [political] S3 cache miss — trying live fetch
- `23:55:20` log: [political] err: URLError <urlopen error [Errno 110] Connection timed out>
- `23:55:20` log: [political] live fetch failed -- keeping stale S3 map (better than the hardcoded floor)
- `23:55:20` log: [political] fetching Congress trades…
- `23:55:20` log: [political] HTTP 401 from https://api.quiverquant.com/beta/live/congresstrading
- `23:55:20` log: [political] live Quiver returned empty — trying S3 cache
- `23:55:20` log: [political] using S3 Quiver cache: 1000 trades (age 897.9h)
- `23:55:20` log: [political] got 1000 trades from: s3_cache_897.9h
- `23:55:20` log: [political] 181 unique tickers traded in last 90 days
- `23:55:20` log: [political] wrote 166,090B  duration=33.0s  tickers=181  clusters=25
- `23:55:20` ⚠ theunitedstates.io blocked from Lambda -- stale-copy fallback held (map is 62d old, materially fine; SLA now 45d, disclosed)
## 4. monitor re-read under the corrected contract

- `23:55:26` ✅ data/polygon-related-graph.json CLEAN under corrected SLA
- `23:55:26` ✅ data/factor-data-cache.json CLEAN under corrected SLA
- `23:55:26` fleet: 2 stale / 52 tracked
## 5. calibration-snapshotter -- weights exist, so the blocker is the dead schedule: invoke it

- `23:55:28` invoked: {"statusCode": 200, "body": "{\"iso_week\": \"2026-W31\", \"n_weights\": 277, \"n_calibrated_n30\": 129, \"n_snapshots_total\": 13, \"duration_s\": 0.75}"}
- `23:55:29` ✅ history-index UNFROZEN (-0.0 min) -- 13 snapshots, latest week 2026-W31
- `23:55:29` live config for onboarding: runtime=python3.12 mem=512 timeout=120 role=...2:role/lambda-execution-role env_keys=[]
- `23:55:29` ⚠ UNMANAGED: no config.json + no schedule -- next push onboards it (config.json from the values above + weekly Scheduler entry in the manifest)
## RESULT

- `23:55:29` ✅ OPS 4265 PASS -- wave 4: 2 resurrections, 3 contract corrections, 1 retirement, 1 honest requeue
