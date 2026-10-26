# ops 6674 - sizing the five fleet-health fixes (read-only)

**Status:** success  
**Duration:** 10.0s  
**Finished:** 2026-10-26T15:20:21+00:00  

## Log
## 1. analytics-snapshot: the three prefixes

- `15:20:18` equity-research/: 35572 objects, 11884.4 MB, 35572 read by the engine (.json minus manifest/latest); ages {'<=1d': 3642, '<=7d': 5568, '<=30d': 14474, '>30d': 11888}; biggest equity-research/industry2/CTHRX.json (841 KB)
- `15:20:18`    equity-research/(top): 20005 objects, 2522.8 MB
- `15:20:18`    equity-research/industry2/: 15566 objects, 9361.5 MB
- `15:20:18`    equity-research/shared/: 1 objects, 0.0 MB
- `15:20:18` edgar-insiders/: 4953 objects, 12.0 MB, 4953 read by the engine (.json minus manifest/latest); ages {'>30d': 3349, '<=30d': 1229, '<=7d': 322, '<=1d': 53}; biggest edgar-insiders/WBD.json (17 KB)
- `15:20:18`    edgar-insiders/(top): 4953 objects, 12.0 MB
- `15:20:19` equity-critique/: 76 objects, 0.6 MB, 76 read by the engine (.json minus manifest/latest); ages {'>30d': 76}; biggest equity-critique/BRK-B.json (10 KB)
- `15:20:19`    equity-critique/(top): 76 objects, 0.6 MB
## 2. earnings-whisper: the seven inputs

- `15:20:19` data/earnings-tracker.json: 37290 bytes (0.0 MB), last modified 2026-10-26T11:32:08
- `15:20:19` data/eps-revision-velocity.json: 55061180 bytes (55.1 MB), last modified 2026-10-26T05:01:48
- `15:20:19` data/options-flow-scanner.json: 19421817 bytes (19.4 MB), last modified 2026-10-25T22:04:12
- `15:20:19` data/insider-trades.json: 1040340 bytes (1.0 MB), last modified 2026-10-25T21:10:12
- `15:20:19` data/8k-filings.json: 5500804 bytes (5.5 MB), last modified 2026-10-16T22:33:47
- `15:20:19` data/revenue-acceleration.json: 4112112 bytes (4.1 MB), last modified 2026-10-25T11:15:50
- `15:20:19` data/smart-money-clusters.json: 26635 bytes (0.0 MB), last modified 2026-10-25T16:00:31
## 3. event-flow-monitor: system-events/audit/<day>.jsonl, 16 days

- `15:20:19` 2026-10-26: None bytes (0.0 MB) ClientError
- `15:20:19` 2026-10-25: None bytes (0.0 MB) ClientError
- `15:20:19` 2026-10-24: None bytes (0.0 MB) ClientError
- `15:20:19` 2026-10-23: None bytes (0.0 MB) ClientError
- `15:20:19` 2026-10-22: None bytes (0.0 MB) ClientError
- `15:20:19` 2026-10-21: None bytes (0.0 MB) ClientError
- `15:20:20` 2026-10-20: None bytes (0.0 MB) ClientError
- `15:20:20` 2026-10-19: None bytes (0.0 MB) ClientError
- `15:20:20` 2026-10-18: None bytes (0.0 MB) ClientError
- `15:20:20` 2026-10-17: None bytes (0.0 MB) ClientError
- `15:20:20` 2026-10-16: 25958542 bytes (26.0 MB) 2026-10-16T19:45:01
- `15:20:20` 2026-10-15: 30847564 bytes (30.8 MB) 2026-10-16T00:00:15
- `15:20:20` 2026-10-14: 30699302 bytes (30.7 MB) 2026-10-15T00:00:15
- `15:20:20` 2026-10-13: 33202510 bytes (33.2 MB) 2026-10-14T00:00:16
- `15:20:20` 2026-10-12: 22038409 bytes (22.0 MB) 2026-10-13T00:00:13
- `15:20:20` 2026-10-11: 31680 bytes (0.0 MB) 2026-10-11T23:56:01
- `15:20:20` 16-day total 142.8 MB
- `15:20:20` objects under system-events/audit/: 139
## 4. treasury-auto-updater / treasury-api

- `15:20:20` treasury-auto-updater: 128 MB / 3 s, handler lambda_function.lambda_handler, modified 2026-10-08T16:20:13, state Active
- `15:20:21`    reserved concurrency: (none)
- `15:20:21` treasury-api: 512 MB / 60 s, handler lambda_function.lambda_handler, modified 2026-10-08T16:20:08, state Active
- `15:20:21`    reserved concurrency: 0
- `15:20:21` live fan-out manifest: treasury-auto-updater in ticks ['daily-morn']; assignment {'fired_hours': [11], 'per_day': 1.0, 'ticks': ['daily-morn']}; manifest generated_at 2026-09-02T01:17:54+00:00
## 5. signal-genealogy: one failing capture's record references

- `15:20:21` capture generated_at 2026-10-14T04:45:33.115758+00:00, 60 record refs, 204 sources
- `15:20:21` records[0] keys: ['created', 'direction', 'forecast_id', 'key', 'registered_at', 'research_context', 'sha256', 'source_key', 'symbol']
- `15:20:21` records[0].research_context keys: ['status']
- `15:20:21` research_context status mix: {'existing_forecast_not_backfilled': 60}
- `15:20:21` ✅ ops 6674 complete (read-only)
