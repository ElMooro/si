# ops 6576 - archive backfill progress, counts, storm-copy cleanup (read-only)

**Status:** success  
**Duration:** 174.5s  
**Finished:** 2026-10-22T15:53:26+00:00  

## Data

| archive_gb | archive_objects | backfilled_pct | job_failed | job_status | job_succeeded |
|---|---|---|---|---|---|
| 439.0 | 1326327 | 103.5 | 0 | Complete | 1282030 |

## Log
- `15:50:32` backfill job: {"job": "523b902a-5e7f-4ade-bf38-8c92eddcc947", "status": "Complete", "total": 1282030, "succeeded": 1282030, "failed": 0, "failure": []}
- `15:52:37` archive: 1326327 current objects, 439.0 GB; live bucket current objects (census): 1281312 -> 103.5% backfilled
- `15:52:37` canary in the archive: REPLICA, STANDARD_IA
- `15:53:26` old copies on the storm keys (count rule keeps 10): {"data/miss-calibrator-proposals.json": {"old_copies": 199999, "calls": 200}, "system-events/ (first page)": {"hottest_key_old_copies_on_page": 865, "truncated": true}}
- `15:53:26` ✅ read
