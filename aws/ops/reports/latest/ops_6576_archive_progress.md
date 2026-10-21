# ops 6576 - archive backfill progress, counts, storm-copy cleanup (read-only)

**Status:** success  
**Duration:** 227.3s  
**Finished:** 2026-10-21T02:48:06+00:00  

## Data

| archive_gb | archive_objects | backfilled_pct | job_failed | job_status | job_succeeded |
|---|---|---|---|---|---|
| 433.1 | 1281180 | 100.0 | 0 | Active | 1229048 |

## Log
- `02:44:19` backfill job: {"job": "523b902a-5e7f-4ade-bf38-8c92eddcc947", "status": "Active", "total": 1282030, "succeeded": 1229048, "failed": 0, "failure": []}
- `02:47:13` archive: 1281180 current objects, 433.1 GB; live bucket current objects (census): 1281312 -> 100.0% backfilled
- `02:47:13` canary in the archive: REPLICA, STANDARD_IA
- `02:48:06` old copies on the storm keys (count rule keeps 10): {"data/miss-calibrator-proposals.json": {"old_copies": 199999, "calls": 200}, "system-events/ (first page)": {"hottest_key_old_copies_on_page": 865, "truncated": true}}
- `02:48:06` ✅ read
