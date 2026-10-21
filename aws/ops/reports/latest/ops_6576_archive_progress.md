# ops 6576 - archive backfill progress, counts, storm-copy cleanup (read-only)

**Status:** success  
**Duration:** 49.0s  
**Finished:** 2026-10-21T00:17:25+00:00  

## Data

| archive_gb | archive_objects | backfilled_pct | job_failed | job_status | job_succeeded |
|---|---|---|---|---|---|
| 0.0 | 4 | 0.0 | 0 | Cancelled | 0 |

## Log
- `00:16:36` backfill job: {"job": "de1a80a2-413d-4731-bd49-2b65e8422f4d", "status": "Cancelled", "total": 0, "succeeded": 0, "failed": 0, "failure": []}
- `00:16:37` archive: 4 current objects, 0.0 GB; live bucket current objects (census): 1281312 -> 0.0% backfilled
- `00:16:37` canary in the archive: REPLICA, STANDARD_IA
- `00:17:25` old copies on the storm keys (count rule keeps 10): {"data/miss-calibrator-proposals.json": {"old_copies": 199999, "calls": 200}, "system-events/ (first page)": {"hottest_key_old_copies_on_page": 865, "truncated": true}}
- `00:17:25` ✅ read
