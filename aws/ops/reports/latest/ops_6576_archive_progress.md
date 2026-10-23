# ops 6576 - archive backfill progress, counts, storm-copy cleanup (read-only)

**Status:** success  
**Duration:** 169.0s  
**Finished:** 2026-10-23T00:03:17+00:00  

## Data

| archive_gb | archive_objects | backfilled_pct | job_failed | job_status | job_succeeded |
|---|---|---|---|---|---|
| 440.9 | 1349350 | 39.9 | 0 | Complete | 2086159 |

## Log
- `00:00:29` backfill job: {"job": "646ccb69-2925-4af5-b176-b155200aacaf", "desc": "ops 6612 warehouse archive backfill #2 f", "status": "Complete", "total": 2086159, "succeeded": 2086159, "failed": 0, "failure": [], "others": [{"job": "523b902a-5e7f-4ade-bf38-8c92eddcc947", "desc": "ops 6583 warehouse archive backfill, cur", "status": "Complete", "total": 1282030, "succeeded": 1282030, "failed": 0, "failure": []}, {"job": "de1a80a2-413d-4731-bd49-2b65e8422f4d", "desc": "ops 6575 warehouse archive backfill (jus", "status": "Cancelled", "total": 0, "succeeded": 0, "failed": 0, "failure": []}]}
- `00:02:27` archive: 1349350 current objects, 440.9 GB; live bucket current objects (census): 3381373 -> 39.9% backfilled
- `00:02:27` canary in the archive: REPLICA, STANDARD_IA
- `00:03:17` old copies on the storm keys (count rule keeps 10): {"data/miss-calibrator-proposals.json": {"old_copies": 199999, "calls": 200}, "system-events/ (first page)": {"hottest_key_old_copies_on_page": 865, "truncated": true}}
- `00:03:17` ✅ read
