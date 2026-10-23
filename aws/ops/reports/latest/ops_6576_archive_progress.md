# ops 6576 - archive backfill progress, counts, storm-copy cleanup (read-only)

**Status:** success  
**Duration:** 531.6s  
**Finished:** 2026-10-23T00:36:48+00:00  

## Data

| archive_gb | archive_objects | backfilled_pct | job_failed | job_status | job_succeeded |
|---|---|---|---|---|---|
| 1031.8 | 3456698 | 102.2 | 0 | Complete | 2086159 |

## Log
- `00:27:57` backfill job: {"job": "646ccb69-2925-4af5-b176-b155200aacaf", "desc": "ops 6612 warehouse archive backfill #2 f", "status": "Complete", "total": 2086159, "succeeded": 2086159, "failed": 0, "failure": [], "others": [{"job": "523b902a-5e7f-4ade-bf38-8c92eddcc947", "desc": "ops 6583 warehouse archive backfill, cur", "status": "Complete", "total": 1282030, "succeeded": 1282030, "failed": 0, "failure": []}, {"job": "de1a80a2-413d-4731-bd49-2b65e8422f4d", "desc": "ops 6575 warehouse archive backfill (jus", "status": "Cancelled", "total": 0, "succeeded": 0, "failed": 0, "failure": []}]}
- `00:35:29` archive listing detail: {"prefixes": 301, "root_files": 1243, "top": [["data/providers/", 1137345], ["data/warm/", 778792], ["audit-private/", 536423], ["data/evidence/", 176224], ["data/etf-holdings-research/", 115620], ["data/option-flow-research/", 95808], ["data/series-cache/", 67401], ["data/fred-levels/", 60444]], "providers": 1137345, "warm": 778792}
- `00:35:29` archive: 3456698 current objects, 1031.8 GB; live bucket current objects (census): 3381373 -> 102.2% backfilled
- `00:35:30` canary in the archive: REPLICA, STANDARD_IA
- `00:36:48` old copies on the storm keys (count rule keeps 10): {"data/miss-calibrator-proposals.json": {"old_copies": 199999, "calls": 200}, "system-events/ (first page)": {"hottest_key_old_copies_on_page": 865, "truncated": true}}
- `00:36:48` ✅ read
