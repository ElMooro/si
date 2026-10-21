# ops 6583 - archive backfill from a current-versions-only manifest

**Status:** success  
**Duration:** 465.0s  
**Finished:** 2026-10-21T02:21:19+00:00  

## Data

| failed | job_id | null_versions | rows | status | succeeded | total |
|---|---|---|---|---|---|---|
| 0 | 523b902a-5e7f-4ade-bf38-8c92eddcc947 | 18765 | 1282031 | Active | 0 | 1282030 |

## Log
- `02:13:35` (root): 42 top-level files (HEAD)
- `02:13:56` data/: 1201 top-level files (HEAD)
- `02:20:29` current objects listed: 1282031 (18765 with a null version id), 2590 LIST calls, 415 s
- `02:20:33` manifest: s3://justhodl-ops-private-857687956942/batch-replication/manifests/20261021T022031Z.csv (1282031 rows, 215.3 MB, etag 6c25c5bcf6a1)
- `02:21:19` ✅ backfill job 523b902a-5e7f-4ade-bf38-8c92eddcc947 created from 1282031 current objects (Active); ops 6576 reads progress
