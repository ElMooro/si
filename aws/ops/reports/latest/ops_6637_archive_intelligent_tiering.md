# ops 6637 - archive copies as Intelligent-Tiering (same whole-bucket rule, same seal)

**Status:** success  
**Duration:** 46.6s  
**Finished:** 2026-10-24T14:49:32+00:00  

## Data

| backup | canary | rule | storage_class_before | storage_class_now |
|---|---|---|---|---|
| backups/replication/justhodl-dashboard-live-20261024T144846Z.json | _health/replication-canary-it-20261024T144846Z.json | warehouse-archive-all | STANDARD_IA | INTELLIGENT_TIERING |

## Log
- `14:48:46` now: warehouse-archive-all -> arn:aws:s3:::justhodl-warehouse-archive-857687956942, storage class STANDARD_IA
- `14:48:46` ✅ backup: s3://justhodl-ops-private-857687956942/backups/replication/justhodl-dashboard-live-20261024T144846Z.json
- `14:48:47` ✅ rule warehouse-archive-all now copies as INTELLIGENT_TIERING
- `14:49:32` canary _health/replication-canary-it-20261024T144846Z.json: source COMPLETED, archive storage class INTELLIGENT_TIERING
- `14:49:32` ✅ new archive copies are INTELLIGENT_TIERING; protection unchanged (every write still copied, nothing deletable but by root)
