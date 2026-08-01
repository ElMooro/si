# ops 4239 — converge schedules, real cross-region DR

**Status:** success  
**Duration:** 55.1s  
**Finished:** 2026-08-01T15:04:24+00:00  

## Data

| dest | region | rules | section | source | verified |
|---|---|---|---|---|---|
| justhodl-dr-usw2-857687956942 | us-west-2 | 1 | crr | justhodl-dashboard-live-dr | True |

## Log
## A. Reconciler v1.1.0 + converge duplicates

- `15:03:36` ✅ marker verified
- `15:03:36` ✅ SSM mode = enforce-duplicates
- `15:03:50` converge run -> {"ok": true, "mode": "enforce-duplicates", "drift_count": 4, "by_class": {"DUPLICATE_TARGET": 4}, "enforced": 4}
- `15:04:10` verify run  -> {"ok": true, "mode": "enforce-duplicates", "drift_count": 0, "by_class": {}, "enforced": 0}
- `15:04:10` ✅ DRIFT = 0 — live AWS now matches the declared manifest
## B1. Destination bucket in us-west-2

- `15:04:10` ✅ created justhodl-dr-usw2-857687956942
- `15:04:11` ✅ destination versioning=Enabled
## B2. Replication role

- `15:04:11` ✅ created role jh-s3-dr-replication
- `15:04:23` ✅ role policy attached — arn:aws:iam::857687956942:role/jh-s3-dr-replication
## B3. Replication rule + read-back verification

- `15:04:24` ✅ replication read-back: 1 rule(s), enabled->justhodl-dr-usw2-857687956942 = True
## B4. Note the stale second bucket

- `15:04:24` ⚠ justhodl-backups-857687956942 — 1 object, 19 days old, versioning DISABLED. Left untouched: identify what wrote it before removing anything.
## RESULT

- `15:04:24` ✅ OPS 4239 PASS — schedules converged and DR now leaves us-east-1.
