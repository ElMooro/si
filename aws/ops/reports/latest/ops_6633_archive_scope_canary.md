# ops 6633 - canary proof: a Disabled higher-priority rule keeps its prefix out of the archive

**Status:** failure  
**Duration:** 260.4s  
**Finished:** 2026-10-23T19:41:49+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. current replication configuration

- `19:37:29` rules now: [('warehouse-archive-all', 1, 'Enabled', '')]
- `19:37:29` ✅ backup: s3://justhodl-ops-private-857687956942/backups/replication/justhodl-dashboard-live-20261023T193729Z.json (restore = put_bucket_replication with that document)
## 2. add the disabled self-test rule

- `19:37:29` read back: [('archive-scope-self-test', 100, 'Disabled', '_health/archive-scope-excluded/'), ('warehouse-archive-all', 1, 'Enabled', '')]
## 3. canary pairs

- `19:37:32` pair 00 at +  2s: excluded -> PENDING | included -> PENDING
- `19:38:04` pair 01 at + 34s: excluded -> PENDING | included -> PENDING
- `19:38:36` pair 02 at + 66s: excluded -> PENDING | included -> PENDING
- `19:39:08` pair 03 at + 98s: excluded -> PENDING | included -> PENDING
- `19:39:40` pair 04 at +130s: excluded -> PENDING | included -> PENDING
- `19:40:13` pair 05 at +163s: excluded -> PENDING | included -> PENDING
- `19:40:45` pair 06 at +195s: excluded -> PENDING | included -> PENDING
- `19:41:17` pair 07 at +227s: excluded -> PENDING | included -> PENDING
- `19:41:49` pair 08 at +259s: excluded -> PENDING | included -> PENDING
- `19:41:49` ⚠ RuntimeError: excluded canaries kept being marked for replication: ['PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING']
## 5. restore

- `19:41:49` ✅ configuration restored to the single whole-bucket rule: True
- `19:41:49` ✗ mechanism not proven - the archive copies everything as before; scope B needs the include-list fallback
