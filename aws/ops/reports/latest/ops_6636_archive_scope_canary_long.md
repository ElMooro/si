# ops 6633 - canary proof: a Disabled higher-priority rule keeps its prefix out of the archive

**Status:** failure  
**Duration:** 1204.1s  
**Finished:** 2026-10-23T20:05:47+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. current replication configuration

- `19:45:44` rules now: [('warehouse-archive-all', 1, 'Enabled', '')]
- `19:45:44` ✅ backup: s3://justhodl-ops-private-857687956942/backups/replication/justhodl-dashboard-live-20261023T194543Z.json (restore = put_bucket_replication with that document)
## 2. add the disabled self-test rule

- `19:45:44` read back: [('archive-scope-self-test', 100, 'Disabled', '_health/archive-scope-excluded/'), ('warehouse-archive-all', 1, 'Enabled', '')]
## 3. canary pairs

- `19:45:47` pair 00 at +  2s: excluded -> PENDING | included -> PENDING
- `19:46:19` pair 01 at + 34s: excluded -> PENDING | included -> PENDING
- `19:46:51` pair 02 at + 67s: excluded -> PENDING | included -> PENDING
- `19:47:24` pair 03 at + 99s: excluded -> PENDING | included -> PENDING
- `19:47:56` pair 04 at +132s: excluded -> PENDING | included -> PENDING
- `19:48:29` pair 05 at +164s: excluded -> PENDING | included -> PENDING
- `19:49:01` pair 06 at +197s: excluded -> PENDING | included -> PENDING
- `19:49:34` pair 07 at +229s: excluded -> PENDING | included -> PENDING
- `19:50:06` pair 08 at +262s: excluded -> PENDING | included -> PENDING
- `19:50:39` pair 09 at +294s: excluded -> PENDING | included -> PENDING
- `19:51:11` pair 10 at +326s: excluded -> PENDING | included -> PENDING
- `19:51:43` pair 11 at +359s: excluded -> PENDING | included -> PENDING
- `19:52:16` pair 12 at +391s: excluded -> PENDING | included -> PENDING
- `19:52:48` pair 13 at +424s: excluded -> PENDING | included -> PENDING
- `19:53:21` pair 14 at +456s: excluded -> PENDING | included -> PENDING
- `19:53:53` pair 15 at +489s: excluded -> PENDING | included -> PENDING
- `19:54:26` pair 16 at +521s: excluded -> PENDING | included -> PENDING
- `19:54:58` pair 17 at +554s: excluded -> PENDING | included -> PENDING
- `19:55:31` pair 18 at +586s: excluded -> PENDING | included -> PENDING
- `19:56:03` pair 19 at +619s: excluded -> PENDING | included -> PENDING
- `19:56:36` pair 20 at +651s: excluded -> PENDING | included -> PENDING
- `19:57:08` pair 21 at +684s: excluded -> PENDING | included -> PENDING
- `19:57:41` pair 22 at +716s: excluded -> PENDING | included -> PENDING
- `19:58:13` pair 23 at +748s: excluded -> PENDING | included -> PENDING
- `19:58:45` pair 24 at +781s: excluded -> PENDING | included -> PENDING
- `19:59:18` pair 25 at +813s: excluded -> PENDING | included -> PENDING
- `19:59:50` pair 26 at +846s: excluded -> PENDING | included -> PENDING
- `20:00:23` pair 27 at +878s: excluded -> PENDING | included -> PENDING
- `20:00:55` pair 28 at +911s: excluded -> PENDING | included -> PENDING
- `20:01:28` pair 29 at +943s: excluded -> PENDING | included -> PENDING
- `20:02:00` pair 30 at +976s: excluded -> PENDING | included -> PENDING
- `20:02:33` pair 31 at +1008s: excluded -> PENDING | included -> PENDING
- `20:03:05` pair 32 at +1041s: excluded -> PENDING | included -> PENDING
- `20:03:38` pair 33 at +1073s: excluded -> PENDING | included -> PENDING
- `20:04:10` pair 34 at +1105s: excluded -> PENDING | included -> PENDING
- `20:04:42` pair 35 at +1138s: excluded -> PENDING | included -> PENDING
- `20:05:15` pair 36 at +1170s: excluded -> PENDING | included -> PENDING
- `20:05:47` pair 37 at +1203s: excluded -> PENDING | included -> PENDING
- `20:05:47` ⚠ RuntimeError: excluded canaries kept being marked for replication: ['PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PENDING', 'PEND
## 5. restore

- `20:05:47` ✅ configuration restored to the single whole-bucket rule: True
- `20:05:47` ✗ mechanism not proven - the archive copies everything as before; scope B needs the include-list fallback
