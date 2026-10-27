# ops 6701 - the old copies of worldbank-full's progress file: live deleted down to one, archive trimmed by its own rule

**Status:** failure  
**Duration:** 84.3s  
**Finished:** 2026-10-27T23:42:06+00:00  

## Log
## 1. what the copies are

- `23:40:58` live data/warm/worldbank-full/_state/state.json: 56709 versions, 153.5 GB, 2026-10-25T03:20:44 .. 2026-10-27T20:59:25
- `23:40:58`    CURRENT 2026-10-27T20:59:25   235818 B: phase COMPLETE, queue 0, have 29468, failures 22, as_of 2026-10-27T20:59:24+00:00, top-level keys ['as_of', 'bytes_total', 'failures', 'have', 'lease_until', 'n_banked', 'n_indicators', 'phase', 'queue', 'version']
- `23:40:58`    old     2026-10-27T06:55:03  2442382 B: phase DRAIN, queue 9177, have 29468, failures 26, as_of 2026-10-27T06:47:47+00:00, top-level keys ['as_of', 'bytes_total', 'failures', 'have', 'lease_until', 'n_banked', 'n_indicators', 'phase', 'queue', 'version']
- `23:40:58`    old     2026-10-27T03:55:39  2688716 B: phase DRAIN, queue 15321, have 29468, failures 26, as_of 2026-10-27T03:53:29+00:00, top-level keys ['as_of', 'bytes_total', 'failures', 'have', 'lease_until', 'n_banked', 'n_indicators', 'phase', 'queue', 'version']
- `23:40:58`    old     2026-10-25T03:20:44  2094064 B: phase COMPLETE, queue 0, have 29468, failures 28, as_of 2026-10-25T03:20:43+00:00, top-level keys ['as_of', 'bytes_total', 'failures', 'have', 'lease_until', 'n_banked', 'n_indicators', 'phase', 'queue', 'version']
- `23:40:58` the indicator data itself lives in src/<indicator>.zip and is not touched
## 2. live bucket: delete the old versions, keep the current one

- `23:41:49` ✗ data/warm/worldbank-full/_state/state.json: 0 old versions deleted (0.0 GB), 56708 errors [{'Key': 'data/warm/worldbank-full/_state/state.json', 'VersionId': 'R1sZA7g4yytRsp8d7CwaMsIoEQHHWBSF', 'Code': 'AccessDenied', 'Message': 'User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: s3:DeleteObjectVersion on resource: "arn:aws:s3:::justhodl-dashboard-live/data/warm/worldbank-full/_state/state.json" with an explicit deny in a resource-based policy'}, {'Key': 'data/warm/worldbank-full/_state/state.json', 'VersionId': 'fK_yLj2dTN1_BFoUaLGnGDoeyNdxmXRV', 'Code': 'AccessDenied', 'Message': 'User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: s3:DeleteObjectVersion on resource: "arn:aws:s3:::justhodl-dashboard-live/data/warm/worldbank-full/_state/state.json" with an explicit deny in a resource-based policy'}]; now 56709 version(s) (0 written while this ran), current 2026-10-27T20:59:25 235818 B
- `23:41:51` ✗ data/warm/worldbank-full/_state/inflight.json: 0 old versions deleted (0.0 GB), 2872 errors [{'Key': 'data/warm/worldbank-full/_state/inflight.json', 'VersionId': '6BE4UP5WUD0OMWun4zBTy7W0tZJFnB1x', 'Code': 'AccessDenied', 'Message': 'User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: s3:DeleteObjectVersion on resource: "arn:aws:s3:::justhodl-dashboard-live/data/warm/worldbank-full/_state/inflight.json" with an explicit deny in a resource-based policy'}, {'Key': 'data/warm/worldbank-full/_state/inflight.json', 'VersionId': '47rPVJHyxYQv.4MYFv1Gy_CfN2.76dvq', 'Code': 'AccessDenied', 'Message': 'User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: s3:DeleteObjectVersion on resource: "arn:aws:s3:::justhodl-dashboard-live/data/warm/worldbank-full/_state/inflight.json" with an explicit deny in a resource-based policy'}]; now 2873 version(s) (0 written while this ran), current 2026-10-27T20:59:25 62 B
## 3. archive bucket (sealed): its own lifecycle rule

- `23:41:52` rule archive-keep-3-old-copies [Enabled] filter {'Prefix': ''} | noncurrent {'NoncurrentDays': 1, 'NewerNoncurrentVersions': 3} | expiration None
- `23:42:06` archive data/warm/worldbank-full/_state/state.json: 56709 versions (56708 old), 153.5 GB; oldest 2026-10-25T03:20:44, newest 2026-10-27T20:59:25; old copies beyond the 3 newest: 56705 / 153.5 GB
- `23:42:06`    oldest old copy is 2.8 days old
- `23:42:06` archive data/warm/worldbank-full/_state/inflight.json: 2873 versions (2872 old), 0.0 GB; oldest 2026-10-27T20:29:33, newest 2026-10-27T20:59:25; old copies beyond the 3 newest: 2869 / 0.0 GB
- `23:42:06`    oldest old copy is 0.1 days old
- `23:42:06` ✅ ops 6701 complete
