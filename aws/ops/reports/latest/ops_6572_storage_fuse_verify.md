# ops 6572 - storage fuse v1.5.0: rights, selftest, live dry run

**Status:** success  
**Duration:** 23.1s  
**Finished:** 2026-10-20T23:30:33+00:00  

## Data

| delta_gb | dry_level | s3_usd_yesterday | selftest_ok | version |
|---|---|---|---|---|
| 0.64 | None | None | True | 1.5.0 |

## Log
- `23:30:10` ✅ governor role: + ListMetrics, Get/PutBucketVersioning on justhodl-dashboard-live only (17 statements)
- `23:30:32` governor version 1.5.0, stage FREEZE
- `23:30:33` storage_selftest: ok=True scenarios={"quiet": null, "warn": "warn", "trip": "trip", "objects": "trip", "two_warns": "trip", "second": "second"} live={"days": {"2026-10-14": {"gb": 98774.016, "objects": 16351308}, "2026-10-15": {"gb": 135574.958, "objects": 18469661}, "2026-10-16": {"gb": 2114.494, "objects": 6795447}, "2026-10-17": {"gb": 1060.503, "objects": 3950632}, "2026-10-18": {"gb": 1060.677, "objects": 3966082}, "2026-10-19": {"gb": 1061.32, "objects": 3981806}}, "versioning": "Enabled"}
- `23:30:33` storage_check (dry): ran=True level=None growth +0.6 GB (+0.1%), +15724 objects over 2026-10-19; bucket 1061.32 GB / 3981806 objects; S3 yesterday $None
- `23:30:33` ✅ storage fuse live: rights in place, logic verified, bucket read end to end; nothing held
