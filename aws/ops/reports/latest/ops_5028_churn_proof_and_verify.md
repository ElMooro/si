## P0 proof on the HOT page range (page-3400+)

**Status:** success  
**Duration:** 3.3s  
**Finished:** 2026-08-29T13:16:39+00:00  

## Data

| churn_proven | concurrency | rule | worst_key_versions |
|---|---|---|---|
| True | 0 | DISABLED | 11870 |

## Log
- `13:16:39`   keys after page-3400.json: 66   versions seen: 12000
- `13:16:39`   page-3466.json           versions=11870  oldest=08-15 19:27  newest=08-29 13:09  median_gap=1.8 min
- `13:16:39`   page-3401.json           versions=   2  oldest=08-09 02:43  newest=08-09 02:44  median_gap=0.6 min
- `13:16:39`   page-3402.json           versions=   2  oldest=08-09 02:43  newest=08-09 02:44  median_gap=0.6 min
- `13:16:39`   page-3403.json           versions=   2  oldest=08-09 02:43  newest=08-09 02:44  median_gap=0.6 min
- `13:16:39`   page-3404.json           versions=   2  oldest=08-09 02:43  newest=08-09 02:44  median_gap=0.6 min
- `13:16:39`   page-3405.json           versions=   2  oldest=08-09 02:43  newest=08-09 02:44  median_gap=0.6 min
- `13:16:39`   page-3406.json           versions=   2  oldest=08-09 02:43  newest=08-09 02:44  median_gap=0.6 min
- `13:16:39`   page-3407.json           versions=   2  oldest=08-09 02:43  newest=08-09 02:44  median_gap=0.6 min
- `13:16:39`   bytes held by this key range: 2.98 GB
- `13:16:39`   CHURN PROVEN -- worst key carries 11870 versions; median gap ~5 min = the rule cadence
## P1 the writer is stopped

- `13:16:39`   rule justhodl-series-extractor-5min: DISABLED
- `13:16:39`   reserved concurrency: 0
- `13:16:39`   justhodl-series-extractor          invocations/h: 07=36 08=36 09=36 10=36 11=36 12=32
- `13:16:39`   justhodl-signal-registry-ingest    invocations/h: 
## P2 purge is progressing

- `13:16:39`   objects (all versions): 08-23=7.74M 08-24=8.63M 08-25=9.14M 08-26=9.69M 08-27=10.17M
- `13:16:39`   NOTE: this metric is daily and lags ~24-48h; the lifecycle sweep runs async -- expect the fall on the 2026-08-30/31 datapoints, not now
- `13:16:39`   lifecycle rules: ['archive-to-glacier-deep-after-90d', 'expire-old-versions-after-30d', 'expire-screener-snapshots-30d', 'jh-noncurrent-14d', 'justhodl-e9-warm-ia', 'justhodl-e9-raw-glacier-ir', 'justhodl-e9-attic-ia', 'ops5027-purge-dead-versions-providers']
- `13:16:39`   ops5027 purge rule present: True {"Expiration": {"ExpiredObjectDeleteMarker": true}, "ID": "ops5027-purge-dead-versions-providers", "Filter": {"Prefix": "data/providers/"}, "Status": "Enabled", "NoncurrentVersionE
## P3 v2 engine deployed (frozen, will not run)

- `13:16:39`   justhodl-series-extractor LastModified=2026-08-29T13:16:38.000+0000 CodeSize=104103
## P4 ledger

- `13:16:39`   -> data/ops/s3-anomaly-closeout.json
- `13:16:39` ops 5028 GREEN -- churn proven on the hot range, writer stopped, purge armed, v2 deployed frozen
