# ops 6612 - archive backfill #2 from the S3 Inventory (current versions only, keys not yet in the archive)

**Status:** success  
**Duration:** 198.3s  
**Finished:** 2026-10-22T16:13:54+00:00  

## Data

| failed | gb | inventory | job_id | null_versions | rows | status | succeeded | total |
|---|---|---|---|---|---|---|---|---|
| 0 | 586.0 | 2026-10-21T01-00Z | 646ccb69-2925-4af5-b176-b155200aacaf | 335 | 2086159 | Active | 0 | 2086159 |

## Log
## 1. current objects from the inventory

- `16:11:01` inventory 2026-10-21T01-00Z: 3381372 current objects, 1022.9 GB
## 2. what the archive already holds

- `16:13:02` archive: 1350879 current keys (1477 LIST calls, 145 s)
- `16:13:04` to copy: 2086159 objects, 586.0 GB; by prefix (top 20): [["data/providers/", 1135637], ["data/warm/", 771589], ["data/series-cache/", 66795], ["data/statement-research/", 13095], ["data/price-observations/", 12173], ["data/research-forecasts/", 9343], ["data/symdir/", 9335], ["data/provider-flow-research/", 8050], ["data/vintage-research/", 6428], ["data/raw/", 6147], ["data/research-network/", 5259], ["data/risk-gate-research/", 3818], ["data/xbrl-fundamentals/", 3381], ["data/short-volume-research/", 3375], ["data/xbrl-raw/", 3323], ["data/signal-board-research/", 2481], ["data/sector-tilt-research/", 2460], ["data/sector-fusion-research/", 2427], ["data/sector-capital-research/", 2418], ["data/sector-research/", 2312]]
## 3. manifest + job

- `16:13:09` manifest: s3://justhodl-ops-private-857687956942/batch-replication/manifests/20261022T161306Z-inventory.csv (2086159 rows, 335 null version ids, 240.9 MB, etag e37b3b9bbda5)
- `16:13:54` ✅ backfill #2 job 646ccb69-2925-4af5-b176-b155200aacaf created from 2086159 current objects not yet in the archive (Active); ops 6576 reads progress
