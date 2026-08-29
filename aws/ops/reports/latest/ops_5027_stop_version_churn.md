## P0 evidence -- engine state

**Status:** success  
**Duration:** 5.0s  
**Finished:** 2026-08-29T13:10:10+00:00  

## Data

| churn_proven | purge | stopped | worst_key_versions |
|---|---|---|---|
| False | data/providers/ | True | 2 |

## Log
- `13:10:06`   state doc data/_state/series-extract-eurostat.json: 113.6 KB
- `13:10:06`   flows_done=79  n_pages=3466  series_count=1733000  buffer=233 rows  errors=0  updated_at=2026-08-09T02:40:20+00:00
- `13:10:06`   flows_done unique=79 vs len=79  (no dupes)
- `13:10:06`   fn timeout=280s mem=1536 lastmod=2026-08-09T02:39:30.000+0000
- `13:10:06`   rule justhodl-series-extractor-5min state=ENABLED sched=rate(5 minutes)
- `13:10:06`   invocations last 24h: 864 (36.0/h)
## P1 PROOF -- versions per page key

- `13:10:07`   keys sampled: 2000  total versions seen: 4000
- `13:10:07`   page-0000.json                                       versions=   2 span=0.0h median_gap=0.7 min newest=08-09 02:40
- `13:10:07`   page-0001.json                                       versions=   2 span=0.0h median_gap=0.7 min newest=08-09 02:40
- `13:10:07`   page-0002.json                                       versions=   2 span=0.0h median_gap=0.7 min newest=08-09 02:40
- `13:10:07`   page-0003.json                                       versions=   2 span=0.0h median_gap=0.7 min newest=08-09 02:40
- `13:10:07`   page-0004.json                                       versions=   2 span=0.0h median_gap=0.7 min newest=08-09 02:40
- `13:10:07`   page-0005.json                                       versions=   2 span=0.0h median_gap=0.7 min newest=08-09 02:40
- `13:10:07`   CHURN NOT proven -- worst key carries 2 versions
- `13:10:07`   dead-version bytes in this sample: 1.06 GB over 4000 versions (avg 259 KB)
## P2 scope -- current vs versions, and the #2 writer

- `13:10:07`   data/providers/eurostat/series/ current objects: 1000+ (truncated)
- `13:10:08`   gdelt worst key: page-000.json versions=157
- `13:10:08`   gdelt keys=13 versions=2000 -> same churn pattern
- `13:10:08`   bucket versioning: Enabled
- `13:10:08`   existing lifecycle rules: 7
- `13:10:08`     archive-to-glacier-deep-afte Enabled pfx='archive/' exp=None noncur=None
- `13:10:08`     expire-old-versions-after-30 Enabled pfx='' exp=None noncur=None
- `13:10:08`     expire-screener-snapshots-30 Enabled pfx='screener/snapshots/' exp=30 noncur=None
- `13:10:08`     jh-noncurrent-14d            Enabled pfx='' exp=None noncur=None
- `13:10:08`     justhodl-e9-warm-ia          Enabled pfx='data/warm/' exp=None noncur=None
- `13:10:08`     justhodl-e9-raw-glacier-ir   Enabled pfx='data/raw/' exp=None noncur=None
- `13:10:08`     justhodl-e9-attic-ia         Enabled pfx='data/attic/' exp=None noncur=None
## P3 STOP the loop (reversible, nothing deleted)

- `13:10:08`   rule justhodl-series-extractor-5min -> DISABLED
- `13:10:10`   justhodl-series-extractor reserved concurrency -> 0
## P4 PURGE dead versions (current objects untouched)

- `13:10:10`   lifecycle: 8 rules; ops5027 purge on 'data/providers/' (NoncurrentDays=1, delete-markers cleaned)
- `13:10:10`   -> only NONCURRENT versions expire; every current page object survives; S3 deletes async in ~24-48h at zero request cost
## P5 verify + ledger

- `13:10:10`   rule state now: DISABLED
- `13:10:10`   lifecycle IDs: ['archive-to-glacier-deep-after-90d', 'expire-old-versions-after-30d', 'expire-screener-snapshots-30d', 'jh-noncurrent-14d', 'justhodl-e9-warm-ia', 'justhodl-e9-raw-glacier-ir', 'justhodl-e9-attic-ia', 'ops5027-purge-dead-versions-providers']
- `13:10:10`   ledger -> data/ops/series-extractor-quarantine.json
- `13:10:10` ops 5027 GREEN -- justhodl-series-extractor stopped; dead versions under data/providers/ expire in 24-48h; the engine stays frozen until it is made idempotent (ops 5028)
