# ops 5165 -- stand-down corrections + expected landing

**Status:** success  
**Duration:** 1.7s  
**Finished:** 2026-09-03T18:25:42+00:00  

## Data

| line | section | usd_day | usd_month |
|---|---|---|---|
| Lambda fleet, everything else (last 12h) | landing | 2.73 | 81.9 |
| justhodl-repo (daily now, was 288/day) | landing | 0.05 | 1.5 |
| justhodl-census-us econ (hourly, 11/12 shards COMPLETE) | landing | 0.6 | 18.0 |
| justhodl-ecb-deep refresh (6h, was 10 min) | landing | 0.02 | 0.6 |
| justhodl-sdmx-walker (OECD temp schedules removed) | landing | 0.05 | 1.5 |
| justhodl-fundamental-census (twice monthly) | landing | 0.02 | 0.6 |
| S3 requests (repo PUT storm removed) | landing | 0.8 | 24.0 |
| S3 storage after dead-version purge (~600 GB) | landing | 0.45 | 13.5 |
| S3 GET/HEAD + access logs + Storage Lens | landing | 0.55 | 16.5 |
| DynamoDB, Secrets Manager, Route 53, CloudWatch, ECR/EBS | landing | 0.45 | 13.5 |
| us-west-2 DR mirror (HOLD -- Khalid's call) | landing | 1.15 | 34.5 |
| SnapStart cache on justhodl-ai-chat (HOLD) | landing | 0.57 | 17.1 |

## Log
## 1. fundamental-census: drop the daily schedule ops 5164 added; twice-monthly design stands

- `18:25:41` ✅    deleted justhodl-fundamental-census-daily (cron(15 3 * * ? *))
- `18:25:41` ✅    designed cadence intact: fundamental-census-sched cron(0 6 1,15 * ? *) ENABLED
## 2. ecb-deep: refresh mode (58/58 complete) -> rate(6 hours)

- `18:25:41` ✅    justhodl-ecb-deep-10min rate(10 minutes) -> rate(6 hours)
## 3. Storage tracking + purge rule

- `18:25:41`    2026-08-28  2851 GB
- `18:25:41`    2026-08-29  1642 GB
- `18:25:41`    2026-08-30  1701 GB
- `18:25:41`    2026-08-31  1917 GB
- `18:25:41`    2026-09-01  1954 GB
- `18:25:42`    purge rule present: True -> {"Expiration": {"ExpiredObjectDeleteMarker": true}, "ID": "ops5164-purge-dead-versions-data", "Filter": {"Prefix": "data/"}, "Status": "Enabled", "NoncurrentVersionExpiration": {"NoncurrentDays": 1}}
## 4. Expected September landing (from ops-5164 last-12h numbers, stood-down lines removed)

- `18:25:42`    Lambda fleet, everything else (last 12h)                   $ 2.73/day   measured, minus the lines below
- `18:25:42`    justhodl-repo (daily now, was 288/day)                     $ 0.05/day   1 run x 337s x 1536MB
- `18:25:42`    justhodl-census-us econ (hourly, 11/12 shards COMPLETE)    $ 0.60/day   288 shard runs x ~60s + 15-min heartbeat
- `18:25:42`    justhodl-ecb-deep refresh (6h, was 10 min)                 $ 0.02/day   4 runs x 51s x 4096MB
- `18:25:42`    justhodl-sdmx-walker (OECD temp schedules removed)         $ 0.05/day   remaining rules only
- `18:25:42`    justhodl-fundamental-census (twice monthly)                $ 0.02/day   design cadence
- `18:25:42`    S3 requests (repo PUT storm removed)                       $ 0.80/day   was $3.5-4.0/day Tier-1
- `18:25:42`    S3 storage after dead-version purge (~600 GB)              $ 0.45/day   was $1.41/day at 1,954 GB
- `18:25:42`    S3 GET/HEAD + access logs + Storage Lens                   $ 0.55/day   edge reads
- `18:25:42`    DynamoDB, Secrets Manager, Route 53, CloudWatch, ECR/EBS   $ 0.45/day   flat
- `18:25:42`    us-west-2 DR mirror (HOLD -- Khalid's call)                $ 1.15/day   $34.56/month until deleted
- `18:25:42`    SnapStart cache on justhodl-ai-chat (HOLD)                 $ 0.57/day   $17/month
- `18:25:42`    TOTAL                                                      $ 7.44/day = $223/month  (drop the two HOLD lines: $5.72/day = $172/month)
- `18:25:42`    reference: Aug 01-08 baseline $4.52/day = $135/month; Sep 01-02 actual $20.72/day = $622/month
- `18:25:42` ✅    ledger updated
- `18:25:42` ✅ ops 5165 complete
