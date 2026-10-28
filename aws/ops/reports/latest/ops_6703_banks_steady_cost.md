# ops 6703 - the steady day after the banks filled themselves (Oct 27), and November (read-only)

**Status:** success  
**Duration:** 72.1s  
**Finished:** 2026-10-28T16:22:25+00:00  

## Data

| archive_copies | day | day_total | egress_gb | estimated | lambda_gbs | live_puts | november_support | november_total | november_usage | writes |
|---|---|---|---|---|---|---|---|---|---|---|
| 292585 | 2026-10-27 | 8.34 | 3.4 | True | 126990 | 318964 | 29.0 | 262.31 | 233.31 | 270949 |

## Log
## A. Cost Explorer per day

- `16:21:13` 2026-10-25 total $6.27 | Amazon Simple Storage Servic $3.45, AWS Lambda $1.91, AmazonCloudWatch $0.48, AWS Secrets Manager $0.12, Amazon EC2 Container Registr $0.09, AWS Cost Explorer $0.08, Amazon DynamoDB $0.07, EC2 - Other $0.04
- `16:21:13` 2026-10-26 total $13.18 | Amazon Simple Storage Servic $9.20, AWS Lambda $2.91, AmazonCloudWatch $0.60, AWS Cost Explorer $0.12, AWS Secrets Manager $0.12, Amazon EC2 Container Registr $0.09, Amazon DynamoDB $0.07, EC2 - Other $0.04
- `16:21:13` 2026-10-27 total $8.34 | Amazon Simple Storage Servic $5.25, AWS Lambda $2.14, AmazonCloudWatch $0.53, AWS Secrets Manager $0.12, Amazon EC2 Container Registr $0.09, AWS Cost Explorer $0.08, Amazon DynamoDB $0.07, EC2 - Other $0.04
- `16:21:13` 2026-10-28 (today, partial) total $5.28 | Amazon Simple Storage Servic $4.66, AWS Lambda $0.43, AmazonCloudWatch $0.12, AWS Cost Explorer $0.02
- `16:21:14` 2026-10-25 S3         $3.45 | TimedStorage-ByteHrs $0.807 (35), Requests-Tier1 $0.796 (159178), Requests-INT-Tier1 $0.646 (129199), TimedStorage-SIA-ByteHrs $0.495 (40), DataTransfer-Out-Bytes $0.299 (3), Requests-Tier2 $0.225 (563086), TimedStorage-SIA-SmObjects $0.072 (6), USE1-StorageLens-ObjCount $0.026 (130090), Inventory-ObjectsListed $0.019 (7657707)
- `16:21:14` 2026-10-26 S3         $9.20 | Requests-Tier1 $3.574 (714710), Requests-INT-Tier1 $3.434 (686890), TimedStorage-ByteHrs $0.823 (36), TimedStorage-SIA-ByteHrs $0.493 (39), DataTransfer-Out-Bytes $0.367 (4), Requests-Tier2 $0.284 (710112), TimedStorage-SIA-SmObjects $0.072 (6), TimedStorage-INT-FA-ByteHrs $0.040 (2), USE1-StorageLens-ObjCount $0.026 (132024)
- `16:21:14` 2026-10-27 S3         $5.25 | Requests-Tier1 $1.595 (318964), Requests-INT-Tier1 $1.463 (292585), TimedStorage-ByteHrs $0.881 (38), TimedStorage-SIA-ByteHrs $0.493 (39), DataTransfer-Out-Bytes $0.309 (3), Requests-Tier2 $0.206 (514875), TimedStorage-INT-FA-ByteHrs $0.077 (3), TimedStorage-SIA-SmObjects $0.072 (6), EarlyDelete-SIA $0.036 (3)
- `16:21:14` 2026-10-28 S3         $4.66 | Requests-Tier1 $2.300 (460075), Requests-INT-Tier1 $2.299 (459795), Requests-Tier2 $0.039 (97807), Retrieval-SIA $0.011 (1), DataTransfer-Out-Bytes $0.007 (0)
- `16:21:14` 2026-10-25 Lambda     $1.91 | Lambda-GB-Second $1.855 (111319), Lambda-GB-Second-ARM $0.034 (2534), DataTransfer-Out-Bytes $0.011 (0), USE1-USW2-AWS-Out-Bytes $0.009 (0)
- `16:21:14` 2026-10-26 Lambda     $2.91 | Lambda-GB-Second $2.832 (169911), Lambda-GB-Second-ARM $0.038 (2834), DataTransfer-Out-Bytes $0.020 (0), USE1-USW2-AWS-Out-Bytes $0.013 (1)
- `16:21:14` 2026-10-27 Lambda     $2.14 | Lambda-GB-Second $2.079 (124726), Lambda-GB-Second-ARM $0.030 (2264), DataTransfer-Out-Bytes $0.016 (0), USE1-USW2-AWS-Out-Bytes $0.014 (1)
- `16:21:14` 2026-10-28 Lambda     $0.43 | Lambda-GB-Second $0.428 (25658)
- `16:21:14` 2026-10-25 CloudWatch $0.48 | CW:GMD-Metrics $0.301 (30097), CW:AlarmMonitorUsage $0.087 (1), CW:Requests $0.037 (3746), CW:MetricMonitorUsage $0.035 (0), USE1-TimedStorage-ByteHrs $0.017 (1)
- `16:21:14` 2026-10-26 CloudWatch $0.60 | CW:GMD-Metrics $0.418 (41776), CW:AlarmMonitorUsage $0.087 (1), CW:Requests $0.038 (3777), CW:MetricMonitorUsage $0.035 (0), USE1-TimedStorage-ByteHrs $0.017 (1), USE1-VendedLog-Bytes $0.005 (0)
- `16:21:14` 2026-10-27 CloudWatch $0.53 | CW:GMD-Metrics $0.344 (34436), CW:AlarmMonitorUsage $0.087 (1), CW:Requests $0.038 (3778), CW:MetricMonitorUsage $0.035 (0), USE1-TimedStorage-ByteHrs $0.015 (0)
- `16:21:14` 2026-10-28 CloudWatch $0.12 | CW:GMD-Metrics $0.076 (7622), CW:Requests $0.018 (1834), CW:AlarmMonitorUsage $0.018 (0)
## B. writes on 2026-10-27 per writer (access logs, sampled) against Oct 26

- `16:22:24` 259323 log objects, read 6000 (x43.2): ~270949 writes on 2026-10-27
- `16:22:24`   justhodl-bank-histories                          110125   (Oct 26: 639059)
- `16:22:24`   justhodl-worldbank-full                           53679   (Oct 26: 45082)
- `16:22:24`   justhodl-bank-factors                             28309   (Oct 26: 104248)
- `16:22:24`   justhodl-repo                                      8557
- `16:22:24`   justhodl-polygon-fx-regime                         7779
- `16:22:24`   justhodl-etf-constituents                          6310
- `16:22:24`   runner:github-actions-justhodl                     6007
- `16:22:24`   justhodl-polygon-bank                              5748   (Oct 26: 26792)
- `16:22:24`   justhodl-etf-global-desk                           4322
- `16:22:24`   justhodl-gdelt-full                                4278
- `16:22:24`   justhodl-polygon-options-flow                      4062
- `16:22:24`   justhodl-tail-risk                                 2420
- `16:22:24`   justhodl-risk-regime                               2204
- `16:22:24`   justhodl-eia-bank                                  2204
- `16:22:24`   justhodl-market-internals                          2161
- `16:22:24` top folders: data/warm/history-book/ 110125, data/warm/worldbank-full/ 53679, data/warm/factor-book/ 28309, data/etf-holdings-research/ 7088, audit-private/20260909-originals/fx-research/ 6223, data/warm/polygon-bank/ 5748, data/repo-history/ 4322, data/watchlist-insights/ 4322, data/warm/gdelt-full/ 4278, data/warm/fred-scoped/ 4235
## C. the banks' own run records

- `16:22:25` justhodl-bank-histories _state/state.json last_run: {"started": "2026-10-28T04:36:06.397706+00:00", "seconds": 790.1, "puts": 123637, "unchanged": 0}
- `16:22:25` justhodl-bank-factors factor-book/manifest.json last_run: {"started": "2026-10-28T02:50:45.112139+00:00", "seconds": 351.0, "symbols": 27138, "written": 27138, "unchanged": 0, "failed": 0, "bytes": 85607725}
- `16:22:25` justhodl-polygon-eod _state/state.json last_run: {"started": "2026-10-28T03:10:35+00:00", "rows": 12634, "tickers_written": 12634, "tickers_unchanged": 0, "unreadable": 0}
- `16:22:25` justhodl-polygon-bank _state/state.json last_run: {"started_at": "2026-10-28T07:48:56.325601+00:00", "calls": 2213, "retries": 103, "puts": 1212, "unchanged": 89, "put_bytes": 23982494, "seconds": 760.0, "finished_at": "2026-10-28T08:01:36.066744+00:00"}
## D. November from 2026-10-27

- `16:22:25` 2026-10-27 = $8.34 (estimated): S3 $5.25 (live PUT/COPY/LIST 318964 = $1.59, archive copies 292585 = $1.46, egress 3.4 GB = $0.31), Lambda $2.14 (126990 GB-s)
- `16:22:25` November from 2026-10-27: $8.34 x 30 = $250.18, minus a fresh month's free allowances ~$16.87 = $233.31 of usage, plus Business Support+ $29.00 = ~$262.31
- `16:22:25` ✅ read-only; nothing changed
