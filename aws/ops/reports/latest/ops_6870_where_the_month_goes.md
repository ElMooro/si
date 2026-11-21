# ops 6870 - where November's money goes (read-only), 2026-11-21T04:14+00:00

**Status:** success  
**Duration:** 131.9s  
**Finished:** 2026-11-21T04:17:04+00:00  

## Data

| cheap_day | cheap_day_s3 | days | dear_day | dear_day_s3 | lambda_mtd | month_at_this_pace | month_to_date | per_day_final | s3_mtd | s3_share_pct | writes_cheap | writes_dear |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-14 | 3.93 | 21 | 2026-11-19 | 7.36 | 31.07 | 278.25 | 183.87 | 9.27 | 107.16 | 58 | 210632 | 458696 |

## Log
## A. Cost Explorer, 2026-11-01 to 2026-11-21

- `04:14:53` month to date $183.87 over 21 days (19 final): Amazon Simple Storage Service $107.16 (58%), AWS Lambda $31.07 (17%), AWS Business Support+ $29.00 (16%), AmazonCloudWatch $6.51 (4%), AWS Secrets Manager $2.39 (1%), Amazon EC2 Container Registry  $1.80 (1%), AWS Cost Explorer $1.52 (1%), Amazon Route 53 $1.50 (1%), Amazon DynamoDB $1.49 (1%)
- `04:14:53` per day, total / S3: 01 $32.61/$1.69, 02 $2.94/$2.40, 03 $7.85/$7.06, 04 $5.31/$4.50, 05 $7.97/$7.18, 06 $6.33/$4.31, 07 $9.23/$6.96, 08 $6.38/$4.09, 09 $9.99/$7.28, 10 $7.39/$4.61, 11 $10.22/$7.42, 12 $7.09/$4.40, 13 $10.11/$7.35, 14 $6.41/$3.93, 15 $9.87/$7.03, 16 $6.93/$3.97, 17 $10.71/$7.22, 18 $7.66/$4.07, 19 $11.20/$7.36, 20 $7.64/$4.31, 21 $0.00/$0.00
- `04:14:54` CloudWatch month to date $6.51: CW:GMD-Metrics $5.66 (565,507), CW:AlarmMonitorUsage $0.79 (18), CW:MetricInsightAlarmUsage $0.07 (1)
- `04:14:54` Lambda month to date $31.07: Lambda-GB-Second $30.02 (2,201,259), Lambda-GB-Second-ARM $0.75 (56,227), USE1-USW2-AWS-Out-Bytes $0.28 (14)
- `04:14:54` S3 month to date $107.16: Requests-Tier1 $35.55 (7,110,833), Requests-INT-Tier1 $33.08 (6,615,695), TimedStorage-ByteHrs $18.20 (791), TimedStorage-SIA-ByteHrs $10.13 (810), Requests-Tier2 $3.77 (9,424,818), TimedStorage-INT-FA-ByteHrs $1.75 (76), TimedStorage-SIA-SmObjects $1.46 (117), USE1-StorageLens-ObjCount $1.18 (5,882,149), Inventory-ObjectsListed $0.68 (273,970,256), USW2-Requests-SIA-Tier1 $0.37 (36,758), Retrieval-SIA $0.31 (31), Monitoring-Automation-INT $0.23 (90,512)
- `04:14:54` S3 on the dear day 2026-11-19 ($7.36) against the cheap day 2026-11-14 ($3.93), by fee:
- `04:14:54`   Requests-Tier1                     $2.75 (549,353)  against  $1.03 (206,803)  = +1.71
- `04:14:54`   Requests-INT-Tier1                 $2.58 (516,175)  against  $0.92 (184,736)  = +1.66
- `04:14:54`   Requests-Tier2                     $0.25 (631,478)  against  $0.20 (508,599)  = +0.05
- `04:14:54`   TimedStorage-INT-FA-ByteHrs        $0.12 (5)  against  $0.10 (4)  = +0.02
- `04:14:54`   USE1-StorageLens-ObjCount          $0.07 (331,414)  against  $0.07 (325,019)  = +0.00
- `04:14:54`   Inventory-ObjectsListed            $0.04 (14,983,626)  against  $0.04 (14,883,729)  = +0.00
- `04:14:54`   TimedStorage-SIA-SmObjects         $0.07 (6)  against  $0.07 (6)  = -0.00
- `04:14:54`   TimedStorage-SIA-ByteHrs           $0.51 (41)  against  $0.51 (41)  = -0.00
- `04:14:54`   TimedStorage-ByteHrs               $0.90 (39)  against  $0.91 (40)  = -0.01
## B. who writes: the access logs of the dear day and the cheap day

- `04:16:13` 2026-11-19: 445682 log objects, read 5000 (x89.1): ~458695 writes
- `04:16:13`   justhodl-bank-histories                        352,089  (77%)  busiest hours UTC 11:00 183,443, 03:00 88,691, 12:00 79,955
- `04:16:13`   justhodl-bank-factors                           27,365  (6%)  busiest hours UTC 02:00 27,365
- `04:16:13`   justhodl-worldbank-full                         13,460  (3%)  busiest hours UTC 05:00 6,240, 07:00 5,705, 08:00 713, 06:00 446
- `04:16:13`   justhodl-polygon-fx-regime                      12,836  (3%)  busiest hours UTC 17:00 2,228, 18:00 2,139, 21:00 2,050, 19:00 2,050
- `04:16:13`   justhodl-polygon-bank                            9,181  (2%)  busiest hours UTC 14:00 4,457, 11:00 4,457, 15:00 267
- `04:16:13`   justhodl-polygon-eod                             8,646  (2%)  busiest hours UTC 03:00 8,646
- `04:16:13`   justhodl-finra-short                             4,546  (1%)  busiest hours UTC 01:00 4,546
- `04:16:13`   justhodl-repo                                    4,457  (1%)  busiest hours UTC 06:00 4,457
- `04:16:13`   justhodl-tail-risk                               3,298  (1%)  busiest hours UTC 21:00 1,515, 15:00 1,426, 18:00 178, 20:00 178
- `04:16:13`   justhodl-bottom-signals                          3,120  (1%)  busiest hours UTC 03:00 3,120
- `04:16:13`   justhodl-dxy-predict                             2,496  (1%)  busiest hours UTC 03:00 2,496
- `04:16:13`   justhodl-global-stress                           1,961  (0%)  busiest hours UTC 19:00 1,961
- `04:16:13`   justhodl-daily-report-v3                         1,604  (0%)  busiest hours UTC 15:00 1,604
- `04:16:13`   justhodl-etf-true-flows                          1,426  (0%)  busiest hours UTC 15:00 1,426
- `04:16:13`   folders: data/warm/history-book/ 352,089, data/warm/factor-book/ 27,365, data/warm/worldbank-full/ 13,460, audit-private/20260909-originals/fx-research/ 11,231, data/warm/polygon-bank/ 9,181, data/warm/polygon-full/ 9,181, data/warm/blackswan/ 5,616, data/short-volume-research/ 4,546, data/repo-history/ 4,457, data/(files) 2,942
- `04:17:03` 2026-11-14: 253896 log objects, read 5000 (x50.8): ~210632 writes
- `04:17:03`   justhodl-bank-histories                        110,292  (52%)  busiest hours UTC 03:00 110,292
- `04:17:03`   justhodl-bank-factors                           55,857  (27%)  busiest hours UTC 02:00 55,857
- `04:17:03`   justhodl-polygon-eod                            17,976  (9%)  busiest hours UTC 03:00 17,976
- `04:17:03`   justhodl-polygon-bank                            6,449  (3%)  busiest hours UTC 14:00 3,910, 09:00 2,539
- `04:17:03`   justhodl-etf-constituents                        5,078  (2%)  busiest hours UTC 22:00 5,078
- `04:17:03`   justhodl-repo                                    2,590  (1%)  busiest hours UTC 06:00 2,590
- `04:17:03`   justhodl-src-mirror                              2,539  (1%)  busiest hours UTC 02:00 2,539
- `04:17:03`   justhodl-liquidity-agent                         1,320  (1%)  busiest hours UTC 12:00 1,320
- `04:17:03`   justhodl-daily-report-v3                         1,219  (1%)  busiest hours UTC 16:00 965, 15:00 254
- `04:17:03`   justhodl-corporate-actions                       1,219  (1%)  busiest hours UTC 06:00 1,219
- `04:17:03`   justhodl-fedwatch-rate-probability                 914  (0%)  busiest hours UTC 23:00 914
- `04:17:03`   justhodl-13f-positions                             660  (0%)  busiest hours UTC 03:00 660
- `04:17:03`   justhodl-dollar-radar                              559  (0%)  busiest hours UTC 19:00 559
- `04:17:03`   justhodl-eurodollar-plumbing                       508  (0%)  busiest hours UTC 12:00 508
- `04:17:03`   folders: data/warm/history-book/ 110,292, data/warm/factor-book/ 55,857, data/warm/polygon-full/ 17,976, data/warm/polygon-bank/ 6,449, data/etf-holdings-research/ 5,078, data/warm/nyfed-research/ 2,539, data/repo-history/ 2,539, data/liquidity-agent-research/ 1,320, data/evidence/ 1,219, data/corporate-actions/ 1,219
- `04:17:03` the dear day's extra writes, by writer: justhodl-bank-histories +241796, justhodl-bank-factors -28492, justhodl-worldbank-full +13459, justhodl-polygon-fx-regime +12835, justhodl-polygon-eod -9329, justhodl-etf-constituents -5077, justhodl-finra-short +4444, justhodl-tail-risk +3298
## C. the banks' own run records (written against unchanged)

- `04:17:03` justhodl-bank-histories state.json: {"updated_at": "2026-11-21T03:18:28.134199+00:00", "phase": "BACKFILL"}
- `04:17:03`     last_run: {"started": "2026-11-21T03:05:32.582965+00:00", "seconds": 775.5, "puts": 123623, "unchanged": 0}
- `04:17:04` justhodl-bank-factors manifest.json: {"schema": "factor-book/1.0", "version": "1.0.9", "engine": "justhodl-bank-factors", "updated_at": "2026-11-21T02:50:44.919034+00:00", "symbols": 27127, "universe": 27127, "common_stocks": 5256, "typed_by_polygon": 12939, "debt_or_structured": 170, "preferreds": 542, "funds": 4385, "warrants_units":
- `04:17:04`     last_run: {"started": "2026-11-21T02:50:44.919018+00:00", "seconds": 415.8, "symbols": 27127, "written": 27127, "unchanged": 0, "failed": 0, "bytes": 86718540}
- `04:17:04` justhodl-polygon-eod state.json: {"version": "1.0.0", "runs": 35, "phase": "STEADY", "key_status": "ok", "lease_until": 0, "updated_at": "2026-11-21T03:13:29+00:00"}
- `04:17:04`     last_run: {"started": "2026-11-21T03:10:35+00:00", "rows": 12667, "tickers_written": 12667, "tickers_unchanged": 0, "unreadable": 0}
- `04:17:04` justhodl-polygon-bank state.json: {"version": "1.0.0", "started_at": "2026-10-25T16:43:33.662760+00:00", "runs": 303, "key_status": "ok", "phase": "BACKFILL", "lease_until": 0, "updated_at": "2026-11-20T22:54:26.698597+00:00"}
- `04:17:04`     last_run: {"started_at": "2026-11-20T22:41:46.154861+00:00", "calls": 2036, "retries": 140, "puts": 785, "unchanged": 323, "put_bytes": 30269203, "seconds": 760.8, "finished_at": "2026-11-20T22:54:26.698573+00:00"}
- `04:17:04` justhodl-fmp-bank data/warm/fmp/_state/bank.json: NoSuchKey
- `04:17:04` justhodl-fmp-bank manifest.json: {"engine": "justhodl-fmp-walker", "version": "1.3.0", "spec_version": "1.2.1", "updated_at": "2026-11-20T11:37:46.741915+00:00", "objects": 55764, "bytes": 10550829192, "pages": 55764, "pages_bytes": 10550829192, "datasets": 78, "datasets_declared": 183, "phase": "BACKFILL", "key_status": "ok", "ban
- `04:17:04`     last_run: {"started_at": "2026-11-20T11:29:07.644987+00:00", "calls": 333, "retries": 16, "puts": 269, "unchanged": 49, "put_bytes": 19700656, "bytes_received": 113710549, "bytes_budget": 1610612736, "seconds": 519.3, "finished_at": "2026-11-20T11:37:46.741031+00:00", "bandwidth_hold_until": 1795246666.7410426}
- `04:17:04` justhodl-fmp-walker data/warm/fmp/_state/walker.json: NoSuchKey
- `04:17:04` justhodl-fmp-walker data/warm/fmp/walker-manifest.json: NoSuchKey
- `04:17:04` justhodl-eia-bank data/warm/eia/manifest.json: NoSuchKey
- `04:17:04` ✅ read-only; nothing changed
