# ops 6870 - where November's money goes (read-only), 2026-11-24T15:40+00:00

**Status:** success  
**Duration:** 144.4s  
**Finished:** 2026-11-24T15:43:00+00:00  

## Data

| cheap_day | cheap_day_s3 | days | dear_day | dear_day_s3 | lambda_mtd | month_at_this_pace | month_to_date | per_day_final | s3_mtd | s3_share_pct | writes_cheap | writes_dear |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-22 | 3.49 | 24 | 2026-11-19 | 7.36 | 38.12 | 272.97 | 208.31 | 9.1 | 121.51 | 58 | 85054 | 458696 |

## Log
## A. Cost Explorer, 2026-11-01 to 2026-11-24

- `15:40:36` month to date $208.31 over 24 days (22 final): Amazon Simple Storage Service $121.51 (58%), AWS Lambda $38.12 (18%), AWS Business Support+ $29.00 (14%), AmazonCloudWatch $7.94 (4%), AWS Secrets Manager $2.79 (1%), Amazon EC2 Container Registry  $2.11 (1%), AWS Cost Explorer $1.87 (1%), Amazon DynamoDB $1.78 (1%), Amazon Route 53 $1.50 (1%)
- `15:40:36` per day, total / S3: 01 $32.61/$1.69, 02 $2.94/$2.40, 03 $7.85/$7.06, 04 $5.31/$4.50, 05 $7.97/$7.18, 06 $6.33/$4.31, 07 $9.23/$6.96, 08 $6.38/$4.09, 09 $9.99/$7.28, 10 $7.39/$4.61, 11 $10.22/$7.42, 12 $7.09/$4.40, 13 $10.11/$7.35, 14 $6.41/$3.93, 15 $9.87/$7.03, 16 $6.93/$3.97, 17 $10.71/$7.22, 18 $7.66/$4.07, 19 $11.20/$7.36, 20 $8.15/$4.45, 21 $9.55/$6.17, 22 $6.25/$3.49, 23 $6.37/$3.45, 24 $1.76/$1.10
- `15:40:37` CloudWatch month to date $7.94: CW:GMD-Metrics $6.77 (676,672), CW:AlarmMonitorUsage $1.10 (21), CW:MetricInsightAlarmUsage $0.08 (1)
- `15:40:37` Lambda month to date $38.12: Lambda-GB-Second $36.82 (2,609,459), Lambda-GB-Second-ARM $0.94 (70,182), USE1-USW2-AWS-Out-Bytes $0.34 (17)
- `15:40:37` S3 month to date $121.51: Requests-Tier1 $39.92 (7,984,653), Requests-INT-Tier1 $36.67 (7,334,958), TimedStorage-ByteHrs $20.94 (911), TimedStorage-SIA-ByteHrs $11.65 (932), Requests-Tier2 $4.61 (11,516,057), TimedStorage-INT-FA-ByteHrs $2.15 (93), TimedStorage-SIA-SmObjects $1.68 (134), USE1-StorageLens-ObjCount $1.45 (7,234,647), Inventory-ObjectsListed $0.80 (319,983,316), USW2-Requests-SIA-Tier1 $0.44 (44,134), Retrieval-SIA $0.39 (39), Monitoring-Automation-INT $0.28 (111,547)
- `15:40:37` S3 on the dear day 2026-11-19 ($7.36) against the cheap day 2026-11-22 ($3.49), by fee:
- `15:40:37`   Requests-INT-Tier1                 $2.58 (516,175)  against  $0.60 (119,219)  = +1.98
- `15:40:37`   Requests-Tier1                     $2.75 (549,353)  against  $0.83 (165,168)  = +1.92
- `15:40:37`   Requests-Tier2                     $0.25 (631,478)  against  $0.25 (616,924)  = +0.01
- `15:40:37`   TimedStorage-SIA-SmObjects         $0.07 (6)  against  $0.07 (6)  = +0.00
- `15:40:37`   TimedStorage-SIA-ByteHrs           $0.51 (41)  against  $0.51 (41)  = +0.00
- `15:40:37`   USE1-StorageLens-ObjCount          $0.07 (331,414)  against  $0.07 (336,186)  = -0.00
- `15:40:37`   Inventory-ObjectsListed            $0.04 (14,983,626)  against  $0.04 (15,377,664)  = -0.00
- `15:40:37`   Retrieval-SIA                      $0.02 (2)  against  $0.02 (2)  = -0.01
- `15:40:37`   TimedStorage-ByteHrs               $0.90 (39)  against  $0.91 (40)  = -0.01
- `15:40:37`   TimedStorage-INT-FA-ByteHrs        $0.12 (5)  against  $0.13 (6)  = -0.01
## B. who writes: the access logs of the dear day and the cheap day

- `15:41:58` 2026-11-19: 445682 log objects, read 5000 (x89.1): ~458695 writes
- `15:41:58`   justhodl-bank-histories                        352,089  (77%)  busiest hours UTC 11:00 183,443, 03:00 88,691, 12:00 79,955
- `15:41:58`   justhodl-bank-factors                           27,365  (6%)  busiest hours UTC 02:00 27,365
- `15:41:58`   justhodl-worldbank-full                         13,460  (3%)  busiest hours UTC 05:00 6,240, 07:00 5,705, 08:00 713, 06:00 446
- `15:41:58`   justhodl-polygon-fx-regime                      12,836  (3%)  busiest hours UTC 17:00 2,228, 18:00 2,139, 21:00 2,050, 19:00 2,050
- `15:41:58`   justhodl-polygon-bank                            9,181  (2%)  busiest hours UTC 14:00 4,457, 11:00 4,457, 15:00 267
- `15:41:58`   justhodl-polygon-eod                             8,646  (2%)  busiest hours UTC 03:00 8,646
- `15:41:58`   justhodl-finra-short                             4,546  (1%)  busiest hours UTC 01:00 4,546
- `15:41:58`   justhodl-repo                                    4,457  (1%)  busiest hours UTC 06:00 4,457
- `15:41:58`   justhodl-tail-risk                               3,298  (1%)  busiest hours UTC 21:00 1,515, 15:00 1,426, 18:00 178, 20:00 178
- `15:41:58`   justhodl-bottom-signals                          3,120  (1%)  busiest hours UTC 03:00 3,120
- `15:41:58`   justhodl-dxy-predict                             2,496  (1%)  busiest hours UTC 03:00 2,496
- `15:41:58`   justhodl-global-stress                           1,961  (0%)  busiest hours UTC 19:00 1,961
- `15:41:58`   justhodl-daily-report-v3                         1,604  (0%)  busiest hours UTC 15:00 1,604
- `15:41:58`   justhodl-etf-true-flows                          1,426  (0%)  busiest hours UTC 15:00 1,426
- `15:41:58`   folders: data/warm/history-book/ 352,089, data/warm/factor-book/ 27,365, data/warm/worldbank-full/ 13,460, audit-private/20260909-originals/fx-research/ 11,231, data/warm/polygon-bank/ 9,181, data/warm/polygon-full/ 9,181, data/warm/blackswan/ 5,616, data/short-volume-research/ 4,546, data/repo-history/ 4,457, data/(files) 2,942
- `15:42:59` 2026-11-22: 322173 log objects, read 5000 (x64.4): ~85053 writes
- `15:42:59`   justhodl-bank-histories                         32,217  (38%)  busiest hours UTC 03:00 32,217
- `15:42:59`   runner:github-actions-justhodl                  19,846  (23%)  busiest hours UTC 21:00 19,846
- `15:42:59`   justhodl-bank-factors                           10,503  (12%)  busiest hours UTC 02:00 10,503
- `15:42:59`   justhodl-polygon-bank                            4,897  (6%)  busiest hours UTC 10:00 3,222, 15:00 1,611, 13:00 64
- `15:42:59`   justhodl-etf-constituents                        3,222  (4%)  busiest hours UTC 22:00 3,222
- `15:42:59`   justhodl-repo                                    2,900  (3%)  busiest hours UTC 06:00 2,900
- `15:42:59`   justhodl-etf-fund-flows                          1,933  (2%)  busiest hours UTC 22:00 1,933
- `15:42:59`   justhodl-tradingview                             1,224  (1%)  busiest hours UTC 20:00 1,224
- `15:42:59`   justhodl-carry-surface                           1,160  (1%)  busiest hours UTC 13:00 1,160
- `15:42:59`   justhodl-symdir                                  1,160  (1%)  busiest hours UTC 23:00 1,160
- `15:42:59`   justhodl-eia-bank                                  838  (1%)  busiest hours UTC 21:00 709, 13:00 129
- `15:42:59`   justhodl-smart-money-cluster                       773  (1%)  busiest hours UTC 16:00 773
- `15:42:59`   justhodl-history-snapshotter                       451  (1%)  busiest hours UTC 13:00 451
- `15:42:59`   justhodl-fmp-walker                                322  (0%)  busiest hours UTC 23:00 258, 22:00 64
- `15:42:59`   folders: data/warm/history-book/ 32,217, audit-private/20260909-originals/financial-statement-research/ 19,846, data/warm/factor-book/ 10,503, data/warm/polygon-bank/ 4,897, audit-private/20260909-originals/etf-constituent-research/ 3,222, data/warm/nyfed-research/ 2,642, data/evidence/ 1,611, data/warm/fred-scoped/ 1,160, data/(files) 902, audit-private/20260909-originals/provider-fund-flow-research/ 902
- `15:42:59` the dear day's extra writes, by writer: justhodl-bank-histories +319871, runner:github-actions-justhodl -19845, justhodl-bank-factors +16862, justhodl-worldbank-full +13459, justhodl-polygon-fx-regime +12771, justhodl-polygon-eod +8646, justhodl-finra-short +4545, justhodl-polygon-bank +4284
## C. the banks' own run records (written against unchanged)

- `15:42:59` justhodl-bank-histories state.json: {"updated_at": "2026-11-24T03:15:59.400870+00:00", "phase": "STEADY"}
- `15:42:59`     last_run: {"started": "2026-11-24T03:05:29.134171+00:00", "seconds": 630.2, "puts": 23635, "unchanged": 250056}
- `15:42:59` justhodl-bank-factors manifest.json: {"schema": "factor-book/1.0", "version": "1.0.9", "engine": "justhodl-bank-factors", "updated_at": "2026-11-24T02:50:45.034366+00:00", "symbols": 27195, "universe": 27195, "common_stocks": 5261, "typed_by_polygon": 12975, "debt_or_structured": 173, "preferreds": 542, "funds": 4386, "warrants_units":
- `15:42:59`     last_run: {"started": "2026-11-24T02:50:45.034354+00:00", "seconds": 436.2, "symbols": 27195, "written": 27195, "unchanged": 0, "failed": 0, "bytes": 86936468}
- `15:42:59` justhodl-polygon-eod state.json: {"version": "1.0.0", "runs": 38, "phase": "STEADY", "key_status": "ok", "lease_until": 0, "updated_at": "2026-11-24T03:14:25+00:00"}
- `15:42:59`     last_run: {"started": "2026-11-24T03:10:35+00:00", "rows": 12854, "tickers_written": 12854, "tickers_unchanged": 0, "unreadable": 0}
- `15:42:59` justhodl-polygon-bank state.json: {"version": "1.0.0", "started_at": "2026-10-25T16:43:33.662760+00:00", "runs": 349, "key_status": "ok", "phase": "BACKFILL", "lease_until": 0, "updated_at": "2026-11-24T15:36:34.592973+00:00"}
- `15:42:59`     last_run: {"started_at": "2026-11-24T15:24:31.874315+00:00", "calls": 2013, "retries": 90, "puts": 841, "unchanged": 392, "put_bytes": 32130139, "seconds": 723.0, "finished_at": "2026-11-24T15:36:34.592943+00:00"}
- `15:43:00` justhodl-fmp-bank data/warm/fmp/_state/bank.json: NoSuchKey
- `15:43:00` justhodl-fmp-bank manifest.json: {"engine": "justhodl-fmp-walker", "version": "1.3.0", "spec_version": "1.2.1", "updated_at": "2026-11-23T23:37:49.950402+00:00", "objects": 85260, "bytes": 12842183113, "pages": 85260, "pages_bytes": 12842183113, "datasets": 78, "datasets_declared": 183, "phase": "BACKFILL", "key_status": "ok", "ban
- `15:43:00`     last_run: {"started_at": "2026-11-23T23:27:38.153597+00:00", "calls": 713, "retries": 0, "puts": 513, "unchanged": 17, "put_bytes": 274743623, "bytes_received": 1610820624, "bytes_budget": 1610612736, "seconds": 612.1, "finished_at": "2026-11-23T23:37:49.949359+00:00"}
- `15:43:00` justhodl-fmp-walker data/warm/fmp/_state/walker.json: NoSuchKey
- `15:43:00` justhodl-fmp-walker data/warm/fmp/walker-manifest.json: NoSuchKey
- `15:43:00` justhodl-eia-bank data/warm/eia/manifest.json: NoSuchKey
- `15:43:00` ✅ read-only; nothing changed
