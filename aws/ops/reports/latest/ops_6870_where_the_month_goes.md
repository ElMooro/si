# ops 6870 - where November's money goes (read-only), 2026-11-22T15:31+00:00

**Status:** success  
**Duration:** 109.0s  
**Finished:** 2026-11-22T15:33:03+00:00  

## Data

| cheap_day | cheap_day_s3 | days | dear_day | dear_day_s3 | lambda_mtd | month_at_this_pace | month_to_date | per_day_final | s3_mtd | s3_share_pct | writes_cheap | writes_dear |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-16 | 3.97 | 22 | 2026-11-19 | 7.36 | 34.11 | 276.57 | 195.12 | 9.22 | 114.14 | 58 | 233925 | 458696 |

## Log
## A. Cost Explorer, 2026-11-01 to 2026-11-22

- `15:31:14` month to date $195.12 over 22 days (20 final): Amazon Simple Storage Service $114.14 (58%), AWS Lambda $34.11 (17%), AWS Business Support+ $29.00 (15%), AmazonCloudWatch $7.11 (4%), AWS Secrets Manager $2.55 (1%), Amazon EC2 Container Registry  $1.92 (1%), AWS Cost Explorer $1.67 (1%), Amazon DynamoDB $1.61 (1%), Amazon Route 53 $1.50 (1%)
- `15:31:15` per day, total / S3: 01 $32.61/$1.69, 02 $2.94/$2.40, 03 $7.85/$7.06, 04 $5.31/$4.50, 05 $7.97/$7.18, 06 $6.33/$4.31, 07 $9.23/$6.96, 08 $6.38/$4.09, 09 $9.99/$7.28, 10 $7.39/$4.61, 11 $10.22/$7.42, 12 $7.09/$4.40, 13 $10.11/$7.35, 14 $6.41/$3.93, 15 $9.87/$7.03, 16 $6.93/$3.97, 17 $10.71/$7.22, 18 $7.66/$4.07, 19 $11.20/$7.36, 20 $8.15/$4.45, 21 $9.55/$6.17, 22 $1.20/$0.66
- `15:31:15` CloudWatch month to date $7.11: CW:GMD-Metrics $6.13 (612,542), CW:AlarmMonitorUsage $0.91 (19), CW:MetricInsightAlarmUsage $0.07 (1)
- `15:31:15` Lambda month to date $34.11: Lambda-GB-Second $32.97 (2,378,003), Lambda-GB-Second-ARM $0.82 (61,741), USE1-USW2-AWS-Out-Bytes $0.30 (15)
- `15:31:15` S3 month to date $114.14: Requests-Tier1 $38.10 (7,620,157), Requests-INT-Tier1 $35.38 (7,076,133), TimedStorage-ByteHrs $19.11 (831), TimedStorage-SIA-ByteHrs $10.64 (851), Requests-Tier2 $4.08 (10,191,902), TimedStorage-INT-FA-ByteHrs $1.87 (82), TimedStorage-SIA-SmObjects $1.53 (123), USE1-StorageLens-ObjCount $1.24 (6,216,704), Inventory-ObjectsListed $0.72 (289,444,712), USW2-Requests-SIA-Tier1 $0.39 (38,603), Retrieval-SIA $0.34 (34), Monitoring-Automation-INT $0.24 (97,315)
- `15:31:15` S3 on the dear day 2026-11-19 ($7.36) against the cheap day 2026-11-16 ($3.97), by fee:
- `15:31:15`   Requests-Tier1                     $2.75 (549,353)  against  $1.09 (217,348)  = +1.66
- `15:31:15`   Requests-INT-Tier1                 $2.58 (516,175)  against  $0.96 (191,781)  = +1.62
- `15:31:15`   Requests-Tier2                     $0.25 (631,478)  against  $0.19 (464,150)  = +0.07
- `15:31:15`   TimedStorage-INT-FA-ByteHrs        $0.12 (5)  against  $0.11 (5)  = +0.01
- `15:31:15`   TimedStorage-ByteHrs               $0.90 (39)  against  $0.89 (39)  = +0.01
- `15:31:15`   USE1-StorageLens-ObjCount          $0.07 (331,414)  against  $0.07 (327,278)  = +0.00
- `15:31:15`   Inventory-ObjectsListed            $0.04 (14,983,626)  against  $0.04 (14,954,268)  = +0.00
- `15:31:15`   TimedStorage-SIA-SmObjects         $0.07 (6)  against  $0.07 (6)  = -0.00
- `15:31:15`   TimedStorage-SIA-ByteHrs           $0.51 (41)  against  $0.51 (41)  = -0.00
## B. who writes: the access logs of the dear day and the cheap day

- `15:32:20` 2026-11-19: 445682 log objects, read 5000 (x89.1): ~458695 writes
- `15:32:20`   justhodl-bank-histories                        352,089  (77%)  busiest hours UTC 11:00 183,443, 03:00 88,691, 12:00 79,955
- `15:32:20`   justhodl-bank-factors                           27,365  (6%)  busiest hours UTC 02:00 27,365
- `15:32:20`   justhodl-worldbank-full                         13,460  (3%)  busiest hours UTC 05:00 6,240, 07:00 5,705, 08:00 713, 06:00 446
- `15:32:20`   justhodl-polygon-fx-regime                      12,836  (3%)  busiest hours UTC 17:00 2,228, 18:00 2,139, 21:00 2,050, 19:00 2,050
- `15:32:20`   justhodl-polygon-bank                            9,181  (2%)  busiest hours UTC 14:00 4,457, 11:00 4,457, 15:00 267
- `15:32:20`   justhodl-polygon-eod                             8,646  (2%)  busiest hours UTC 03:00 8,646
- `15:32:20`   justhodl-finra-short                             4,546  (1%)  busiest hours UTC 01:00 4,546
- `15:32:20`   justhodl-repo                                    4,457  (1%)  busiest hours UTC 06:00 4,457
- `15:32:20`   justhodl-tail-risk                               3,298  (1%)  busiest hours UTC 21:00 1,515, 15:00 1,426, 18:00 178, 20:00 178
- `15:32:20`   justhodl-bottom-signals                          3,120  (1%)  busiest hours UTC 03:00 3,120
- `15:32:20`   justhodl-dxy-predict                             2,496  (1%)  busiest hours UTC 03:00 2,496
- `15:32:20`   justhodl-global-stress                           1,961  (0%)  busiest hours UTC 19:00 1,961
- `15:32:20`   justhodl-daily-report-v3                         1,604  (0%)  busiest hours UTC 15:00 1,604
- `15:32:20`   justhodl-etf-true-flows                          1,426  (0%)  busiest hours UTC 15:00 1,426
- `15:32:20`   folders: data/warm/history-book/ 352,089, data/warm/factor-book/ 27,365, data/warm/worldbank-full/ 13,460, audit-private/20260909-originals/fx-research/ 11,231, data/warm/polygon-bank/ 9,181, data/warm/polygon-full/ 9,181, data/warm/blackswan/ 5,616, data/short-volume-research/ 4,546, data/repo-history/ 4,457, data/(files) 2,942
- `15:33:03` 2026-11-16: 276966 log objects, read 5000 (x55.4): ~233925 writes
- `15:33:03`   justhodl-bank-histories                        162,524  (69%)  busiest hours UTC 03:00 162,524
- `15:33:03`   justhodl-bank-factors                           16,618  (7%)  busiest hours UTC 02:00 16,618
- `15:33:03`   justhodl-polygon-bank                            8,531  (4%)  busiest hours UTC 12:00 5,539, 10:00 2,548, 14:00 332, 15:00 111
- `15:33:03`   justhodl-etf-constituents                        8,364  (4%)  busiest hours UTC 22:00 8,364
- `15:33:03`   justhodl-polygon-fx-regime                       5,816  (2%)  busiest hours UTC 21:00 3,213, 13:00 1,219, 14:00 665, 16:00 277
- `15:33:03`   justhodl-worldbank-wgi                           5,539  (2%)  busiest hours UTC 03:00 5,539
- `15:33:03`   justhodl-tail-risk                               4,210  (2%)  busiest hours UTC 21:00 1,606, 16:00 1,440, 18:00 831, 14:00 111
- `15:33:03`   justhodl-tradingview                             2,714  (1%)  busiest hours UTC 20:00 2,714
- `15:33:03`   justhodl-repo                                    2,659  (1%)  busiest hours UTC 06:00 2,659
- `15:33:03`   justhodl-etf-fund-flows                          2,271  (1%)  busiest hours UTC 22:00 2,271
- `15:33:03`   justhodl-symdir                                  2,105  (1%)  busiest hours UTC 14:00 1,828, 19:00 277
- `15:33:03`   justhodl-risk-regime                             1,828  (1%)  busiest hours UTC 16:00 942, 18:00 443, 14:00 222, 19:00 111
- `15:33:03`   justhodl-polygon-options-flow                    1,551  (1%)  busiest hours UTC 19:00 1,551
- `15:33:03`   justhodl-implied-prob                              942  (0%)  busiest hours UTC 18:00 499, 21:00 388, 14:00 55
- `15:33:03`   folders: data/warm/history-book/ 162,524, data/warm/factor-book/ 16,618, data/warm/polygon-bank/ 8,531, data/etf-holdings-research/ 5,595, data/warm/worldbank-wgi/ 5,539, audit-private/20260909-originals/fx-research/ 4,930, data/warm/fred-scoped/ 4,542, audit-private/20260909-originals/tail-research/ 3,047, audit-private/20260909-originals/etf-constituent-research/ 2,770, data/evidence/ 2,327
- `15:33:03` the dear day's extra writes, by writer: justhodl-bank-histories +189565, justhodl-worldbank-full +12628, justhodl-bank-factors +10746, justhodl-polygon-eod +8646, justhodl-etf-constituents -8364, justhodl-polygon-fx-regime +7019, justhodl-worldbank-wgi -5539, justhodl-finra-short +4545
## C. the banks' own run records (written against unchanged)

- `15:33:03` justhodl-bank-histories state.json: {"updated_at": "2026-11-22T03:14:12.536609+00:00", "phase": "STEADY"}
- `15:33:03`     last_run: {"started": "2026-11-22T03:05:28.737223+00:00", "seconds": 523.8, "puts": 26051, "unchanged": 247032}
- `15:33:03` justhodl-bank-factors manifest.json: {"schema": "factor-book/1.0", "version": "1.0.9", "engine": "justhodl-bank-factors", "updated_at": "2026-11-22T02:50:45.138220+00:00", "symbols": 27128, "universe": 27128, "common_stocks": 5254, "typed_by_polygon": 12941, "debt_or_structured": 172, "preferreds": 542, "funds": 4385, "warrants_units":
- `15:33:03`     last_run: {"started": "2026-11-22T02:50:45.138206+00:00", "seconds": 431.2, "symbols": 27128, "written": 27128, "unchanged": 0, "failed": 0, "bytes": 86594320}
- `15:33:03` justhodl-polygon-eod state.json: {"version": "1.0.0", "runs": 36, "phase": "STEADY", "key_status": "ok", "lease_until": 0, "updated_at": "2026-11-22T03:10:35+00:00"}
- `15:33:03`     last_run: {"started": "2026-11-22T03:10:35+00:00", "rows": 0, "tickers_written": 0, "tickers_unchanged": 0, "unreadable": 0}
- `15:33:03` justhodl-polygon-bank state.json: {"version": "1.0.0", "started_at": "2026-10-25T16:43:33.662760+00:00", "runs": 328, "key_status": "ok", "phase": "BACKFILL", "lease_until": 0, "updated_at": "2026-11-22T15:10:05.573679+00:00"}
- `15:33:03`     last_run: {"started_at": "2026-11-22T14:57:25.139835+00:00", "calls": 2080, "retries": 132, "puts": 785, "unchanged": 369, "put_bytes": 39610064, "seconds": 760.6, "finished_at": "2026-11-22T15:10:05.573659+00:00"}
- `15:33:03` justhodl-fmp-bank data/warm/fmp/_state/bank.json: NoSuchKey
- `15:33:03` justhodl-fmp-bank manifest.json: {"engine": "justhodl-fmp-walker", "version": "1.3.0", "spec_version": "1.2.1", "updated_at": "2026-11-21T23:37:53.798473+00:00", "objects": 67431, "bytes": 11345487597, "pages": 67431, "pages_bytes": 11345487597, "datasets": 78, "datasets_declared": 183, "phase": "BACKFILL", "key_status": "ok", "ban
- `15:33:03`     last_run: {"started_at": "2026-11-21T23:27:38.025213+00:00", "calls": 614, "retries": 16, "puts": 405, "unchanged": 16, "put_bytes": 153905321, "bytes_received": 889106349, "bytes_budget": 1610612736, "seconds": 616.0, "finished_at": "2026-11-21T23:37:53.797607+00:00", "bandwidth_hold_until": 1795376273.7976165}
- `15:33:03` justhodl-fmp-walker data/warm/fmp/_state/walker.json: NoSuchKey
- `15:33:03` justhodl-fmp-walker data/warm/fmp/walker-manifest.json: NoSuchKey
- `15:33:03` justhodl-eia-bank data/warm/eia/manifest.json: NoSuchKey
- `15:33:03` ✅ read-only; nothing changed
