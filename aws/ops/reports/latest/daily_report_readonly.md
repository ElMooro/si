# Read-only diagnosis: justhodl-daily-report-v3

**Status:** success  
**Duration:** 6.5s  
**Finished:** 2026-04-22T23:13:30+00:00  

## Data

| code_last_modified | day | env_keys | errors | invocations | max_duration_ms | memory | name | prop | runtime | schedule | source | state | timeout |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-03-05T03:23:09.000+0000 |  | EMAIL_FROM,EMAIL_TO,FRED_API_KEY,NEWS_API_KEY,POLYGON_API_KEY,S3_BUCKET |  |  |  | 1024 |  | config | python3.12 |  |  |  | 900 |
|  | 2026-04-22 |  | 0 | 3 | 256160 |  |  |  |  |  |  |  |  |
|  | 2026-04-21 |  | 0 | 292 | 380182 |  |  |  |  |  |  |  |  |
|  | 2026-04-20 |  | 0 | 292 | 352271 |  |  |  |  |  |  |  |  |
|  | 2026-04-19 |  | 0 | 291 | 319440 |  |  |  |  |  |  |  |  |
|  | 2026-04-18 |  | 0 | 290 | 445337 |  |  |  |  |  |  |  |  |
|  | 2026-04-17 |  | 0 | 291 | 427580 |  |  |  |  |  |  |  |  |
|  | 2026-04-16 |  | 0 | 292 | 347852 |  |  |  |  |  |  |  |  |
|  | 2026-04-15 |  | 0 | 292 | 379332 |  |  |  |  |  |  |  |  |
|  | 2026-04-14 |  | 0 | 292 | 402533 |  |  |  |  |  |  |  |  |
|  | 2026-04-13 |  | 0 | 292 | 510062 |  |  |  |  |  |  |  |  |
|  | 2026-04-12 |  | 0 | 291 | 353150 |  |  |  |  |  |  |  |  |
|  | 2026-04-11 |  | 0 | 290 | 374092 |  |  |  |  |  |  |  |  |
|  | 2026-04-10 |  | 0 | 291 | 322847 |  |  |  |  |  |  |  |  |
|  | 2026-04-09 |  | 0 | 292 | 663612 |  |  |  |  |  |  |  |  |
|  | 2026-04-08 |  | 0 | 292 | 351331 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | justhodl-daily-8am |  |  | cron(0 13 * * ? *) | EB Rule | ENABLED |  |
|  |  |  |  |  |  |  | justhodl-daily-v3 |  |  | cron(0 13 * * ? *) | EB Rule | ENABLED |  |
|  |  |  |  |  |  |  | justhodl-v9-auto-refresh |  |  | rate(5 minutes) | EB Rule | ENABLED |  |
|  |  |  |  |  |  |  | justhodl-v9-evening |  |  | cron(0 23 ? * MON-FRI *) | EB Rule | ENABLED |  |
|  |  |  |  |  |  |  | justhodl-v9-morning |  |  | cron(0 13 ? * MON-FRI *) | EB Rule | ENABLED |  |

## Log
## Function configuration

- `23:13:23`   Runtime: python3.12 · Handler: lambda_function.lambda_handler
- `23:13:23`   Memory: 1024 MB · Timeout: 900s
- `23:13:23`   Code LastModified: 2026-03-05T03:23:09.000+0000
- `23:13:23`   CodeSize: 29532 bytes
- `23:13:23`   Environment keys: ['EMAIL_FROM', 'EMAIL_TO', 'FRED_API_KEY', 'NEWS_API_KEY', 'POLYGON_API_KEY', 'S3_BUCKET']
## Invocation metrics, last 90 days

- `23:13:26`   90d totals → Invocations: 18789 · Errors: 1971 · Throttles: 0
- `23:13:26`   Most recent invocation day: 2026-04-22 (3 invoked)
- `23:13:26`   Oldest invocation day (in 90d window): 2026-02-17
- `23:13:26`   Last 15 days with any activity:
- `23:13:26`     2026-04-22  inv=  3  err= 0  maxDuration=256160ms
- `23:13:26`     2026-04-21  inv=292  err= 0  maxDuration=380182ms
- `23:13:26`     2026-04-20  inv=292  err= 0  maxDuration=352271ms
- `23:13:26`     2026-04-19  inv=291  err= 0  maxDuration=319440ms
- `23:13:26`     2026-04-18  inv=290  err= 0  maxDuration=445337ms
- `23:13:26`     2026-04-17  inv=291  err= 0  maxDuration=427580ms
- `23:13:26`     2026-04-16  inv=292  err= 0  maxDuration=347852ms
- `23:13:26`     2026-04-15  inv=292  err= 0  maxDuration=379332ms
- `23:13:26`     2026-04-14  inv=292  err= 0  maxDuration=402533ms
- `23:13:26`     2026-04-13  inv=292  err= 0  maxDuration=510062ms
- `23:13:26`     2026-04-12  inv=291  err= 0  maxDuration=353150ms
- `23:13:26`     2026-04-11  inv=290  err= 0  maxDuration=374092ms
- `23:13:26`     2026-04-10  inv=291  err= 0  maxDuration=322847ms
- `23:13:26`     2026-04-09  inv=292  err= 0  maxDuration=663612ms
- `23:13:26`     2026-04-08  inv=292  err= 0  maxDuration=351331ms
## EventBridge Rules targeting this function

- `23:13:26`   - `justhodl-daily-8am` | State=ENABLED | Schedule=cron(0 13 * * ? *)
- `23:13:26`   - `justhodl-daily-v3` | State=ENABLED | Schedule=cron(0 13 * * ? *)
- `23:13:26`   - `justhodl-v9-auto-refresh` | State=ENABLED | Schedule=rate(5 minutes)
- `23:13:26`   - `justhodl-v9-evening` | State=ENABLED | Schedule=cron(0 23 ? * MON-FRI *)
- `23:13:26`   - `justhodl-v9-morning` | State=ENABLED | Schedule=cron(0 13 ? * MON-FRI *)
## EventBridge Scheduler schedules targeting this function

- `23:13:26`   (none found)
## Recent errors/warnings from CloudWatch Logs (last 7 days)

- `23:13:29`   (no error-pattern matches in last 7 days)
## Last 20 log events (any content, last 7 days)

- `23:13:29`     [2026-04-15 23:13:49] START RequestId: 21107e3d-becc-402a-8a18-b820175bf157 Version: $LATEST
- `23:13:29`     [2026-04-15 23:13:49] [V10] Start 2026-04-15T23:13:49.368181
- `23:13:29`     [2026-04-15 23:14:11] FRED batch 1: 4 series
- `23:13:29`     [2026-04-15 23:14:24] FRED batch 6: 43 series
- `23:13:29`     [2026-04-15 23:14:39] FRED batch 11: 81 series
- `23:13:29`     [2026-04-15 23:15:02] FRED batch 16: 120 series
- `23:13:29`     [2026-04-15 23:15:17] FRED batch 21: 153 series
- `23:13:29`     [2026-04-15 23:15:31] FRED batch 26: 180 series
- `23:13:29`     [2026-04-15 23:15:41] [V10] FRED: 203/233 in 111.8s
- `23:13:29`     [2026-04-15 23:15:41] [V10] Fetching 188 stocks...
- `23:13:29`     [2026-04-15 23:15:42] Stocks batch 1: 5/5
- `23:13:29`     [2026-04-15 23:15:53] Stocks batch 11: 55/55
- `23:13:29`     [2026-04-15 23:16:04] Stocks batch 21: 105/105
- `23:13:29`     [2026-04-15 23:16:16] Stocks batch 31: 155/155
- `23:13:29`     [2026-04-15 23:16:23] [V10] Stocks: 187/188
- `23:13:29`     [2026-04-15 23:16:23] [V10] Crypto...
- `23:13:29`     [2026-04-15 23:16:24] [V10] Crypto: 25 coins
- `23:13:29`     [2026-04-15 23:16:24] [V10] ECB CISS...
- `23:13:29`     [2026-04-15 23:16:29] ECB CISS error CISS.D.U2.Z0Z.4F.EC.SS_MM.CON: 'utf-8' codec can't decode byte 0x8b in position 1: invalid start byte
- `23:13:29`     [2026-04-15 23:16:29] ECB CISS error CISS.D.U2.Z0Z.4F.EC.SS_FX.CON: 'utf-8' codec can't decode byte 0x8b in position 1: invalid start byte
## Code scan — what does the function write?

- `23:13:29`   put_object Bucket=s seen: []
- `23:13:29`   put_object Key=s seen:    ['data/ath.json', 'data/report.json']
- `23:13:29`   All JSON refs in code:    ['data/ath.json', 'data/report.json']
## Current data.json object

- `23:13:30`   LastModified: 2026-02-18T13:00:53+00:00  Age: 63 days  Size: 60635 bytes
- `23:13:30` Read-only diagnosis complete
