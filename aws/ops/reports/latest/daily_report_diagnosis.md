# Why is data.json stale? Diagnosing justhodl-daily-report-v3

**Status:** success  
**Duration:** 778.1s  
**Finished:** 2026-04-22T23:12:56+00:00  

## Data

| age_days | last_modified | name | object | schedule | size | source | state |
|---|---|---|---|---|---|---|---|
|  |  | justhodl-daily-8am |  | cron(0 13 * * ? *) |  | EB Rule | ENABLED |
|  |  | justhodl-daily-v3 |  | cron(0 13 * * ? *) |  | EB Rule | ENABLED |
|  |  | justhodl-v9-auto-refresh |  | rate(5 minutes) |  | EB Rule | ENABLED |
|  |  | justhodl-v9-evening |  | cron(0 23 ? * MON-FRI *) |  | EB Rule | ENABLED |
|  |  | justhodl-v9-morning |  | cron(0 13 ? * MON-FRI *) |  | EB Rule | ENABLED |
| 63 | 2026-02-18T13:00:53+00:00 |  | data.json |  | 60635 |  |  |

## Log
## Function configuration

- `22:59:58`   Runtime: python3.12
- `22:59:58`   Handler: lambda_function.lambda_handler
- `22:59:58`   Memory:  1024 MB · Timeout: 900s
- `22:59:58`   LastModified: 2026-03-05T03:23:09.000+0000
## Invocation history

- `23:00:00`   90d totals: 18785 invocations, 1971 errors
- `23:00:00`   Last 10 active days:
- `23:00:00`     2026-04-22: 11 invocations, 0 errors
- `23:00:00`     2026-04-21: 292 invocations, 0 errors
- `23:00:00`     2026-04-20: 292 invocations, 0 errors
- `23:00:00`     2026-04-19: 291 invocations, 0 errors
- `23:00:00`     2026-04-18: 290 invocations, 0 errors
- `23:00:00`     2026-04-17: 291 invocations, 0 errors
- `23:00:00`     2026-04-16: 292 invocations, 0 errors
- `23:00:00`     2026-04-15: 292 invocations, 0 errors
- `23:00:00`     2026-04-14: 292 invocations, 0 errors
- `23:00:00`     2026-04-13: 292 invocations, 0 errors
## EventBridge Rules targeting this function

- `23:00:00`   - `justhodl-daily-8am` State=ENABLED Schedule=cron(0 13 * * ? *)
- `23:00:00`   - `justhodl-daily-v3` State=ENABLED Schedule=cron(0 13 * * ? *)
- `23:00:00`   - `justhodl-v9-auto-refresh` State=ENABLED Schedule=rate(5 minutes)
- `23:00:00`   - `justhodl-v9-evening` State=ENABLED Schedule=cron(0 23 ? * MON-FRI *)
- `23:00:00`   - `justhodl-v9-morning` State=ENABLED Schedule=cron(0 13 ? * MON-FRI *)
## EventBridge Scheduler schedules targeting this function

- `23:00:00`   No Scheduler schedules target this function
## Recent error lines from CloudWatch Logs (last 7 days)

- `23:07:51`   (no errors found in last 7 days — but also likely no invocations)
## What S3 keys does the function reference in its code?

- `23:07:51`   JSON keys / S3 objects referenced in code:
- `23:07:51`     - data/ath.json
- `23:07:51`     - data/report.json
## data.json in S3

- `23:07:51`   data.json LastModified: 2026-02-18T13:00:53+00:00
- `23:07:51`   Age: 63 days
- `23:07:51`   Size: 60635 bytes
## Manually invoking justhodl-daily-report-v3 (RequestResponse, timeout 300s)

- `23:12:56` ✗   Invoke failed: Read timeout on endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-daily-report-v3/invocations"
- `23:12:56` Diagnosis complete
