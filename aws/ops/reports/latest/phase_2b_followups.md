# Phase 2b follow-ups: deletes + investigation + email dedup

**Status:** success  
**Duration:** 3.6s  
**Finished:** 2026-04-22T23:53:49+00:00  

## Data

| action | avg_ms | inv_30d | memory_mb | monthly_cost_usd | status | target | v2_sends_email | v3_sends_email | verdict |
|---|---|---|---|---|---|---|---|---|---|
| delete |  |  |  |  | deleted | nyfed-cmdi-fetcher |  |  |  |
| delete |  |  |  |  | deleted | nyfed-main-aggregator |  |  |  |
| cost-estimate | 319 | 31758 | 512 | 0.0909 |  | enhanced-openbb-handler |  |  |  |
| email-dedup |  |  |  |  |  |  | True | False | v2 is the ACTIVE email sender; v3 doesn't send email. v2 is needed. |

## Log
## A. Delete 2 truly-dead Lambdas

- `23:53:46` ✅   Lambda nyfed-cmdi-fetcher deleted
- `23:53:46`     Log group /aws/lambda/nyfed-cmdi-fetcher deleted
- `23:53:46` ✅   Lambda nyfed-main-aggregator deleted
- `23:53:47`     Log group /aws/lambda/nyfed-main-aggregator deleted
## B. Investigate enhanced-openbb-handler + its warmers

- `23:53:47`   enhanced-openbb-handler source files: 1
- `23:53:47`   Main file: lambda_function.py (2024 bytes)
- `23:53:47`   First 30 lines of lambda_function.py:
- `23:53:47`     import json
- `23:53:47`     import urllib.request
- `23:53:47`     from datetime import datetime
- `23:53:47`     
- `23:53:47`     def lambda_handler(event, context):
- `23:53:47`         path = event.get('path', '')
- `23:53:47`         FRED_KEY = "2f057499936072679d8843d7fce99989"
- `23:53:47`         
- `23:53:47`         # Map endpoints to FRED series IDs
- `23:53:47`         if '/unemployment' in path:
- `23:53:47`             series_id = 'UNRATE'
- `23:53:47`         elif '/money_measures' in path:
- `23:53:47`             series_id = 'M2SL'
- `23:53:47`         elif '/dashboard/overview' in path:
- `23:53:47`             # Get multiple indicators for dashboard
- `23:53:47`             indicators = {}
- `23:53:47`             for name, sid in [('unemployment', 'UNRATE'), ('gdp', 'GDP'), ('cpi', 'CPIAUCSL'), ('dxy', 'DTWEXBGS'), ('treasury_10y', 'DGS10')]:
- `23:53:47`                 url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&limit=1&sort_order=desc"
- `23:53:47`                 with urllib.request.urlopen(url) as response:
- `23:53:47`                     data = json.loads(response.read())
- `23:53:47`                     if data['observations']:
- `23:53:47`                         indicators[name] = {"value": float(data['observations'][0]['value']), "date": data['observations'][0]['date']}
- `23:53:47`             return {
- `23:53:47`                 'statusCode': 200,
- `23:53:47`                 'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
- `23:53:47`                 'body': json.dumps({"dashboard": {"indicators": indicators, "lastUpdated": datetime.now().isoformat()}})
- `23:53:47`             }
- `23:53:47`         else:
- `23:53:47`             series_id = 'UNRATE'  # Default
- `23:53:47`         
- `23:53:47`   Handler behavior:
- `23:53:47`     writes_s3: False
- `23:53:47`     reads_s3: False
- `23:53:47`     invokes_other: False
- `23:53:47`     http_responses: False
- `23:53:47`     openbb_calls: False
- `23:53:47`     polygon_calls: False
- `23:53:47`     writes_dynamo: False
- `23:53:47` 
- `23:53:47`   Warmer rules:
- `23:53:47`     lambda-warmer-system3: State=ENABLED Schedule=rate(5 minutes)
- `23:53:47`       → target: enhanced-openbb-handler
- `23:53:47`         payload: {"httpMethod":"GET","path":"/api/search","queryStringParameters":{"query":"warm"},"headers":{"X-Warmer":"true"}}
- `23:53:47`     lambda-warmer-system3-frequent: State=ENABLED Schedule=rate(2 minutes)
- `23:53:47`       → target: enhanced-openbb-handler
- `23:53:47`         payload: {"httpMethod":"GET","path":"/api/search","queryStringParameters":{"query":"warm"},"headers":{"X-Warmer":"true"}}
- `23:53:48` 
- `23:53:48`   Cost estimate (last 30 days):
- `23:53:48`     Invocations: 31,758
- `23:53:48`     Avg duration: 319 ms
- `23:53:48`     Memory: 512 MB
- `23:53:48`     Compute: $0.0845
- `23:53:48`     Requests: $0.0064
- `23:53:48`     Total: $0.0909 / month
## C. Compare email-reports-v2 vs daily-report-v3 email behavior

- `23:53:49`   justhodl-email-reports-v2:
- `23:53:49`     Code size: 18753 bytes, 1 file(s)
- `23:53:49`     Uses SES send_email: True
- `23:53:49`     HTML email: True
- `23:53:49`     From: raafouis@gmail.com
- `23:53:49` 
- `23:53:49`   justhodl-daily-report-v3:
- `23:53:49`     Code size: 94117 bytes, 1 file(s)
- `23:53:49`     Uses SES send_email: False
- `23:53:49`     HTML email: False
- `23:53:49` 
- `23:53:49`   v2 schedule: ['DailyEmailReportsV2', 'DailyEmailReportsV2_8AMET']
- `23:53:49`     DailyEmailReportsV2: cron(0 12 * * ? *) · State=ENABLED
- `23:53:49`     DailyEmailReportsV2_8AMET: cron(0 12 * * ? *) · State=ENABLED
- `23:53:49` 
- `23:53:49`   VERDICT: v2 is the ACTIVE email sender; v3 doesn't send email. v2 is needed.
- `23:53:49` Done
