# Smoke test 6 migrated Lambdas

**Status:** success  
**Duration:** 52.8s  
**Finished:** 2026-04-22T23:32:33+00:00  

## Data

| fn_error | lambda_name | retired_model | stale | status_code | verdict |
|---|---|---|---|---|---|
| Unhandled | justhodl-morning-intelligence | — | — | 200 | FUNCTION_ERROR |
| — | justhodl-investor-agents | — | — | 200 | OK |
| — | justhodl-bloomberg-v8 | — | — | 200 | OK |
| — | justhodl-chat-api | — | — | 200 | STATUS_ERROR |
| — | justhodl-crypto-intel | — | — | 200 | OK |
| Unhandled | justhodl-signal-logger | — | — | 200 | FUNCTION_ERROR |

## Log
## justhodl-morning-intelligence

- `23:31:40`   Note: Runs tomorrow at 8am ET. Writes briefing to S3.
- `23:31:42` ✗   ✗ FunctionError=Unhandled
- `23:31:42`   Response preview: {"errorMessage": "float() argument must be a string or a real number, not 'dict'", "errorType": "TypeError", "requestId": "9fec8c90-7dd7-4715-99fc-f2816312798a", "stackTrace": ["  File \"/var/task/lam
- `23:31:42`   Log tail (last 10 lines):
- `23:31:42`     [S3] learning/prompt_templates.json: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `23:31:42`     [WEIGHTS] 12 loaded
- `23:31:42`     [ERROR] TypeError: float() argument must be a string or a real number, not 'dict'
- `23:31:42`     Traceback (most recent call last):
- `23:31:42`       File "/var/task/lambda_function.py", line 337, in lambda_handler
- `23:31:42`         m=extract_metrics(all_data,weights)
- `23:31:42`       File "/var/task/lambda_function.py", line 148, in extract_metrics
- `23:31:42`         "khalid_adj":round(float(ki)*kw,1) if ki else 0,
- `23:31:42`     END RequestId: 9fec8c90-7dd7-4715-99fc-f2816312798a
- `23:31:42`     REPORT RequestId: 9fec8c90-7dd7-4715-99fc-f2816312798a	Duration: 1185.24 ms	Billed Duration: 1790 ms	Memory Size: 256 MB	Max Memory Used: 115 MB	Init Duration: 604.61 ms	
## justhodl-investor-agents

- `23:31:42`   Note: 6-agent consensus analysis. Uses Claude Haiku 4.5.
- `23:31:49` ✅   Status 200 · 7690 bytes · no stale signals
- `23:31:49`   Body preview: {"statusCode": 200, "headers": {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Allow-Methods": "POST,OPTIONS", "Content-Type": "application/json"},
## justhodl-bloomberg-v8

- `23:31:49`   Note: Terminal UI data source.
- `23:31:50` ✅   Status 200 · 22867 bytes · no stale signals
- `23:31:50`   Body preview: {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Content-Type"}, "body": "{\"timestamp\": \"2026-04-22 18:31:49 
## justhodl-chat-api

- `23:31:50`   Note: Second chat endpoint. Hardcoded retired model — EXPECTED FAIL.
- `23:31:52` ✗   ✗ Response indicates error
- `23:31:52`   Body: {"statusCode": 500, "headers": {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Allow-Methods": "POST,OPTIONS"}, "body": "{\"error\": \"HTTP Error 400: Bad Request\"}"}
## justhodl-crypto-intel

- `23:31:52`   Note: Crypto dashboard data. Hardcoded retired model in analysis path — may partial-fail.
- `23:32:31` ✅   Status 200 · 168 bytes · no stale signals
- `23:32:31`   Body preview: {"statusCode": 200, "body": "{\"status\": \"published\", \"risk\": 53, \"regime\": \"ELEVATED\", \"ok\": 12, \"total\": 17, \"ai\": true, \"tfs\": 15, \"time\": 37.7}"}
## justhodl-signal-logger

- `23:32:31`   Note: Learning system — logs signals to DynamoDB.
- `23:32:33` ✗   ✗ FunctionError=Unhandled
- `23:32:33`   Response preview: {"errorMessage": "float() argument must be a string or a real number, not 'dict'", "errorType": "TypeError", "requestId": "bbd63f10-72a3-41b4-bb70-2e1eb8ef7710", "stackTrace": ["  File \"/var/task/lam
- `23:32:33`   Log tail (last 10 lines):
- `23:32:33`     START RequestId: bbd63f10-72a3-41b4-bb70-2e1eb8ef7710 Version: $LATEST
- `23:32:33`     [ERROR] TypeError: float() argument must be a string or a real number, not 'dict'
- `23:32:33`     Traceback (most recent call last):
- `23:32:33`       File "/var/task/lambda_function.py", line 63, in lambda_handler
- `23:32:33`         ki=float(ki)
- `23:32:33`     END RequestId: bbd63f10-72a3-41b4-bb70-2e1eb8ef7710
- `23:32:33`     REPORT RequestId: bbd63f10-72a3-41b4-bb70-2e1eb8ef7710	Duration: 501.44 ms	Billed Duration: 1069 ms	Memory Size: 256 MB	Max Memory Used: 110 MB	Init Duration: 566.69 ms	
- `23:32:33` 
- `23:32:33` ⚠ Some Lambdas have issues — see per-function section above
- `23:32:33` Done
