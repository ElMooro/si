# ops 4577 — FRED ground truth + storm purge + key repair

**Status:** failure  
**Duration:** 352.9s  
**Finished:** 2026-08-10T04:15:58+00:00  

## Error

```
SystemExit: 1
```

## Data

| _justhodl_fred-api-key | _justhodl_fred_api-key | answer | env.FRED_API_KEY | gates_failed | throttles_10m_before |
|---|---|---|---|---|---|
| 32 | 32 |  | 32 |  |  |
|  |  |  |  |  | 126 |
|  |  | {} |  |  |  |
|  |  |  |  | 2 |  |

## Log
## 1. Key-chain audit (three belts)

## 2. Async storm purge (drop the retry backlog)

- `04:10:06`   event-invoke config → age 60s / retries 0 (purging)
- `04:11:36` ✅   restored → age 1h / retries 1 (watchdog covers gaps)
## 3. The lambda's own answer

- `04:11:36` ✗ FunctionError: {"errorMessage": "'categories_done'", "errorType": "KeyError", "requestId": "2707b9b7-acca-4bd0-b294-8c904a90c59b", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 707, in lambda_handler\n    print(json.dumps({k: m[k] for k in\n"]}
## 4. Recent log tail (last 25 lines, ground truth)

- `04:11:37`   | INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89
- `04:11:37`   | START RequestId: 7d217296-981d-48c8-a213-be28847d143e Version: $LATEST
- `04:11:37`   | [ERROR] KeyError: 'categories_done'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 707, in lambda_handler
    print(json.dumps({
- `04:11:37`   | END RequestId: 7d217296-981d-48c8-a213-be28847d143e
- `04:11:37`   | REPORT RequestId: 7d217296-981d-48c8-a213-be28847d143e	Duration: 349.86 ms	Billed Duration: 895 ms	Memory Size: 2048 MB	Max Memory Used: 98 MB	Init Duration: 54
- `04:11:37`   | START RequestId: c32dd450-0bd9-4a48-960d-4105fc3e0e74 Version: $LATEST
- `04:11:37`   | [ERROR] KeyError: 'categories_done'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 707, in lambda_handler
    print(json.dumps({
- `04:11:37`   | END RequestId: c32dd450-0bd9-4a48-960d-4105fc3e0e74
- `04:11:37`   | REPORT RequestId: c32dd450-0bd9-4a48-960d-4105fc3e0e74	Duration: 213.74 ms	Billed Duration: 214 ms	Memory Size: 2048 MB	Max Memory Used: 98 MB	
XRAY TraceId: 1-
- `04:11:37`   | START RequestId: ce8efa1b-3680-4770-8238-8fac22e425fc Version: $LATEST
- `04:11:37`   | INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89
- `04:11:37`   | START RequestId: 220cdabb-1512-43b2-8b05-746eaa5a2cdd Version: $LATEST
- `04:11:37`   | [ERROR] KeyError: 'categories_done'
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 685, in lambda_handler
    print(json.dumps({
- `04:11:37`   | END RequestId: 220cdabb-1512-43b2-8b05-746eaa5a2cdd
- `04:11:37`   | REPORT RequestId: 220cdabb-1512-43b2-8b05-746eaa5a2cdd	Duration: 361.77 ms	Billed Duration: 847 ms	Memory Size: 2048 MB	Max Memory Used: 98 MB	Init Duration: 48
- `04:11:37`   | START RequestId: be3f9c75-1570-4945-8760-67f6cd42d683 Version: $LATEST
- `04:11:37`   | [ERROR] NameError: name 's3' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 726, in lambda_handler
    s3.put_obj
- `04:11:37`   | END RequestId: be3f9c75-1570-4945-8760-67f6cd42d683
- `04:11:37`   | REPORT RequestId: be3f9c75-1570-4945-8760-67f6cd42d683	Duration: 780973.49 ms	Billed Duration: 780974 ms	Memory Size: 2048 MB	Max Memory Used: 99 MB	
XRAY Trace
## 5. Live-run proof: checkpoint within 200s

- `04:14:58` ✗ no state movement in 200s after the kick — read the log tail above
## 6. Storm check after purge

- `04:15:58` ⚠ Throttles(5m)=6 (was 126/10m before purge)
## VERDICT

- `04:15:58` ✗ 2 gate(s) failed
