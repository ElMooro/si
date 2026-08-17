# 1. CloudWatch tail

**Status:** failure  
**Duration:** 0.9s  
**Finished:** 2026-08-17T18:32:05+00:00  

## Error

```
SystemExit: 1
```

## Log
- `18:32:04`   [ERROR] TypeError: can't subtract offset-naive and offset-aware datetimes
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 213, in lambda_handler
    
- `18:32:04`   REPORT RequestId: 833a5f83-f04a-4bd7-bc89-99814e2c841a	Duration: 378.95 ms	Billed Duration: 762 ms	Memory Size: 512 MB	Max Memory Used: 110 MB	Init Duration: 382.28 ms	
XRAY TraceI
- `18:32:04`   [ERROR] TypeError: can't subtract offset-naive and offset-aware datetimes
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 213, in lambda_handler
    
- `18:32:04`   REPORT RequestId: 833a5f83-f04a-4bd7-bc89-99814e2c841a	Duration: 380.88 ms	Billed Duration: 381 ms	Memory Size: 512 MB	Max Memory Used: 111 MB	
XRAY TraceId: 1-6a8351f8-71113ea5280
- `18:32:04`   [ERROR] TypeError: can't subtract offset-naive and offset-aware datetimes
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 213, in lambda_handler
    
- `18:32:04`   REPORT RequestId: 833a5f83-f04a-4bd7-bc89-99814e2c841a	Duration: 349.88 ms	Billed Duration: 350 ms	Memory Size: 512 MB	Max Memory Used: 123 MB	
XRAY TraceId: 1-6a8351f8-71113ea5280
# 2. synchronous invoke (root cause capture)

- `18:32:05`   StatusCode=200 FunctionError=Unhandled
- `18:32:05`   payload: {"errorMessage": "can't subtract offset-naive and offset-aware datetimes", "errorType": "TypeError", "requestId": "650bf6a9-b93c-4bdd-a0d3-9b50a9b2fd37", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 213, in lambda_handler\n    n_new, gerr = grade(bank, led, today)\n", "  File \"/var/task/lambda_function.py\", line 123, in grade\n    age_d = (datetime.fromisoformat(today)\n"]}
- `18:32:05` ✗ lambda still erroring -- traceback above
