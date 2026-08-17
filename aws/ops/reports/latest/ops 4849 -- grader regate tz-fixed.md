# 1. CloudWatch tail

**Status:** success  
**Duration:** 2.2s  
**Finished:** 2026-08-17T18:35:27+00:00  

## Log
- `18:35:25`   [ERROR] TypeError: can't subtract offset-naive and offset-aware datetimes
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 213, in lambda_handler
    
- `18:35:25`   REPORT RequestId: 833a5f83-f04a-4bd7-bc89-99814e2c841a	Duration: 378.95 ms	Billed Duration: 762 ms	Memory Size: 512 MB	Max Memory Used: 110 MB	Init Duration: 382.28 ms	
XRAY TraceI
- `18:35:25`   [ERROR] TypeError: can't subtract offset-naive and offset-aware datetimes
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 213, in lambda_handler
    
- `18:35:25`   REPORT RequestId: 833a5f83-f04a-4bd7-bc89-99814e2c841a	Duration: 380.88 ms	Billed Duration: 381 ms	Memory Size: 512 MB	Max Memory Used: 111 MB	
XRAY TraceId: 1-6a8351f8-71113ea5280
- `18:35:25`   [ERROR] TypeError: can't subtract offset-naive and offset-aware datetimes
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 213, in lambda_handler
    
- `18:35:25`   REPORT RequestId: 833a5f83-f04a-4bd7-bc89-99814e2c841a	Duration: 349.88 ms	Billed Duration: 350 ms	Memory Size: 512 MB	Max Memory Used: 123 MB	
XRAY TraceId: 1-6a8351f8-71113ea5280
- `18:35:25`   [ERROR] TypeError: can't subtract offset-naive and offset-aware datetimes
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 213, in lambda_handler
    
- `18:35:25`   REPORT RequestId: 650bf6a9-b93c-4bdd-a0d3-9b50a9b2fd37	Duration: 417.86 ms	Billed Duration: 418 ms	Memory Size: 512 MB	Max Memory Used: 124 MB	
XRAY TraceId: 1-6a8353a4-173f3c806e5
# 2. synchronous invoke (root cause capture)

- `18:35:26`   StatusCode=200 FunctionError=None
- `18:35:26`   payload: {"ok": true, "status": "LIVE", "n_weeks_banked": 1, "n_graded_rows": 0, "weights_status": "PROVISIONAL"}
- `18:35:26` ✅ synchronous invoke clean
# 3. truths

- `18:35:27` ✅   LIVE; week 2026-08-17 banked
- `18:35:27` ✅   bucket large              banked 15 == min(40,15)
- `18:35:27` ✅   bucket mid                banked 15 == min(40,15)
- `18:35:27` ✅   bucket small              banked 15 == min(40,15)
- `18:35:27` ✅   bucket micro              banked 15 == min(40,15)
- `18:35:27` ✅   bucket etf_equity         banked 15 == min(40,15)
- `18:35:27` ✅   bucket etf_bond           banked 0 == min(40,0)
- `18:35:27` ✅   bucket etf_commodity      banked 3 == min(40,3)
- `18:35:27` ✅   bucket etf_crypto_alt     banked 0 == min(40,0)
- `18:35:27` ✅   bucket comeback           banked 6 == min(40,6)
- `18:35:27` ✅   age 0d -> accruing, ETA 2026-09-14
- `18:35:27` ✅   weights: n=0, consumption-deferred note present
- `18:35:27`   banked weeks=1
# 4. verdict

- `18:35:27` ✅ Fusion 4 grader LIVE -- claims banked, grading clock running
