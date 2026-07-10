## 1. Ensure engine

**Status:** failure  
**Duration:** 9.4s  
**Finished:** 2026-07-10T04:56:40+00:00  

## Error

```
SystemExit: 1
```

## Data

| code_age_min | fn_exists | n_fails | n_warns | need_fresh_deploy | verdict |
|---|---|---|---|---|---|
|  |  |  |  | False |  |
|  | True |  |  |  |  |
| 45.2 |  |  |  |  |  |
|  |  | 1 | 0 |  | FAIL |

## Log
## 2. Segment the market (SYNC + log tail)

- `04:56:40` engine log tail:
START RequestId: b92a770e-7c92-4a13-84e3-613428ff991f Version: $LATEST
[ERROR] RuntimeError: POLYGON key missing
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 317, in lambda_handler
    raise RuntimeError("POLYGON key missing")
END RequestId: b92a770e-7c92-4a13-84e3-613428ff991f
REPORT RequestId: b92a770e-7c92-4a13-84e3-613428ff991f	Duration: 3.10 ms	Billed Duration: 546 ms	Memory Size: 2048 MB	Max Memory Used: 95 MB	Init Duration: 542.01 ms	
XRAY TraceId: 1-6a507b87-0cc4438344b48ef127ab1931	SegmentId: d0025b207c551fde	Sampled: true	

