# ops 6669 - equity-research: one request traced end to end (read-only)

**Status:** failure  
**Duration:** 33.6s  
**Finished:** 2026-10-26T11:30:57+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6669_equity_research_trace.py", line 67, in main
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"queryStringParameters": {"ticker": "AAPL", "refresh": "1"}, "rawQueryString": "ticker=AAPL&refresh=1"}).encode())
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/site-packages/botocore/client.py", line 606, in _api_call
    return self._make_api_call(operation_name, kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.15/x64/lib/python3.12/site-packages/botocore/client.py", line 1094, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.errorfactory.TooManyRequestsException: An error occurred (TooManyRequestsException) when calling the Invoke operation (reached max retries: 8): Rate Exceeded.

```

## Log
## 1. the shim's decisions on the runner (same code the function runs)

- `11:30:24` profile?symbol=AAPL -> shaped True | bank rows 1 in 0.37s | fresh_enough True | never False
- `11:30:24` quote?symbol=AAPL -> shaped True | bank rows 0 in 0.00s | fresh_enough None | never True
- `11:30:24` income-statement?symbol=AAPL&period=annual&limit=20 -> shaped True | bank rows 4 in 0.00s | fresh_enough True | never False
- `11:30:24` ratios?symbol=AAPL&period=annual&limit=15 -> shaped True | bank rows 3 in 0.00s | fresh_enough True | never False
- `11:30:24` key-metrics-ttm?symbol=AAPL -> shaped True | bank rows 1 in 0.12s | fresh_enough True | never False
- `11:30:24` historical-price-eod/light?symbol=AAPL&from=2016-10-26 -> shaped True | bank rows 5000 in 0.25s | fresh_enough True | never False
- `11:30:24` fmp_bank STATS {"s3_gets": 7, "s3_misses": 0, "table_builds": 0, "table_evictions": 0, "table_refetches": 0, "history_book_hits": 4} | history book phase STEADY
- `11:30:24` fmp-bank manifest updated_at 2026-10-26T02:55:44.008955+00:00 endpoints_served 28
## 2. one invocation, traced

- `11:30:25` deployed: modified 2026-10-26T11:07:39, 1024 MB, 300 s
