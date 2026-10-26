# ops 6669 - equity-research: one request traced end to end (read-only)

**Status:** success  
**Duration:** 27.0s  
**Finished:** 2026-10-26T11:12:53+00:00  

## Log
## 1. the shim's decisions on the runner (same code the function runs)

- `11:12:27` profile?symbol=AAPL -> shaped True | bank rows 1 in 0.22s | fresh_enough True | never False
- `11:12:27` quote?symbol=AAPL -> shaped True | bank rows 0 in 0.00s | fresh_enough None | never True
- `11:12:27` income-statement?symbol=AAPL&period=annual&limit=20 -> shaped True | bank rows 4 in 0.00s | fresh_enough True | never False
- `11:12:27` ratios?symbol=AAPL&period=annual&limit=15 -> shaped True | bank rows 3 in 0.00s | fresh_enough True | never False
- `11:12:27` key-metrics-ttm?symbol=AAPL -> shaped True | bank rows 1 in 0.05s | fresh_enough True | never False
- `11:12:27` historical-price-eod/light?symbol=AAPL&from=2016-10-26 -> shaped True | bank rows 5000 in 0.15s | fresh_enough True | never False
- `11:12:27` fmp_bank STATS {"s3_gets": 7, "s3_misses": 0, "table_builds": 0, "table_evictions": 0, "table_refetches": 0, "history_book_hits": 4} | history book phase STEADY
- `11:12:27` fmp-bank manifest updated_at 2026-10-26T02:55:44.008955+00:00 endpoints_served 28
## 2. one invocation, traced

- `11:12:27` deployed: modified 2026-10-26T11:07:39, 1024 MB, 300 s
- `11:12:28` invoke RequestId 78904452-105f-4418-b9d6-cd6b83f7e337: status 200, 156 bytes, FunctionError None, head b'{"statusCode": 404, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": "{\\"error\\": \\"No data for ticker AAPL\\"}"}'
- `11:12:53` 3 log lines for this request:
- `11:12:53`    START RequestId: 78904452-105f-4418-b9d6-cd6b83f7e337 Version: $LATEST
- `11:12:53`    END RequestId: 78904452-105f-4418-b9d6-cd6b83f7e337
- `11:12:53`    REPORT RequestId: 78904452-105f-4418-b9d6-cd6b83f7e337	Duration: 490.54 ms	Billed Duration: 491 ms	Memory Size: 1024 MB	Max Memory Used: 989 MB	 ⏎ XRAY TraceId: 1-6adf359b-3ad3b7e25250a31c047d8028	SegmentId: a3b82daec95f8a0e	Sampled: true
- `11:12:53` engine / shim lines since the invoke (2):
- `11:12:53`    11:12:27 | [research] fetching AAPL
- `11:12:53`    11:12:28 | [research] 0/43 endpoints returned data
- `11:12:53` ✅ ops 6669 complete (read-only)
