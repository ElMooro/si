# ops 6669 - equity-research: one request traced end to end (read-only)

**Status:** success  
**Duration:** 31.9s  
**Finished:** 2026-10-26T11:35:10+00:00  

## Log
## 1. the shim's decisions on the runner (same code the function runs)

- `11:34:38` profile?symbol=AAPL -> shaped True | bank rows 1 in 0.16s | fresh_enough True | never False
- `11:34:38` quote?symbol=AAPL -> shaped True | bank rows 0 in 0.00s | fresh_enough None | never True
- `11:34:38` income-statement?symbol=AAPL&period=annual&limit=20 -> shaped True | bank rows 4 in 0.00s | fresh_enough True | never False
- `11:34:38` ratios?symbol=AAPL&period=annual&limit=15 -> shaped True | bank rows 3 in 0.00s | fresh_enough True | never False
- `11:34:38` key-metrics-ttm?symbol=AAPL -> shaped True | bank rows 1 in 0.03s | fresh_enough True | never False
- `11:34:38` historical-price-eod/light?symbol=AAPL&from=2016-10-26 -> shaped True | bank rows 5000 in 0.09s | fresh_enough True | never False
- `11:34:38` fmp_bank STATS {"s3_gets": 7, "s3_misses": 0, "table_builds": 0, "table_evictions": 0, "table_refetches": 0, "history_book_hits": 4} | history book phase STEADY
- `11:34:38` fmp-bank manifest updated_at 2026-10-26T02:55:44.008955+00:00 endpoints_served 28
## 2. one invocation, traced

- `11:34:38` deployed: modified 2026-10-26T11:33:37, 1024 MB, 300 s
- `11:34:44` invoke RequestId 59c0e9ac-9732-4c1b-b349-6151ce72b3de: status 200, 142036 bytes, FunctionError None, head b'{"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=3600"}, "body": "{\\"capital_structure_context\\": {\\"contract\\"'
- `11:35:10` 3 log lines for this request:
- `11:35:10`    START RequestId: 59c0e9ac-9732-4c1b-b349-6151ce72b3de Version: $LATEST
- `11:35:10`    END RequestId: 59c0e9ac-9732-4c1b-b349-6151ce72b3de
- `11:35:10`    REPORT RequestId: 59c0e9ac-9732-4c1b-b349-6151ce72b3de	Duration: 4752.45 ms	Billed Duration: 5386 ms	Memory Size: 1024 MB	Max Memory Used: 227 MB	Init Duration: 633.23 ms	 ⏎ XRAY TraceId: 1-6adf3ace-16477f126ac9f0a8427a4da4	SegmentId: f6e0fa77e85e4831	Sampled: true
- `11:35:10` engine / shim lines since the invoke (3):
- `11:35:10`    11:34:39 | [research] fetching AAPL
- `11:35:10`    11:34:40 | [fmp-bankfirst] first hit: hits=1 stale=0 misses=0 unshaped=0 passthrough=1 bypassed=0 errors=0 bank_time=0.2s
- `11:35:10`    11:34:41 | [research] 23/43 endpoints returned data
- `11:35:10` ✅ ops 6669 complete (read-only)
