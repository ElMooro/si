# ops 6669 - equity-research: one request traced end to end (read-only)

**Status:** success  
**Duration:** 29.1s  
**Finished:** 2026-10-26T09:17:18+00:00  

## Log
## 1. the shim's decisions on the runner (same code the function runs)

- `09:16:49` profile?symbol=AAPL -> shaped True | bank rows 1 in 0.22s | fresh_enough True | never False
- `09:16:49` quote?symbol=AAPL -> shaped True | bank rows 0 in 0.00s | fresh_enough None | never True
- `09:16:49` income-statement?symbol=AAPL&period=annual&limit=20 -> shaped True | bank rows 4 in 0.00s | fresh_enough True | never False
- `09:16:49` ratios?symbol=AAPL&period=annual&limit=15 -> shaped True | bank rows 3 in 0.00s | fresh_enough True | never False
- `09:16:49` key-metrics-ttm?symbol=AAPL -> shaped True | bank rows 1 in 0.05s | fresh_enough True | never False
- `09:16:49` historical-price-eod/light?symbol=AAPL&from=2016-10-26 -> shaped True | bank rows 0 in 0.00s | fresh_enough None | never False
- `09:16:49` fmp_bank STATS {"s3_gets": 6, "s3_misses": 0, "table_builds": 0, "table_evictions": 0, "table_refetches": 0, "history_book_hits": 4} | history book phase STEADY
- `09:16:49` fmp-bank manifest updated_at 2026-10-26T02:55:44.008955+00:00 endpoints_served 28
## 2. one invocation, traced

- `09:16:49` deployed: modified 2026-10-26T08:58:32, 1024 MB, 300 s
- `09:16:52` invoke RequestId a068400d-514c-4c9c-ad6e-18c062729aa6: status 200, 113024 bytes, FunctionError None, head b'{"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=3600"}, "body": "{\\"capital_structure_context\\": {\\"contract\\"'
- `09:17:18` 3 log lines for this request:
- `09:17:18`    START RequestId: a068400d-514c-4c9c-ad6e-18c062729aa6 Version: $LATEST
- `09:17:18`    END RequestId: a068400d-514c-4c9c-ad6e-18c062729aa6
- `09:17:18`    REPORT RequestId: a068400d-514c-4c9c-ad6e-18c062729aa6	Duration: 2587.65 ms	Billed Duration: 2588 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	 ⏎ XRAY TraceId: 1-6adf1a81-6e4cc8ed4cbb807c1b90b0a8	SegmentId: 46f6619fc6b26200	Sampled: true
- `09:17:18` engine / shim lines since the invoke (3):
- `09:17:18`    09:16:49 | [research] fetching AAPL
- `09:17:18`    09:16:50 | [fmp-bankfirst] hits=67 stale=0 misses=94 unshaped=18 passthrough=40 bypassed=182 errors=0 bank_time=2.7s
- `09:17:18`    09:16:50 | [research] 21/43 endpoints returned data
- `09:17:18` ✅ ops 6669 complete (read-only)
