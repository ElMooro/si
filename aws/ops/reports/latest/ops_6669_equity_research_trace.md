# ops 6669 - equity-research: one request traced end to end (read-only)

**Status:** success  
**Duration:** 32.8s  
**Finished:** 2026-10-26T08:42:51+00:00  

## Log
## 1. the shim's decisions on the runner (same code the function runs)

- `08:42:22` profile?symbol=AAPL -> shaped True | bank rows 1 in 3.54s | fresh_enough True | never False
- `08:42:22` quote?symbol=AAPL -> shaped True | bank rows 0 in 0.00s | fresh_enough None | never True
- `08:42:22` income-statement?symbol=AAPL&period=annual&limit=20 -> shaped True | bank rows 4 in 0.00s | fresh_enough False | never False
- `08:42:22` ratios?symbol=AAPL&period=annual&limit=15 -> shaped True | bank rows 3 in 0.00s | fresh_enough False | never False
- `08:42:23` key-metrics-ttm?symbol=AAPL -> shaped True | bank rows 1 in 1.26s | fresh_enough True | never False
- `08:42:23` historical-price-eod/light?symbol=AAPL&from=2016-10-26 -> shaped True | bank rows 0 in 0.00s | fresh_enough None | never False
- `08:42:23` fmp_bank STATS {"s3_gets": 11, "s3_misses": 2, "table_builds": 5, "table_evictions": 0, "table_refetches": 0, "history_book_hits": 2} | history book phase STEADY
- `08:42:23` fmp-bank manifest updated_at 2026-10-26T02:55:44.008955+00:00 endpoints_served 28
## 2. one invocation, traced

- `08:42:24` deployed: modified 2026-10-26T07:24:34, 1024 MB, 300 s
- `08:42:25` invoke RequestId 721e496f-0f3b-46b8-89d5-4c4cff14759e: status 200, 149 bytes, FunctionError Unhandled, head b'{"errorType":"Runtime.OutOfMemory","errorMessage":"RequestId: 721e496f-0f3b-46b8-89d5-4c4cff14759e Error: Runtime exited with error: signal: killed"}'
- `08:42:50` 3 log lines for this request:
- `08:42:50`    START RequestId: 721e496f-0f3b-46b8-89d5-4c4cff14759e Version: $LATEST
- `08:42:50`    END RequestId: 721e496f-0f3b-46b8-89d5-4c4cff14759e
- `08:42:50`    REPORT RequestId: 721e496f-0f3b-46b8-89d5-4c4cff14759e	Duration: 1164.71 ms	Billed Duration: 1165 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adf1270-57d913ac6a27978419963a94	SegmentId: 7449b4890487e02d	Sampled: true
- `08:42:51` engine / shim lines since the invoke (2):
- `08:42:51`    08:42:24 | [research] fetching AAPL
- `08:42:51`    08:42:24 | [fmp-bankfirst] first hit: hits=37 stale=29 misses=94 unshaped=21 passthrough=40 bypassed=53 errors=0 bank_time=0.6s
- `08:42:51` ✅ ops 6669 complete (read-only)
