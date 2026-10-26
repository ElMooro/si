# ops 6683 - equity-research out-of-memory timeline, last 7 h (read-only)

**Status:** success  
**Duration:** 8.4s  
**Finished:** 2026-10-26T20:52:35+00:00  

## Log
- `20:52:27` deployed: 1024 MB / 300 s, modified 2026-10-26T17:23:50, reserved concurrency 2
## 1. REPORT lines: 145 invocations

- `20:52:32` ok 126 (memory p50 976 MB, max 1024 MB), errors/timeouts 19
- `20:52:32` errors before the redeploy (2026-10-26T17:23:50): 6; after: 13
- `20:52:32`    14:43:19 | Duration 33568.54 ms Memory Size 1024 MB Max Memory Used 1024 MB Status error
- `20:52:32`    15:04:12 | Duration 31443.65 ms Memory Size 1024 MB Max Memory Used 1024 MB Status error
- `20:52:32`    15:08:41 | Duration 300000.00 ms Memory Size 1024 MB Max Memory Used 1024 MB Status timeout
XRAY TraceId: 1-6adf6bcc-63102f2769fba4d2508c8b2f
- `20:52:32`    16:19:17 | Duration 33277.90 ms Memory Size 1024 MB Max Memory Used 1024 MB Status error
- `20:52:32`    16:19:18 | Duration 34117.46 ms Memory Size 1024 MB Max Memory Used 1024 MB Status error
- `20:52:32`    16:54:46 | Duration 300000.00 ms Memory Size 1024 MB Max Memory Used 1024 MB Init Duration 573.49 ms Status timeout
XRAY TraceId: 1-6adf84aa-116053cb17a8494e445c972f
- `20:52:32`    17:37:00 | Duration 32986.70 ms Memory Size 1024 MB Max Memory Used 1024 MB Status error
- `20:52:32`    17:41:27 | Duration 300000.00 ms Memory Size 1024 MB Max Memory Used 1024 MB Status timeout
XRAY TraceId: 1-6adf8f9b-4857fb6d510f4fca5c0d2493
- `20:52:32`    17:54:30 | Duration 32079.13 ms Memory Size 1024 MB Max Memory Used 1023 MB Status error
- `20:52:32`    17:58:58 | Duration 300000.00 ms Memory Size 1024 MB Max Memory Used 1024 MB Status timeout
XRAY TraceId: 1-6adf93b6-7934013760b79dc942df855b
- `20:52:32`    18:50:11 | Duration 3539.36 ms Memory Size 1024 MB Max Memory Used 1024 MB Status error
- `20:52:32`    18:55:08 | Duration 300000.00 ms Memory Size 1024 MB Max Memory Used 1024 MB Init Duration 645.28 ms Status timeout
XRAY TraceId: 1-6adfa0df-1c12af5f01d10fa44e16c790
- `20:52:32`    19:08:32 | Duration 32060.49 ms Memory Size 1024 MB Max Memory Used 1024 MB Status error
- `20:52:32`    19:13:00 | Duration 300000.00 ms Memory Size 1024 MB Max Memory Used 1024 MB Status timeout
XRAY TraceId: 1-6adfa510-54e79f660cb884a42595e0ab
- `20:52:32`    19:40:59 | Duration 13506.49 ms Memory Size 1024 MB Max Memory Used 1023 MB Status error
- `20:52:32`    19:46:54 | Duration 300000.00 ms Memory Size 1024 MB Max Memory Used 1024 MB Status timeout
XRAY TraceId: 1-6adfad02-257841de5ae68a5b127d5aae
- `20:52:32`    20:11:04 | Duration 300000.00 ms Memory Size 1024 MB Max Memory Used 1024 MB Init Duration 601.81 ms Status timeout
XRAY TraceId: 1-6adfb2ab-68e0b6fe3a22f4db31c3feaf
- `20:52:32`    20:48:57 | Duration 1166.31 ms Memory Size 1024 MB Max Memory Used 1024 MB Status error
- `20:52:32`    20:49:28 | Duration 31997.21 ms Memory Size 1024 MB Max Memory Used 1024 MB Status error
## 2. the engine's and the shim's lines around the errors

- `20:52:35` 416 matching lines; showing the 25 lines before each of the last 6 errors
- `20:52:35` --- error at 19:13:00 (timeout
XRAY TraceId: 1-6adfa510-54e79f660cb884a42595e0ab)
- `20:52:35` --- error at 19:40:59 (error)
- `20:52:35`    19:40:45 | [research] fetching ARTR
- `20:52:35`    19:40:46 | [research] fetching ARTR
- `20:52:35`    19:40:46 | [fmp-bankfirst] hits=40 stale=7 misses=79 unshaped=21 passthrough=29 bypassed=139 errors=0 bank_time=5.5s
- `20:52:35`    19:40:46 | [fmp-bankfirst] hits=40 stale=7 misses=80 unshaped=21 passthrough=29 bypassed=268 errors=0 bank_time=5.3s[fmp_get] key-metrics → HTTP 429
- `20:52:35`    19:40:59 | REPORT RequestId: 9ac8283d-3d9d-4532-9636-6b966f52986c	Duration: 13506.49 ms	Billed Duration: 13507 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adfacbd-22ef34627c251bb7669ac0b5	SegmentId: c8
- `20:52:35` --- error at 19:46:54 (timeout
XRAY TraceId: 1-6adfad02-257841de5ae68a5b127d5aae)
- `20:52:35`    19:44:15 | [research] fetching VINO
- `20:52:35`    19:44:16 | [research] 0/43 endpoints returned data
- `20:52:35`    19:46:03 | [research] fetching TRV
- `20:52:35`    19:46:03 | [research] 0/43 endpoints returned data
- `20:52:35` --- error at 20:11:04 (timeout
XRAY TraceId: 1-6adfb2ab-68e0b6fe3a22f4db31c3feaf)
- `20:52:35`    20:10:40 | [research] fetching INNV
- `20:52:35`    20:10:41 | [research] 0/43 endpoints returned data
- `20:52:35` --- error at 20:48:57 (error)
- `20:52:35`    20:46:14 | [research] fetching DOCMF
- `20:52:35`    20:46:14 | [fmp-bankfirst] first hit: hits=9 stale=0 misses=11 unshaped=6 passthrough=3 bypassed=328 errors=0 bank_time=0.2s
- `20:52:35`    20:46:14 | [research] fetching DOCMF
- `20:52:35`    20:46:14 | [research] 14/43 endpoints returned data
- `20:52:35`    20:46:15 | [fmp-bankfirst] first hit: hits=1 stale=0 misses=0 unshaped=0 passthrough=1 bypassed=0 errors=0 bank_time=0.0s
- `20:52:35`    20:46:15 | [research] 14/43 endpoints returned data
- `20:52:35`    20:48:56 | [research] fetching ALPN
- `20:52:35`    20:48:56 | [research] fetching ALPN
- `20:52:35`    20:48:56 | [fmp-bankfirst] hits=16 stale=1 misses=24 unshaped=9 passthrough=12 bypassed=0 errors=0 bank_time=3.3s
- `20:52:35`    20:48:56 | [fmp-bankfirst] hits=16 stale=1 misses=25 unshaped=9 passthrough=12 bypassed=0 errors=0 bank_time=3.4s
- `20:52:35`    20:48:57 | REPORT RequestId: 992b3a22-5ae0-496e-a567-7cb77d204818	Duration: 1166.31 ms	Billed Duration: 1167 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adfbcb8-48e31dee72f6a4d55c6ae0d3	SegmentId: 2c71
- `20:52:35` --- error at 20:49:28 (error)
- `20:52:35`    20:48:56 | [research] fetching ALPN
- `20:52:35`    20:48:56 | [research] fetching ALPN
- `20:52:35`    20:48:56 | [fmp-bankfirst] hits=16 stale=1 misses=24 unshaped=9 passthrough=12 bypassed=0 errors=0 bank_time=3.3s
- `20:52:35`    20:48:56 | [fmp-bankfirst] hits=16 stale=1 misses=25 unshaped=9 passthrough=12 bypassed=0 errors=0 bank_time=3.4s
- `20:52:35`    20:48:57 | REPORT RequestId: 992b3a22-5ae0-496e-a567-7cb77d204818	Duration: 1166.31 ms	Billed Duration: 1167 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adfbcb8-48e31dee72f6a4d55c6ae0d3	SegmentId: 2c71
- `20:52:35`    20:49:25 | [fmp-bankfirst] bank stepped aside for the rest of this invocation: bank time cap 90s (attempts=72 bank_time=97.2s)
- `20:52:35`    20:49:27 | [research] 9/43 endpoints returned data
- `20:52:35`    20:49:28 | REPORT RequestId: 53f4098c-c062-48a6-a2bf-e56ca1e55b65	Duration: 31997.21 ms	Billed Duration: 31998 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adfbcb8-535392171e7f012b22b662f6	SegmentId: 58
## 3. tickers asked (from '[research]' lines) and shim bypass reasons in the window

- `20:52:35` bypass reasons: {'last 10 attempts averaged 0.53s with no hit': 2, 'last 10 attempts averaged 0.70s with no hit': 2, 'last 10 attempts averaged 1.07s with no hit': 1, 'last 10 attempts averaged 0.63s with no hit': 2, 'bank time cap 90s': 5, 'last 10 attempts averaged 0.71s with no hit': 1, 'last 10 attempts averaged 0.51s with no hit': 1, 'last 10 attempts averaged 0.82s with no hit': 2, 'last 10 attempts averaged 0.67s with no hit': 1, 'last 10 attempts averaged 0.52s with no hit': 1, 'last 10 attempts averaged 0.56s with no hit': 1, 'last 10 attempts averaged 1.17s with no hit': 2, 'last 10 attempts averaged 0.68s with no hit': 1, 'last 10 attempts averaged 1.21s with no hit': 1, 'last 10 attempts averaged 0.65s with no hit': 1, 'last 10 attempts averaged 0.72s with no hit': 1, 'last 10 attempts averaged 0.60s with no hit': 1, 'last 10 attempts averaged 0.50s with no hit': 1, 'bank time cap 126s': 1, 'bank time cap 127s': 1, 'last 10 attempts averaged 0.73s with no hit': 3, 'last 10 attempts averaged 0.93s with no hit': 1, 'table cache thrashing': 1, 'last 10 attempts averaged 0.92s with no hit': 1, 'last 10 attempts averaged 1.01s with no hit': 1}
- `20:52:35` ✅ ops 6683 complete (read-only)
