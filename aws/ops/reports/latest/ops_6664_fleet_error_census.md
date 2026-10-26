# ops 6664 - fleet error census since the FMP block (2026-10-26 01:00 UTC) - read-only

**Status:** success  
**Duration:** 32.5s  
**Finished:** 2026-10-26T06:24:10+00:00  

## Log
- `06:23:47` functions: 913; fmp_bank importers in the repo: 221; fused so far: 28
## 1. totals

- `06:24:06` invoked since the block: 169 functions; with errors: 9; total errors 74; total invocations 1845
- `06:24:06` failing FMP consumers: 8 (fused: 5, not yet fused: 3); failing non-FMP engines: 1
## 2. failing functions (errors desc) - class, memory, timeout, duration max, last error lines

- `06:24:07` justhodl-equity-research: errors 52 / inv 722 | out_of_memory | 1024 MB / 300 s | max 300 s | fmp Y fused n | REPORT RequestId: 385dafdd-a77d-4d1a-a18f-7da72c7f4170	Duration: 2055.60 ms	Billed Duration: 2056 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error
- `06:24:07` justhodl-insider-radar: errors 6 / inv 6 | unknown | 512 MB / 300 s | max 300 s | fmp Y fused Y | 
- `06:24:07` justhodl-divcut-warning: errors 3 / inv 4 | out_of_memory | 1024 MB / 540 s | max 27 s | fmp Y fused Y | REPORT RequestId: c28637d7-d1b6-4c38-8a84-22f953225923	Duration: 7191.78 ms	Billed Duration: 7192 ms	Memory Size: 768 MB	Max Memory Used: 768 MB	Status: error	E
- `06:24:07` justhodl-stock-valuations: errors 3 / inv 4 | out_of_memory | 1536 MB / 600 s | max 70 s | fmp Y fused Y | REPORT RequestId: a19bb68e-37c9-4d05-85c4-1ec397335327	Duration: 56228.62 ms	Billed Duration: 56229 ms	Memory Size: 640 MB	Max Memory Used: 640 MB	Status: error
- `06:24:08` justhodl-gap-metrics: errors 3 / inv 3 | unknown | 256 MB / 240 s | max 240 s | fmp Y fused n | 
- `06:24:08` justhodl-event-flow-monitor: errors 3 / inv 3 | unknown | 512 MB / 120 s | max 120 s | fmp n fused n | 
- `06:24:09` justhodl-eps-revision-velocity: errors 2 / inv 3 | exception | 1024 MB / 300 s | max 39 s | fmp Y fused n | [ERROR] ValueError: All provider populations unavailable; preserve previous publication | Traceback (most recent call last): |   File "/var/task/lambda_function
- `06:24:10` justhodl-eva-spread: errors 1 / inv 1 | unknown | 1024 MB / 600 s | max 600 s | fmp Y fused Y | 
- `06:24:10` justhodl-bank-factors: errors 1 / inv 4 | out_of_memory | 3008 MB / 900 s | max 306 s | fmp Y fused Y | REPORT RequestId: a2a94943-6d94-4fad-bd9d-8c68f60a0d0f	Duration: 18913.07 ms	Billed Duration: 19450 ms	Memory Size: 3008 MB	Max Memory Used: 3008 MB	Init Durati
## 3. by class

- `06:24:10` out_of_memory: 4 -> justhodl-equity-research justhodl-divcut-warning justhodl-stock-valuations justhodl-bank-factors
- `06:24:10` unknown: 4 -> justhodl-insider-radar justhodl-gap-metrics justhodl-event-flow-monitor justhodl-eva-spread
- `06:24:10` exception: 1 -> justhodl-eps-revision-velocity
## 4. wave-3 queue = failing FMP consumers not yet fused, by errors

- `06:24:10` justhodl-equity-research justhodl-gap-metrics justhodl-eps-revision-velocity
- `06:24:10` queue json: {"since": "2026-10-26T01:00:00+00:00", "wave3_queue": ["justhodl-equity-research", "justhodl-gap-metrics", "justhodl-eps-revision-velocity"], "classes": {"out_of_memory": 4, "unknown": 4, "exception": 1}}
- `06:24:10` ✅ ops 6664 complete (read-only)
