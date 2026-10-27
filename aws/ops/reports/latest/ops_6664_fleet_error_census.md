# ops 6664 - fleet error census since the FMP block (2026-10-26 23:40 UTC) - read-only

**Status:** success  
**Duration:** 71.7s  
**Finished:** 2026-10-27T18:08:45+00:00  

## Log
- `18:07:45` functions: 914; fmp_bank importers in the repo: 222; fused so far: 28
## 1. totals

- `18:08:35` invoked since the block: 604 functions; with errors: 16; total errors 46; total invocations 2792
- `18:08:35` failing FMP consumers: 7 (fused: 0, not yet fused: 7); failing non-FMP engines: 9
## 2. failing functions (errors desc) - class, memory, timeout, duration max, last error lines

- `18:08:36` justhodl-estimate-revisions: errors 6 / inv 6 | exception | 512 MB / 120 s | max 1 s | fmp Y fused n | [ERROR] ValueError: Original received calendar required | Traceback (most recent call last): |   File "/var/task/lambda_function.py", line 440, in lambda_handle
- `18:08:37` justhodl-volatility-squeeze-hunter: errors 3 / inv 3 | exception | 1024 MB / 600 s | max 0 s | fmp Y fused n | [ERROR] ValueError: No original-scope membership; preserve current | Traceback (most recent call last): |   File "/var/task/lambda_function.py", line 534, in la
- `18:08:37` justhodl-causality-scanner: errors 3 / inv 3 | unknown | 2048 MB / 600 s | max 600 s | fmp n fused n | 
- `18:08:38` justhodl-momentum-breakout: errors 3 / inv 3 | exception | 1024 MB / 300 s | max 1 s | fmp Y fused n | [ERROR] ValueError: No original-scope membership; preserve current | Traceback (most recent call last): |   File "/var/task/lambda_function.py", line 454, in la
- `18:08:38` justhodl-dark-pool: errors 3 / inv 3 | exception | 1024 MB / 300 s | max 37 s | fmp n fused n | [ERROR] ValueError: Request already attempted; inspect retained status instead of recollecting | Traceback (most recent call last): |   File "/var/task/lambda_f
- `18:08:39` justhodl-share-flows: errors 3 / inv 3 | exception | 4096 MB / 900 s | max 2 s | fmp n fused n | [ERROR] ValueError: Request already attempted; inspect its retained failure or ambiguous state | Traceback (most recent call last): |   File "/var/task/lambda_f
- `18:08:39` justhodl-forensic-screen: errors 3 / inv 3 | exception | 1024 MB / 840 s | max 2 s | fmp n fused n | [ERROR] ValueError: Request already attempted; inspect its retained failure or ambiguous state | Traceback (most recent call last): |   File "/var/task/lambda_f
- `18:08:40` justhodl-predictability: errors 3 / inv 3 | unknown | 1024 MB / 600 s | max 600 s | fmp Y fused n | 
- `18:08:41` justhodl-opportunity-screener: errors 3 / inv 3 | out_of_memory | 256 MB / 120 s | max 5 s | fmp n fused n | REPORT RequestId: f06ae0ae-606a-4ef1-98e1-7cd315764e9d	Duration: 5103.78 ms	Billed Duration: 5104 ms	Memory Size: 256 MB	Max Memory Used: 256 MB	Status: error	E
- `18:08:41` justhodl-port-cargo: errors 3 / inv 3 | exception | 1536 MB / 780 s | max 69 s | fmp n fused n | [ERROR] CaptureError: Whole provider acquisition failed | Traceback (most recent call last): |   File "/var/task/lambda_function.py", line 536, in lambda_handle
- `18:08:42` justhodl-research-backtest: errors 3 / inv 3 | exception | 2048 MB / 900 s | max 855 s | fmp n fused n | [ERROR] RuntimeError: RESEARCH_DEADLINE_EXCEEDED_NO_PUBLICATION | Traceback (most recent call last): |   File "/var/task/lambda_function.py", line 578, in lambd
- `18:08:43` justhodl-revenue-acceleration: errors 3 / inv 3 | exception | 1024 MB / 600 s | max 0 s | fmp Y fused n | [ERROR] ValueError: No selected universe; preserve previous publication | Traceback (most recent call last): |   File "/var/task/lambda_function.py", line 565, 
- `18:08:43` justhodl-earnings-pead: errors 3 / inv 3 | exception | 1024 MB / 600 s | max 1 s | fmp Y fused n | [ERROR] ValueError: No selected universe; preserve previous publication | Traceback (most recent call last): |   File "/var/task/lambda_function.py", line 521, 
- `18:08:44` justhodl-chart-patterns: errors 2 / inv 2 | unknown | 1024 MB / 600 s | max 600 s | fmp Y fused n | 
- `18:08:45` justhodl-portfolio-snapshot: errors 1 / inv 1 | exception | 512 MB / 180 s | max 0 s | fmp n fused n | [ERROR] OrderingUnavailable: Snapshot retention conflicts with lifecycle expiry | Traceback (most recent call last): |   File "/var/task/lambda_function.py", li
- `18:08:45` justhodl-census-us: errors 1 / inv 15 | unknown | 1024 MB / 850 s | max 850 s | fmp n fused n | 
## 3. by class

- `18:08:45` exception: 11 -> justhodl-estimate-revisions justhodl-volatility-squeeze-hunter justhodl-momentum-breakout justhodl-dark-pool justhodl-share-flows justhodl-forensic-screen justhodl-port-cargo justhodl-research-backtest justhodl-revenue-acceleration justhodl-earnings-pead justhodl-portfolio-snapshot
- `18:08:45` unknown: 4 -> justhodl-causality-scanner justhodl-predictability justhodl-chart-patterns justhodl-census-us
- `18:08:45` out_of_memory: 1 -> justhodl-opportunity-screener
## 4. wave-3 queue = failing FMP consumers not yet fused, by errors

- `18:08:45` justhodl-estimate-revisions justhodl-volatility-squeeze-hunter justhodl-momentum-breakout justhodl-predictability justhodl-revenue-acceleration justhodl-earnings-pead justhodl-chart-patterns
- `18:08:45` queue json: {"since": "2026-10-26T23:40:00+00:00", "wave3_queue": ["justhodl-estimate-revisions", "justhodl-volatility-squeeze-hunter", "justhodl-momentum-breakout", "justhodl-predictability", "justhodl-revenue-acceleration", "justhodl-earnings-pead", "justhodl-chart-patterns"], "classes": {"exception": 11, "unknown": 4, "out_of_memory": 1}}
- `18:08:45` ✅ ops 6664 complete (read-only)
