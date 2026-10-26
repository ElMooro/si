# ops 6664 - fleet error census since the FMP block (2026-10-26 05:04 UTC) - read-only

**Status:** success  
**Duration:** 43.2s  
**Finished:** 2026-10-26T11:05:51+00:00  

## Log
- `11:05:19` functions: 914; fmp_bank importers in the repo: 222; fused so far: 28
## 1. totals

- `11:05:43` invoked since the block: 209 functions; with errors: 13; total errors 68; total invocations 1174
- `11:05:43` failing FMP consumers: 8 (fused: 3, not yet fused: 5); failing non-FMP engines: 5
## 2. failing functions (errors desc) - class, memory, timeout, duration max, last error lines

- `11:05:45` justhodl-equity-research: errors 34 / inv 306 | out_of_memory | 1024 MB / 300 s | max 300 s | fmp Y fused n | REPORT RequestId: fe57bad4-7cd4-4036-b133-835e5403a413	Duration: 28608.26 ms	Billed Duration: 29199 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Init Durati
- `11:05:46` justhodl-insider-radar: errors 5 / inv 6 | unknown | 1024 MB / 300 s | max 300 s | fmp Y fused Y | 
- `11:05:47` justhodl-stock-screener: errors 3 / inv 3 | fmp_429 | 1280 MB / 900 s | max 900 s | fmp Y fused n | ERR price-target-consensus: exhausted retries (HTTP Error 429: Too Many Requests)
- `11:05:47` justhodl-analytics-snapshot: errors 3 / inv 3 | unknown | 512 MB / 600 s | max 600 s | fmp n fused n | 
- `11:05:47` justhodl-signal-genealogy: errors 3 / inv 3 | exception | 2048 MB / 600 s | max 113 s | fmp n fused n | [ERROR] ValueError: unreconciled archive cannot produce chronology | Traceback (most recent call last): |   File "/var/task/lambda_function.py", line 33, in lam
- `11:05:48` justhodl-starmine: errors 3 / inv 3 | unknown | 1536 MB / 900 s | max 900 s | fmp Y fused Y | 
- `11:05:48` justhodl-eps-revision-velocity: errors 3 / inv 3 | exception | 1024 MB / 300 s | max 4 s | fmp Y fused n | [ERROR] ValueError: All provider populations unavailable; preserve previous publication | Traceback (most recent call last): |   File "/var/task/lambda_function
- `11:05:49` justhodl-earnings-whisper: errors 3 / inv 3 | unknown | 256 MB / 60 s | max 60 s | fmp n fused n | 
- `11:05:49` treasury-auto-updater: errors 3 / inv 3 | unknown | 128 MB / 3 s | max 3 s | fmp n fused n | 
- `11:05:50` justhodl-momentum-leaders: errors 3 / inv 3 | exception | 1024 MB / 300 s | max 1 s | fmp Y fused n | [ERROR] ValueError: No received market observations; preserve current | Traceback (most recent call last): |   File "/var/task/lambda_function.py", line 656, in
- `11:05:50` justhodl-earnings-pead: errors 3 / inv 3 | exception | 1024 MB / 600 s | max 2 s | fmp Y fused n | [ERROR] ValueError: All earnings populations unavailable; preserve previous publication | Traceback (most recent call last): |   File "/var/task/lambda_function
- `11:05:51` justhodl-insider-buyback-confluence: errors 1 / inv 2 | unknown | 768 MB / 300 s | max 300 s | fmp Y fused Y | 
- `11:05:51` justhodl-event-flow-monitor: errors 1 / inv 1 | unknown | 512 MB / 120 s | max 120 s | fmp n fused n | 
## 3. by class

- `11:05:51` unknown: 7 -> justhodl-insider-radar justhodl-analytics-snapshot justhodl-starmine justhodl-earnings-whisper treasury-auto-updater justhodl-insider-buyback-confluence justhodl-event-flow-monitor
- `11:05:51` exception: 4 -> justhodl-signal-genealogy justhodl-eps-revision-velocity justhodl-momentum-leaders justhodl-earnings-pead
- `11:05:51` out_of_memory: 1 -> justhodl-equity-research
- `11:05:51` fmp_429: 1 -> justhodl-stock-screener
## 4. wave-3 queue = failing FMP consumers not yet fused, by errors

- `11:05:51` justhodl-equity-research justhodl-stock-screener justhodl-eps-revision-velocity justhodl-momentum-leaders justhodl-earnings-pead
- `11:05:51` queue json: {"since": "2026-10-26T05:04:35+00:00", "wave3_queue": ["justhodl-equity-research", "justhodl-stock-screener", "justhodl-eps-revision-velocity", "justhodl-momentum-leaders", "justhodl-earnings-pead"], "classes": {"out_of_memory": 1, "unknown": 7, "fmp_429": 1, "exception": 4}}
- `11:05:51` ✅ ops 6664 complete (read-only)
