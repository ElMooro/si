# ops 6664 - fleet error census since the FMP block (2026-10-26 14:46 UTC) - read-only

**Status:** success  
**Duration:** 50.7s  
**Finished:** 2026-10-26T20:48:12+00:00  

## Log
- `20:47:32` functions: 914; fmp_bank importers in the repo: 222; fused so far: 28
## 1. totals

- `20:48:08` invoked since the block: 184 functions; with errors: 7; total errors 40; total invocations 1939
- `20:48:08` failing FMP consumers: 5 (fused: 0, not yet fused: 5); failing non-FMP engines: 2
## 2. failing functions (errors desc) - class, memory, timeout, duration max, last error lines

- `20:48:09` justhodl-equity-research: errors 16 / inv 129 | out_of_memory | 1024 MB / 300 s | max 300 s | fmp Y fused n | REPORT RequestId: eb1c0d8b-149a-487a-8135-68168d1b380d	Duration: 31443.65 ms	Billed Duration: 31444 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: err
- `20:48:10` justhodl-momentum-leaders: errors 6 / inv 6 | exception | 1024 MB / 300 s | max 1 s | fmp Y fused n | [ERROR] ValueError: No received market observations; preserve current | Traceback (most recent call last): |   File "/var/task/lambda_function.py", line 701, in
- `20:48:10` justhodl-estimate-revisions: errors 6 / inv 6 | exception | 512 MB / 120 s | max 1 s | fmp Y fused n | [ERROR] ValueError: Original received calendar required | Traceback (most recent call last): |   File "/var/task/lambda_function.py", line 440, in lambda_handle
- `20:48:11` justhodl-earnings-pead: errors 6 / inv 6 | exception | 1024 MB / 600 s | max 2 s | fmp Y fused n | [ERROR] ValueError: All earnings populations unavailable; preserve previous publication | Traceback (most recent call last): |   File "/var/task/lambda_function
- `20:48:11` justhodl-eps-revision-velocity: errors 4 / inv 5 | exception | 1024 MB / 300 s | max 7 s | fmp Y fused n | [ERROR] ValueError: All provider populations unavailable; preserve previous publication | Traceback (most recent call last): |   File "/var/task/lambda_function
- `20:48:12` justhodl-portfolio-snapshot: errors 1 / inv 1 | exception | 512 MB / 180 s | max 0 s | fmp n fused n | [ERROR] OrderingUnavailable: Snapshot retention conflicts with lifecycle expiry | Traceback (most recent call last): |   File "/var/task/lambda_function.py", li
- `20:48:12` cftc-futures-positioning-agent: errors 1 / inv 1 | unknown | 512 MB / 120 s | max 120 s | fmp n fused n | 
## 3. by class

- `20:48:12` exception: 5 -> justhodl-momentum-leaders justhodl-estimate-revisions justhodl-earnings-pead justhodl-eps-revision-velocity justhodl-portfolio-snapshot
- `20:48:12` out_of_memory: 1 -> justhodl-equity-research
- `20:48:12` unknown: 1 -> cftc-futures-positioning-agent
## 4. wave-3 queue = failing FMP consumers not yet fused, by errors

- `20:48:12` justhodl-equity-research justhodl-momentum-leaders justhodl-estimate-revisions justhodl-earnings-pead justhodl-eps-revision-velocity
- `20:48:12` queue json: {"since": "2026-10-26T14:46:47+00:00", "wave3_queue": ["justhodl-equity-research", "justhodl-momentum-leaders", "justhodl-estimate-revisions", "justhodl-earnings-pead", "justhodl-eps-revision-velocity"], "classes": {"out_of_memory": 1, "exception": 5, "unknown": 1}}
- `20:48:12` ✅ ops 6664 complete (read-only)
