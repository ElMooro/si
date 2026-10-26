# ops 6672 - the five fleet-health failures: 24 h of REPORT lines, errors and their own prints (read-only)

**Status:** success  
**Duration:** 9.9s  
**Finished:** 2026-10-26T15:01:13+00:00  

## Log
## justhodl-analytics-snapshot

- `15:01:04` REPORT: 3 events
- `15:01:04`    errors/timeouts 3, ok 0; last ok at None
- `15:01:04`    09:10:07 | Duration: 600000.00 ms Max Memory Used: 282 MB Status: timeout ⏎ XRAY TraceId: 1-6adf1696-6089f18219ae521550533b19
- `15:01:04`    09:21:03 | Duration: 600000.00 ms Max Memory Used: 285 MB Status: timeout ⏎ XRAY TraceId: 1-6adf1696-6089f18219ae521550533b19
- `15:01:04`    09:32:56 | Duration: 600000.00 ms Max Memory Used: 278 MB Status: timeout ⏎ XRAY TraceId: 1-6adf1696-6089f18219ae521550533b19
- `15:01:05` engine lines: 0 events
## justhodl-earnings-whisper

- `15:01:05` REPORT: 3 events
- `15:01:05`    errors/timeouts 3, ok 0; last ok at None
- `15:01:05`    08:01:41 | Duration: 60000.00 ms Max Memory Used: 256 MB Status: timeout ⏎ XRAY TraceId: 1-6adf08a7-514202e8792dff8f79ed066c
- `15:01:05`    08:03:43 | Duration: 60000.00 ms Max Memory Used: 256 MB Status: timeout ⏎ XRAY TraceId: 1-6adf08a7-514202e8792dff8f79ed066c
- `15:01:05`    08:06:56 | Duration: 60000.00 ms Max Memory Used: 256 MB Status: timeout ⏎ XRAY TraceId: 1-6adf08a7-514202e8792dff8f79ed066c
- `15:01:06` engine lines: 3 events
- `15:01:06`    08:00:41 | [whisper] Loading data…
- `15:01:06`    08:02:43 | [whisper] Loading data…
- `15:01:06`    08:05:56 | [whisper] Loading data…
## treasury-auto-updater

- `15:01:08` REPORT: 3 events
- `15:01:08`    errors/timeouts 3, ok 0; last ok at None
- `15:01:08`    11:00:15 | Duration: 3000.00 ms Max Memory Used: 91 MB Status: timeout ⏎ XRAY TraceId: 1-6adf32ba-2960d6432b3aade257631760
- `15:01:08`    11:01:14 | Duration: 3000.00 ms Max Memory Used: 91 MB Status: timeout ⏎ XRAY TraceId: 1-6adf32ba-2960d6432b3aade257631760
- `15:01:08`    11:03:07 | Duration: 3000.00 ms Max Memory Used: 91 MB Status: timeout ⏎ XRAY TraceId: 1-6adf32ba-2960d6432b3aade257631760
- `15:01:08` engine lines: 0 events
## justhodl-signal-genealogy

- `15:01:10` REPORT: 6 events
- `15:01:10`    errors/timeouts 0, ok 6; last ok at 13:39:53
- `15:01:10`    13:33:03 | Duration: 120865.93 ms Max Memory Used: 257 MB
- `15:01:10`    13:36:00 | Duration: 116306.72 ms Max Memory Used: 260 MB
- `15:01:10`    13:39:53 | Duration: 114886.94 ms Max Memory Used: 261 MB
- `15:01:10` engine lines: 6 events
- `15:01:10`    06:42:36 | [ERROR] ValueError: unreconciled archive cannot produce chronology ⏎ Traceback (most recent call last): ⏎   File "/var/task/lambda_function.py", line 33, in lambda_handler ⏎     result=run(client,client,cutoff,Path(directory)/'run',guard) ⏎   File "/var/task/genealogy_native_runtime.py", line 105, i
- `15:01:10`    06:45:21 | [ERROR] ValueError: unreconciled archive cannot produce chronology ⏎ Traceback (most recent call last): ⏎   File "/var/task/lambda_function.py", line 33, in lambda_handler ⏎     result=run(client,client,cutoff,Path(directory)/'run',guard) ⏎   File "/var/task/genealogy_native_runtime.py", line 105, i
- `15:01:10`    06:49:22 | [ERROR] ValueError: unreconciled archive cannot produce chronology ⏎ Traceback (most recent call last): ⏎   File "/var/task/lambda_function.py", line 33, in lambda_handler ⏎     result=run(client,client,cutoff,Path(directory)/'run',guard) ⏎   File "/var/task/genealogy_native_runtime.py", line 105, i
- `15:01:10`    13:33:03 | [ERROR] ValueError: unreconciled archive cannot produce chronology ⏎ Traceback (most recent call last): ⏎   File "/var/task/lambda_function.py", line 33, in lambda_handler ⏎     result=run(client,client,cutoff,Path(directory)/'run',guard) ⏎   File "/var/task/genealogy_native_runtime.py", line 105, i
- `15:01:10`    13:36:00 | [ERROR] ValueError: unreconciled archive cannot produce chronology ⏎ Traceback (most recent call last): ⏎   File "/var/task/lambda_function.py", line 33, in lambda_handler ⏎     result=run(client,client,cutoff,Path(directory)/'run',guard) ⏎   File "/var/task/genealogy_native_runtime.py", line 105, i
- `15:01:10`    13:39:53 | [ERROR] ValueError: unreconciled archive cannot produce chronology ⏎ Traceback (most recent call last): ⏎   File "/var/task/lambda_function.py", line 33, in lambda_handler ⏎     result=run(client,client,cutoff,Path(directory)/'run',guard) ⏎   File "/var/task/genealogy_native_runtime.py", line 105, i
## justhodl-event-flow-monitor

- `15:01:11` REPORT: 3 events
- `15:01:11`    errors/timeouts 3, ok 0; last ok at None
- `15:01:11`    05:02:41 | Duration: 120000.00 ms Max Memory Used: 512 MB Status: timeout ⏎ XRAY TraceId: 1-6adede79-423beaf20b3bb96015f5125f
- `15:01:11`    05:05:47 | Duration: 120000.00 ms Max Memory Used: 511 MB Status: timeout ⏎ XRAY TraceId: 1-6adede79-423beaf20b3bb96015f5125f
- `15:01:11`    05:09:51 | Duration: 120000.00 ms Max Memory Used: 511 MB Status: timeout ⏎ XRAY TraceId: 1-6adede79-423beaf20b3bb96015f5125f
- `15:01:12` engine lines: 0 events
## treasury-api

- `15:01:12` REPORT: 0 events
- `15:01:12`    errors/timeouts 0, ok 0; last ok at None
- `15:01:13` engine lines: 0 events
- `15:01:13` ✅ ops 6672 complete (read-only)
