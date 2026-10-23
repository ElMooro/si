# ops 6619 - justhodl-gdelt-sentiment probe (read-only), 2026-10-23 11:36 UTC

**Status:** success  
**Duration:** 3.5s  
**Finished:** 2026-10-23T11:36:09+00:00  

## Data

| errors_72h | invocations_72h | log_lines | max_duration_ms | state | throttles_72h | timeout_s |
|---|---|---|---|---|---|---|
| 0 | 3 | 0 | 1150 | Active | 0 | 300 |

## Log
## function

- `11:36:06` state Active, last modified 2026-09-09T03:31:18.000+0000, timeout 300s, memory 1024 MB, runtime python3.12, reserved concurrency null
- `11:36:06` reserved concurrency (get_function_concurrency): 2
## metrics, last 72 h by hour

- `11:36:06` Invocations (Sum): total/max 3  [["10-21 03:00", 1], ["10-22 03:00", 1], ["10-23 03:00", 1]]
- `11:36:07` Errors (Sum): total/max 0  []
- `11:36:07` Throttles (Sum): total/max 0  []
- `11:36:07` Duration (Maximum): total/max 1150  [["10-21 03:00", 1150], ["10-22 03:00", 425], ["10-23 03:00", 413]]
## log group, last 72 h (errors, timeouts, tracebacks)

- `11:36:08` matching lines: 0 (showing last 0)
- `11:36:09`   10-21 03:43:08: REPORT RequestId: 42a98892-03fc-4f33-b049-6895934a0f37	Duration: 1149.76 ms	Billed Duration: 1460 ms	Memory Size: 1024 MB	Max Memory Used: 170 MB	Init Duration: 309.25 ms	 XRAY TraceId: 1-6ad834cb-61b3f4ff2b3023de78bd8b8d	SegmentId: 46eb802308198362	Sampled: true
## declared output

- `11:36:09` data/gdelt-news.json last modified 2026-10-21 03:43 UTC, 4770 bytes
- `11:36:09` top-level: {"generated_at": "2026-10-21T03:43:08+00:00", "gdelt_timestamp": "20261021034500", "stats": "dict[6]", "themes": "dict[4]", "asset_sentiment": "dict[1]", "headlines_by_tone": "dict[2]"}
- `11:36:09` ✅ read-only; nothing invoked, nothing written
