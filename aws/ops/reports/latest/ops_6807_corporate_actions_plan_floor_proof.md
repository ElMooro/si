# ops 6807 - justhodl-corporate-actions plan-history-floor proof 2026-11-13 (ladder, then one daily-path Event invocation from the saved cursor)

**Status:** failure  
**Duration:** 432.7s  
**Finished:** 2026-11-13T11:36:36+00:00  

## Error

```
SystemExit: 1
```

## Data

| cursor_after | cursor_before | deployed_last_modified | due | duration_s | factor_gaps | failed | floored_403 | http_403_lines | plan_floor_403 | processed | stopped_early | written |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 110 | 45 | 2026-11-13T11:31:56.000+0000 | 200 | 244.2 | 22 | 0 | 557 | 5 | 2021-11-11 | 65 | True | 65 |

## Log
## 1. the three-rung ladder

- `11:29:23` rung 1: repo has the marker
- `11:29:24`   attempt 1: marker False, State Active / Successful - waiting 15 s
- `11:29:40`   attempt 2: marker False, State Active / Successful - waiting 15 s
- `11:29:56`   attempt 3: marker False, State Active / Successful - waiting 15 s
- `11:30:11`   attempt 4: marker False, State Active / Successful - waiting 15 s
- `11:30:27`   attempt 5: marker False, State Active / Successful - waiting 15 s
- `11:30:43`   attempt 6: marker False, State Active / Successful - waiting 15 s
- `11:30:58`   attempt 7: marker False, State Active / Successful - waiting 15 s
- `11:31:14`   attempt 8: marker False, State Active / Successful - waiting 15 s
- `11:31:30`   attempt 9: marker False, State Active / Successful - waiting 15 s
- `11:31:45`   attempt 10: marker False, State Active / Successful - waiting 15 s
- `11:32:01` rung 2+3 at attempt 11: artifact has the marker, State Active / LastUpdateStatus Successful, LastModified 2026-11-13T11:31:56.000+0000
## 2. the daily path, once, from the saved cursor (Event invocation; result read back from S3 and the log)

- `11:32:06` before: cursor 45 (state updated_at 2026-11-13T06:34:38.142921+00:00); index LastModified 2026-11-13T06:34:39+00:00
- `11:32:06` invoked at 2026-11-13T11:32:06+00:00: HTTP 202
- `11:36:21` after: cursor 110 (state updated_at 2026-11-13T11:36:11.152238+00:00); index LastModified 2026-11-13T11:36:12+00:00 (rewritten)
- `11:36:36` done: {"cursor": 110, "disagreements": 777, "due": 200, "duration_s": 244.2, "factor_gaps": 22, "failed": 0, "floored_403": 557, "plan_floor_403": "2021-11-11", "polygon_key": true, "processed": 65, "stopped_early": true, "universe": 11757, "window": 200, "with_events": 42, "written": 65}
- `11:36:36` 403 lines printed by this run: 5 (yesterday's sweep: 184) | [corpact] plan history floor learned: 403 for 2021-11-11 (ABR 2021-11-12)
- `11:36:36` ✗ window not cleared: processed 65 of 200 due, stopped_early True, duration 244.2 s
