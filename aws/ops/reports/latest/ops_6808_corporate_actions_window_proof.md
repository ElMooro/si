# ops 6808 - justhodl-corporate-actions 900 s window proof 2026-11-13 (ladder, then one daily-path Event invocation from the saved cursor)

**Status:** failure  
**Duration:** 1100.0s  
**Finished:** 2026-11-13T12:02:45+00:00  

## Error

```
SystemExit: 1
```

## Data

| cursor_after | cursor_before | deployed_last_modified | due | duration_s | factor_gaps | failed | floored_403 | http_403_lines | plan_floor_403 | processed | stopped_early | written |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 261 | 110 | 2026-11-13T11:48:09.000+0000 | 200 | 841.1 | 63 | 0 | 2269 | 24 | 2021-11-13 | 151 | True | 151 |

## Log
## 1. the three-rung ladder

- `11:44:25` rung 1: repo has the marker
- `11:44:25`   attempt 1: marker False, State Active / Successful - waiting 15 s
- `11:44:41`   attempt 2: marker False, State Active / Successful - waiting 15 s
- `11:44:56`   attempt 3: marker False, State Active / Successful - waiting 15 s
- `11:45:11`   attempt 4: marker False, State Active / Successful - waiting 15 s
- `11:45:27`   attempt 5: marker False, State Active / Successful - waiting 15 s
- `11:45:42`   attempt 6: marker False, State Active / Successful - waiting 15 s
- `11:45:57`   attempt 7: marker False, State Active / Successful - waiting 15 s
- `11:46:13`   attempt 8: marker False, State Active / Successful - waiting 15 s
- `11:46:28`   attempt 9: marker False, State Active / Successful - waiting 15 s
- `11:46:43`   attempt 10: marker False, State Active / Successful - waiting 15 s
- `11:46:59`   attempt 11: marker False, State Active / Successful - waiting 15 s
- `11:47:14`   attempt 12: marker False, State Active / Successful - waiting 15 s
- `11:47:29`   attempt 13: marker False, State Active / Successful - waiting 15 s
- `11:47:45`   attempt 14: marker False, State Active / Successful - waiting 15 s
- `11:48:00`   attempt 15: marker True, State Active / InProgress - waiting 15 s
- `11:48:15` rung 2+3 at attempt 16: artifact has the marker, State Active / LastUpdateStatus Successful, LastModified 2026-11-13T11:48:09.000+0000
## 2. the daily path, once, from the saved cursor (Event invocation; result read back from S3 and the log)

- `11:48:21` before: cursor 110 (state updated_at 2026-11-13T11:36:11.152238+00:00); index LastModified 2026-11-13T11:36:12+00:00
- `11:48:21` invoked at 2026-11-13T11:48:21+00:00: HTTP 202
- `12:02:29` after: cursor 261 (state updated_at 2026-11-13T12:02:22.456775+00:00); index LastModified 2026-11-13T12:02:23+00:00 (rewritten)
- `12:02:45` done: {"cursor": 261, "disagreements": 1950, "due": 200, "duration_s": 841.1, "factor_gaps": 63, "failed": 0, "floored_403": 2269, "plan_floor_403": "2021-11-13", "polygon_key": true, "processed": 151, "stopped_early": true, "universe": 11757, "window": 200, "with_events": 107, "written": 151}
- `12:02:45` 403 lines printed by this run: 24 (yesterday 184; ops 6807 after the floor: 5) | [corpact] plan history floor learned: 403 for 2021-11-13 (AFIF 2021-11-15)
- `12:02:45` ✗ window not cleared: processed 151 of 200 due, stopped_early True, duration 841.1 s
