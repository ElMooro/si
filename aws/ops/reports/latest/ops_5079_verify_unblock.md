## P0 throttles SINCE the fix (17:00Z)

**Status:** failure  
**Duration:** 1117.2s  
**Finished:** 2026-08-31T20:19:32+00:00  

## Error

```
SystemExit: 1
```

## Log
- `20:00:55`   last 1h -- census-us throttles=5,054 invocations=103 | fleet throttles=5,071
- `20:00:55`   last 2h -- census-us throttles=9,440 invocations=237 | fleet throttles=9,474
- `20:00:55`   last 3h -- census-us throttles=13,311 invocations=378 | fleet throttles=13,365
- `20:00:55`   ratio now 49.07 refused per success (was 300:1)
- `20:00:55`   still refusing more than it runs -- 20 may be too few for 12 shards plus retries in flight
## P1 the sentinel

- `20:00:55`   health doc generated_at=2026-08-30T15:45:04+00:00 (the page showed this as a day old)
- `20:00:56`   invoke accepted (attempt 1)
- `20:08:58`   sentinel STILL not producing output
- `20:08:58`   dead-lanes chip absent even on a fresh sweep -- then it IS a code path, not the throttling
- `20:08:58`   overall=DEGRADED worst=census-us incidents=5
## P2 census-us timeseries

- `20:08:58`   before updated_at=2026-08-25T23:49:53+00:00 phase=COMPLETE
- `20:08:58`   timeseries invoke accepted (attempt 1)
- `20:19:30`   still unmoved after 10 min
## P3 the lanes

- `20:19:30`   census-econ 1,203 / 1,226 entries
- `20:19:31`   BOJ 60,725 / 120,394 series (50.4%) · 294,539 rows
- `20:19:32`   census-econ objects in S3: 6,765
- `20:19:32` ops 5079 RED: P1:nosweep; P1:nochip; P2:nomove
