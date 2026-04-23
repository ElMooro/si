# v3.2 verification — smart TTL in production

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-04-23T15:43:40+00:00  

## Data

| check | daily | monthly | sample_stamped | total | weekly |
|---|---|---|---|---|---|
| cache-meta-stamps |  |  | 0 | 25 |  |
| cadence-dist | 0 | 0 |  |  | 0 |

## Log
## 1. daily-report-v3 logs (latest stream)

- `15:43:40`   Stream: $LATEST]794a421813bf4435952d8f9477b47995 (1.7 min ago)
- `15:43:40`     [V10] Start 2026-04-23T15:42:00.598719
- `15:43:40`     [ERROR] NameError: name 'timezone' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 1616, in lambda_handler
    should_skip, reason = _should_skip_fetch(ca
- `15:43:40`     [V10] Start 2026-04-23T15:43:01.004843
- `15:43:40`     [V10] FRED v3.2: skipped 0 via smart TTL ({}), fetching 233
## 2. fred-cache.json — check for _meta.fetched_at stamps

- `15:43:40`   Cache last modified: 2026-04-23T15:42:03+00:00 (1.6 min ago)
- `15:43:40`   Total series: 25
## 3. Inferred cadence distribution (what TTL would classify)

- `15:43:40`   daily (≤3d): 0
- `15:43:40`   weekly (4-10d): 0
- `15:43:40`   monthly (11-45d): 0
- `15:43:40`   quarterly (46-120d): 0
- `15:43:40`   annual (>120d): 0
- `15:43:40`   unknown: 0
## 4. daily-report-v3 duration trend (last 30 min)

- `15:43:40`   2026-04-23T15:13:00+00:00: 267870 ms
- `15:43:40`   2026-04-23T15:18:00+00:00: 224934 ms
- `15:43:40`   2026-04-23T15:23:00+00:00: 232984 ms
- `15:43:40`   2026-04-23T15:28:00+00:00: 236104 ms
- `15:43:40`   2026-04-23T15:33:00+00:00: 238402 ms
- `15:43:40`   2026-04-23T15:38:00+00:00: 95885 ms
- `15:43:40` Done
