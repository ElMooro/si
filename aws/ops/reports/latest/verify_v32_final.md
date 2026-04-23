# v3.2 final verification — after timezone fix + cache split

**Status:** success  
**Duration:** 0.9s  
**Finished:** 2026-04-23T15:49:18+00:00  

## Data

| check | status | total_last_40min |
|---|---|---|
| name-error | CLEAN |  |
| name-error | CLEAN |  |
| name-error | CLEAN |  |
| errors |  | 1 |

## Log
## 1. Recent daily-report-v3 logs (filter for v3.2 output + errors)

- `15:49:17`   Stream 1: EST]8c9c8346c1af41a2ab2bde24231ee868 (0.5 min ago)
- `15:49:17`     [V10] Start 2026-04-23T15:48:50.112083
- `15:49:17`     [V10] FRED v3.2: skipped 0 via smart TTL ({}), fetching 233
- `15:49:17` ✅     ✓ v3.2 log output present, no NameError
- `15:49:17`   Stream 2: EST]2bc3b8d062cf4052b216ec57a4e12c46 (3.1 min ago)
- `15:49:17`     [V10] Start 2026-04-23T15:46:14.555226
- `15:49:17`     [V10] FRED v3.2: skipped 0 via smart TTL ({}), fetching 233
- `15:49:17` ✅     ✓ v3.2 log output present, no NameError
- `15:49:17`   Stream 3: EST]e098241bc6524cb5bdd91c1ad3310cce (5.5 min ago)
- `15:49:18`     [V10] Start 2026-04-23T15:43:50.082190
- `15:49:18`     [V10] FRED v3.2: skipped 0 via smart TTL ({}), fetching 233
- `15:49:18` ✅     ✓ v3.2 log output present, no NameError
## 2. fred-cache.json — shape + _meta stamps

- `15:49:18` ✗   Cache fetch failed: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
## 3. fred-cache-secretary.json (separate key)

- `15:49:18`   Not yet created (normal — secretary only writes if >70% fetched): An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
## 4. daily-report-v3 Duration trend (last 40 min)

- `15:49:18`   2026-04-23T15:09:00+00:00: avg 267870 ms, max 267870 ms
- `15:49:18`   2026-04-23T15:14:00+00:00: avg 224934 ms, max 224934 ms
- `15:49:18`   2026-04-23T15:19:00+00:00: avg 232984 ms, max 232984 ms
- `15:49:18`   2026-04-23T15:24:00+00:00: avg 236104 ms, max 236104 ms
- `15:49:18`   2026-04-23T15:29:00+00:00: avg 238402 ms, max 238402 ms
- `15:49:18`   2026-04-23T15:34:00+00:00: avg 191508 ms, max 191508 ms
- `15:49:18`   2026-04-23T15:39:00+00:00: avg 262 ms, max 262 ms
## 5. daily-report-v3 Errors (last 40 min)

- `15:49:18`   2026-04-23T15:09:00+00:00: 0 errors
- `15:49:18`   2026-04-23T15:14:00+00:00: 0 errors
- `15:49:18`   2026-04-23T15:19:00+00:00: 0 errors
- `15:49:18`   2026-04-23T15:24:00+00:00: 0 errors
- `15:49:18`   2026-04-23T15:29:00+00:00: 0 errors
- `15:49:18`   2026-04-23T15:34:00+00:00: 0 errors
- `15:49:18`   2026-04-23T15:39:00+00:00: 1 errors
- `15:49:18` Done
