# ops 6786 - the HTTP status Polygon returns to justhodl-corporate-actions (read-only, no secret printed)

**Status:** success  
**Duration:** 4.2s  
**Finished:** 2026-11-11T11:31:44+00:00  

## Data

| aggs_status | dividends_status | engine_key_source | same_key_as_polygon_bank | splits_status |
|---|---|---|---|---|
| 200 | 200 | ssm | True | 200 |

## Log
## 1. the key the engine resolves (names and equality only)

- `11:31:40` SSM /justhodl/polygon/api-key: present (32 chars)
- `11:31:40` function env has 1 variables; Polygon names present: none | timeout 300s memory 256MB | runtime python3.12 | last modified 2026-11-04T21:31:54.000+0000 | state Active/Successful
- `11:31:41` justhodl-polygon-bank resolves an env key; same key as the engine: True
## 2. the engine's three request shapes for AAPL

- `11:31:41` v3 splits -> HTTP 200 | {"status": "OK", "results": 5}
- `11:31:41` v3 dividends -> HTTP 200 | {"status": "OK", "results": 58}
- `11:31:41` v2 aggs 2026-11-10 unadjusted -> HTTP 200 | {"status": "OK", "resultsCount": 1, "count": 1, "queryCount": 1, "results": 1}
## 3. contrast: the same splits call with the key polygon-bank resolves

- `11:31:41` same key as the engine (or unreadable) - the section-2 answer stands for both
## 4. the engine's last invocations (REPORT lines)

- `11:31:42`   11-11 06:48 duration 300 s status timeout
- `11:31:43`   11-10 06:48 duration 300 s status timeout
- `11:31:43`   11-09 06:35 duration 294 s
- `11:31:44`   11-08 06:48 duration 300 s status timeout
- `11:31:44` ✅ all three shapes answer 200 for the engine's key - the failure is not the key or the plan; look at pacing / the universe size
