# ops 5621 -- public path vs S3 for the reviewed artifacts (read-only)

**Status:** success  
**Duration:** 16.4s  
**Finished:** 2026-09-17T16:22:53+00:00  

## Data

| diagnostic_fields |
|---|
| 0 |

## Log
## 1. justhodl.ai/data/<key> vs the S3 object

- `16:22:38` ⚠ data/carry-surface.json: edge HTTP 503 | S3 09-17 14:49 257,345B | marker=None | {"generated_at": "2026-09-17T14:49:26.084283+00:00"}
- `16:22:38`     edge body: b'{"error":"reviewed artifact unavailable"}'
- `16:22:45` ✅ data/jh-fusion.json: edge HTTP 200 | S3 09-17 15:56 348,359B | marker=None | {"generated_at": "2026-09-17T15:56:01Z"}
- `16:22:46` ✅ data/usd-funding.json: edge HTTP 200 | S3 09-16 22:22 10,303B | marker=None | {"status": "GREEN"}
- `16:22:46` ✅ data/floor-audit.json: edge HTTP 200 | S3 09-16 21:37 704,737B | marker=None | {"as_of": "2026-09-16T21:37:18+00:00"}
- `16:22:47` ✅ data/cascade-validation-log.json: edge HTTP 200 | S3 09-16 22:30 13,420B | marker=None | {"generated_at": "2026-09-16T22:30:36.704726+00:00"}
- `16:22:47` ⚠ etf-flows/daily.json: edge HTTP 404 | S3 09-16 22:00 170,860B | marker=None | {"generated_at": "2026-09-16T22:00:25.610097+00:00"}
- `16:22:47`     edge body: b'<!DOCTYPE html>\n<html>\n  <head>\n    <meta http-equiv="Content-type" content="text/html; charset=utf-8">\n    <meta http-equiv="Content-Security-Policy" content="default-src \'none\'; style-src \'unsafe-in'
- `16:22:48` ⚠ macro/regime.json: edge HTTP 404 | S3 09-16 22:15 12,111B | marker=None | {"generated_at": "2026-09-16T22:15:37.335890+00:00"}
- `16:22:48`     edge body: b'<!DOCTYPE html>\n<html>\n  <head>\n    <meta http-equiv="Content-type" content="text/html; charset=utf-8">\n    <meta http-equiv="Content-Security-Policy" content="default-src \'none\'; style-src \'unsafe-in'
- `16:22:48` ✅ data/auction-crisis.json: edge HTTP 200 | S3 09-17 13:06 43,966B | marker=None | {"generated_at": "2026-09-17T13:06:37.523383+00:00"}
- `16:22:49` ✅ data/liquidity-flow.json: edge HTTP 200 | S3 09-17 15:53 15,690B | marker=None | {"generated_at": "2026-09-17T15:53:06+00:00", "as_of": "2026-09-09"} | quality={"observation_date": "2026-09-09", "publication_date": "2026-09-17", "frequency": "mixed", "freshness_basis": "observati
## 1b. Which raw-diagnostic fields does carry-surface.json still carry? (paths + types only, never values)

## 2. Invocations / errors in the last 24h + newest log stream

- `16:22:50` justhodl-carry-surface: code LastModified 2026-09-17T16:21:19.000+0000 | 24h invocations=7 errors=0 | newest log 09-17 16:22
- `16:22:51` justhodl-auction-crisis-detector: code LastModified 2026-09-17T15:53:05.000+0000 | 24h invocations=2 errors=0 | newest log 09-17 13:06
- `16:22:51`     REPORT RequestId: 2b4df270-9d1d-4dde-a39c-3e08900fa326	Duration: 53849.18 ms	Billed Duration: 54277 ms	Memory Size: 1024 MB	Max Memory Used: 105 MB	Init Duratio
- `16:22:52` justhodl-auction-crisis-ai: code LastModified 2026-09-17T16:19:49.000+0000 | 24h invocations=24 errors=0 | newest log 09-17 16:10
- `16:22:52`     [auction-crisis-ai] ERROR: source is 3.1h stale (> 2h limit)
- `16:22:52`     REPORT RequestId: d66aac10-d8be-4a4c-828a-4256e42998c2	Duration: 187.71 ms	Billed Duration: 694 ms	Memory Size: 512 MB	Max Memory Used: 97 MB	Init Duration: 505
- `16:22:53` justhodl-liquidity-flow: code LastModified 2026-09-17T14:43:53.000+0000 | 24h invocations=3 errors=0 | newest log 09-17 15:53
- `16:22:53`     REPORT RequestId: 65226b34-1cd5-4b17-b0f5-06917c6cb89e	Duration: 2806.63 ms	Billed Duration: 3167 ms	Memory Size: 256 MB	Max Memory Used: 98 MB	Init Duration: 3
- `16:22:53` ✅ probe complete (no writes, no invokes)
