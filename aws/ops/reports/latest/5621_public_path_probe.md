# ops 5621 -- public path vs S3 for the reviewed artifacts (read-only)

**Status:** success  
**Duration:** 10.5s  
**Finished:** 2026-09-17T16:24:10+00:00  

## Data

| diagnostic_fields | top_level_keys |
|---|---|
| 0 | ['version', 'methodology_version', 'generated_at', 'elapsed_s', 'n_assets', 'financing_rate_pct', 'by_class', 'all_assets', 'cross_asset_top', 'cross_asset_bottom', 'risk_adjusted_leaders', 'dislocation_leaders', 'unwind_overlay', 'n_dormant', 'regime_summary', 'massive_fx', 'methodology', 'quality', 'financing_source', 'field_units'] |

## Log
## 1. justhodl.ai/data/<key> vs the S3 object

- `16:24:01` ✅ data/carry-surface.json: edge HTTP 200 | S3 09-17 16:22 217,956B | marker='20260910.v1' | {"ok": true, "generated_at": "2026-09-17T16:22:16.495701+00:00"} | quality={"observation_date": "2026-09-15", "publication_date": "2026-09-17T16:22:16.495701+00:00", "frequency": "daily", "freshn
- `16:24:02` ✅ data/jh-fusion.json: edge HTTP 200 | S3 09-17 15:56 348,359B | marker=None | {"generated_at": "2026-09-17T15:56:01Z"}
- `16:24:03` ✅ data/usd-funding.json: edge HTTP 200 | S3 09-16 22:22 10,303B | marker=None | {"status": "GREEN"}
- `16:24:04` ✅ data/floor-audit.json: edge HTTP 200 | S3 09-16 21:37 704,737B | marker=None | {"as_of": "2026-09-16T21:37:18+00:00"}
- `16:24:04` ✅ data/cascade-validation-log.json: edge HTTP 200 | S3 09-16 22:30 13,420B | marker=None | {"generated_at": "2026-09-16T22:30:36.704726+00:00"}
- `16:24:05` ⚠ etf-flows/daily.json: edge HTTP 404 | S3 09-16 22:00 170,860B | marker=None | {"generated_at": "2026-09-16T22:00:25.610097+00:00"}
- `16:24:05`     edge body: b'<!DOCTYPE html>\n<html>\n  <head>\n    <meta http-equiv="Content-type" content="text/html; charset=utf-8">\n    <meta http-equiv="Content-Security-Policy" content="default-src \'none\'; style-src \'unsafe-in'
- `16:24:05` ⚠ macro/regime.json: edge HTTP 404 | S3 09-16 22:15 12,111B | marker=None | {"generated_at": "2026-09-16T22:15:37.335890+00:00"}
- `16:24:05`     edge body: b'<!DOCTYPE html>\n<html>\n  <head>\n    <meta http-equiv="Content-type" content="text/html; charset=utf-8">\n    <meta http-equiv="Content-Security-Policy" content="default-src \'none\'; style-src \'unsafe-in'
- `16:24:06` ✅ data/auction-crisis.json: edge HTTP 200 | S3 09-17 13:06 43,966B | marker=None | {"generated_at": "2026-09-17T13:06:37.523383+00:00"}
- `16:24:06` ✅ data/liquidity-flow.json: edge HTTP 200 | S3 09-17 15:53 15,690B | marker=None | {"generated_at": "2026-09-17T15:53:06+00:00", "as_of": "2026-09-09"} | quality={"observation_date": "2026-09-09", "publication_date": "2026-09-17", "frequency": "mixed", "freshness_basis": "observati
## 1b. Which raw-diagnostic fields does carry-surface.json still carry? (paths + types only, never values)

## 2. Invocations / errors in the last 24h + newest log stream

- `16:24:07` justhodl-carry-surface: code LastModified 2026-09-17T16:21:19.000+0000 | 24h invocations=8 errors=0 | newest log 09-17 16:22
- `16:24:07`     REPORT RequestId: 8f838daf-c3d0-4b61-bfd9-2a0ecbf44c26	Duration: 26838.78 ms	Billed Duration: 27369 ms	Memory Size: 1024 MB	Max Memory Used: 107 MB	Init Duratio
- `16:24:08` justhodl-auction-crisis-detector: code LastModified 2026-09-17T15:53:05.000+0000 | 24h invocations=2 errors=0 | newest log 09-17 13:06
- `16:24:08`     REPORT RequestId: 2b4df270-9d1d-4dde-a39c-3e08900fa326	Duration: 53849.18 ms	Billed Duration: 54277 ms	Memory Size: 1024 MB	Max Memory Used: 105 MB	Init Duratio
- `16:24:09` justhodl-auction-crisis-ai: code LastModified 2026-09-17T16:19:49.000+0000 | 24h invocations=24 errors=0 | newest log 09-17 16:10
- `16:24:09`     [auction-crisis-ai] ERROR: source is 3.1h stale (> 2h limit)
- `16:24:09`     REPORT RequestId: d66aac10-d8be-4a4c-828a-4256e42998c2	Duration: 187.71 ms	Billed Duration: 694 ms	Memory Size: 512 MB	Max Memory Used: 97 MB	Init Duration: 505
- `16:24:10` justhodl-liquidity-flow: code LastModified 2026-09-17T14:43:53.000+0000 | 24h invocations=3 errors=0 | newest log 09-17 15:53
- `16:24:10`     REPORT RequestId: 65226b34-1cd5-4b17-b0f5-06917c6cb89e	Duration: 2806.63 ms	Billed Duration: 3167 ms	Memory Size: 256 MB	Max Memory Used: 98 MB	Init Duration: 3
- `16:24:10` ✅ probe complete (no writes, no invokes)
