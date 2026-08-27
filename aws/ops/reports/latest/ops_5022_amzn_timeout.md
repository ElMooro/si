# ops 5022 -- AMZN first-build vs timeout

**Status:** success  
**Duration:** 8.9s  
**Finished:** 2026-08-27T21:06:05+00:00  

## Data

| age | cache_control | cf_cache_status | edge_firstfail | edge_pt60 | err | gen_s | gfx | memory_mb | schema | timeout_s | url_gfx | url_s | url_schema | url_status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | 1024 |  | 300 |  |  |  |  |
|  |  |  |  |  | None | 3.8 | True |  | 2.9.3 |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | True | 4.2 | 2.9.3 | 200 |
| 0 | max-age=600 | DYNAMIC | 6 | 6 |  |  |  |  |  |  |  |  |  |  |

## Log
## G0 function config

## P1 AMZN cold build (direct invoke, refresh=1)

## G1 timeout headroom (config change only if needed)

- `21:06:01` ✅ timeout 300s has 78.9x headroom over this cold build -- unchanged
## P2 URL-path proof (as the browser calls it)

## P3 edge serves the new page WITHOUT a buster

- `21:06:05` ✅ AMZN builds within limits on the exact browser path and the edge serves the fixed page to plain requests
