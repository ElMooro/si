# ops 3985 — invoke-only: enforce, settle, fire

**Status:** success  
**Duration:** 158.1s  
**Finished:** 2026-07-28T12:41:53+00:00  

## Data

| memory | timeout | v20_marker_deployed |
|---|---|---|
| 3008 | 900 |  |
|  |  | True |

## Log
- `12:39:23`   pass-1 fired; sleeping 150s so pass-2 lands outside the FRED limiter
- `12:41:53` ✅ FIRED — v1.6 async under 900s/3008MB; verify via ops 3984 in ~13 min
