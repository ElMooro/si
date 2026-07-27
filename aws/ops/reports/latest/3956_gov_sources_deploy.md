# ops 3956 — gov-sources registry deploy

**Status:** success  
**Duration:** 112.0s  
**Finished:** 2026-07-27T03:23:34+00:00  

## Data

| n_agencies | n_degraded | n_live | statuses |
|---|---|---|---|
| 13 | 0 | 10 | {'boj': 'LIVE', 'mof_japan': 'LIVE', 'us_treasury': 'LIVE', 'eurostat': 'LIVE', 'norges': 'LIVE', 'bcrp': 'LIVE', 'ecb': 'LIVE', 'boe': 'LIVE', 'snb': 'LIVE', 'imf': 'LIVE', 'fred': 'DOWN', 'bcch': 'BLOCKED_ACCOUNT', 'gov_proxy': 'SPEC_ONLY'} |

## Log
## function: settle or self-heal create

- `03:21:43` ✅   engine settled
## schedule: gov-sources-daily cron(50 11)

- `03:21:43` ✅   schedule created (role from vault schedule)
## invoke + poll artifact

- `03:21:54` ✅   artifact written ~10s
## served page check (edge retries)

- `03:21:54`   attempt 1: HTTP Error 404: Not Found
- `03:22:14`   attempt 2: HTTP Error 404: Not Found
- `03:22:34`   attempt 3: HTTP Error 404: Not Found
- `03:22:54`   attempt 4: HTTP Error 404: Not Found
- `03:23:14`   attempt 5: HTTP Error 404: Not Found
- `03:23:34` ✅   served attempt 6 (7682b)
- `03:23:34`   nav-manifest served has entry: True (soft)
- `03:23:34` ✅   engine deployed (marker in zip)
- `03:23:34` ✅   schedule armed
- `03:23:34` ✅   artifact written
- `03:23:34` ✅   13 agencies
- `03:23:34` ✅   n_live >= 8
- `03:23:34` ✅   boj LIVE
- `03:23:34` ✅   mof_japan LIVE
- `03:23:34` ✅   bcrp LIVE
- `03:23:34` ✅   vault join boj >= 1
- `03:23:34` ✅   vault join mof >= 1
- `03:23:34` ✅   page served with markers
- `03:23:34` ✅ PASS_ALL — gov-sources registry LIVE: 10/13 agencies probing green, page served, schedule armed cron(50 11)
