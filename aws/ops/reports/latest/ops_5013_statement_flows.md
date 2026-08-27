# ops 5013 -- statement flows (equity-research v2.6 + assets/jh-flows.js)

**Status:** success  
**Duration:** 165.5s  
**Finished:** 2026-08-27T03:06:41+00:00  

## Data

| end_cash_m | fy | gen_s | periods | revenue_m | schema | segments | ticker | zip_kb |
|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | 162 |
|  |  | 4.0 |  |  | 2.6 |  | AAOI |  |
|  |  | 3.5 |  |  | 2.6 |  | NVDA |  |
| 216.0 | FY2025 |  | 3 | 455.7 |  | 4 | AAOI |  |
| 10605.0 | FY2026 |  | 3 | 215938.0 |  | 5 | NVDA |  |

## Log
## G0 preflight -- repo carries every piece

- `03:03:55` ✅ lambda v2.6 markers, shared lib, and OPS5013 block present
## G1 deploy v2.6 (code only)

- `03:04:02` ✅ code updated; configuration/env untouched
## P1 regenerate AAOI+NVDA with real data

- `03:04:02` doc cache busted for AAOI
- `03:04:02` doc cache busted for NVDA
## P2 statement_flows reconciliation on real data

- `03:04:10` ✅ AAOI: all three statements reconcile
- `03:04:10` ✅ NVDA: all three statements reconcile
## G2 live -- lib served + page wired

- `03:04:10` live fetch: HTTP Error 404: Not Found
- `03:04:10` waiting for site sync (lib=False page=False)
- `03:04:25` live fetch: HTTP Error 404: Not Found
- `03:04:25` waiting for site sync (lib=False page=False)
- `03:04:40` live fetch: HTTP Error 404: Not Found
- `03:04:40` waiting for site sync (lib=False page=False)
- `03:04:55` live fetch: HTTP Error 404: Not Found
- `03:04:55` waiting for site sync (lib=False page=False)
- `03:05:10` live fetch: HTTP Error 404: Not Found
- `03:05:10` waiting for site sync (lib=False page=False)
- `03:05:25` live fetch: HTTP Error 404: Not Found
- `03:05:25` waiting for site sync (lib=False page=False)
- `03:05:40` live fetch: HTTP Error 404: Not Found
- `03:05:40` waiting for site sync (lib=False page=False)
- `03:05:55` live fetch: HTTP Error 404: Not Found
- `03:05:55` waiting for site sync (lib=False page=False)
- `03:06:10` live fetch: HTTP Error 404: Not Found
- `03:06:10` waiting for site sync (lib=False page=False)
- `03:06:25` live fetch: HTTP Error 404: Not Found
- `03:06:25` waiting for site sync (lib=False page=False)
- `03:06:41` ✅ assets/jh-flows.js live and OPS5013 on the served page
- `03:06:41` ✅ OPS 5013 PASS -- income / balance-sheet / cash-flow Sankeys live on real data; shared engine available to every research page via assets/jh-flows.js
