# ops 5014 -- version-gated cache + in-place auto-upgrade

**Status:** success  
**Duration:** 14.6s  
**Finished:** 2026-08-27T14:32:27+00:00  

## Data

| from_cache | fy | gen_s | periods | revenue_m | schema | ticker | zip_kb |
|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | 162 |
| False |  | 3.2 |  |  |  | ORCL |  |
|  | FY2026 |  | 3 | 67358.0 | 2.6 | ORCL |  |
|  |  | 0.2 |  |  |  | MSFT |  |
|  | FY2026 |  | 3 | 331839.0 | 2.6 | MSFT |  |
|  |  | 2.0 |  |  |  | GOOGL |  |
|  | FY2025 |  | 3 | 402836.0 | 2.6 | GOOGL |  |

## Log
## G0 preflight

- `14:32:12` ✅ version gate, schema constant, and 3 auto-upgrade layers present
## G1 deploy (code only)

- `14:32:19` ✅ code updated; configuration/env untouched
## P0 controlled stale-schema probe (ORCL)

- `14:32:21` ORCL cache doctored: schema '2.6' -> 2.5-test-stale
- `14:32:24` ✅ ORCL: fresh current-schema doc with reconciling flows
## P1 behavioral proof on untouched tickers

- `14:32:25` ✅ MSFT: fresh current-schema doc with reconciling flows
- `14:32:27` ✅ GOOGL: fresh current-schema doc with reconciling flows
## G2 live page carries the auto-upgrade layers

- `14:32:27` ✅ served page carries all 3 auto-upgrade layers
- `14:32:27` ✅ OPS 5014 PASS -- every ticker now serves the current engine: stale-schema cache is a miss server-side, and the page upgrades itself in place
