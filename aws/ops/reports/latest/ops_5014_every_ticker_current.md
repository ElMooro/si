# ops 5014 -- version-gated cache + in-place auto-upgrade

**Status:** failure  
**Duration:** 10.1s  
**Finished:** 2026-08-27T14:27:35+00:00  

## Error

```
SystemExit: behavioral proof failed
```

## Data

| fy | gen_s | periods | revenue_m | schema | ticker | zip_kb |
|---|---|---|---|---|---|---|
|  |  |  |  |  |  | 162 |
|  | 1.1 |  |  |  | MSFT |  |
|  | 2.6 |  |  |  | GOOGL |  |
| FY2025 |  | 3 | 402836.0 | 2.6 | GOOGL |  |

## Log
## G0 preflight

- `14:27:25` ✅ version gate, schema constant, and 3 auto-upgrade layers present
## G1 deploy (code only)

- `14:27:31` ✅ code updated; configuration/env untouched
## P1 behavioral proof on untouched tickers

- `14:27:35` ✅ GOOGL: fresh current-schema doc with reconciling flows
- `14:27:35` ✗ MSFT: served from cache -- version gate did not fire
