# ops 3211 — ledger counted right, crash named or gone

**Status:** failure  
**Duration:** 85.6s  
**Finished:** 2026-07-13T05:06:50+00:00  

## Error

```
SystemExit: 1
```

## Data

| bad_sids_named | n_fails | n_warns | new_errors | scan_pages | verdict | wl_signals |
|---|---|---|---|---|---|---|
|  |  |  |  | 15 |  | 13 |
| 0 |  |  | 0 |  |  |  |
|  | 1 | 0 |  |  | FAIL |  |

## Log
## 1. Paginated ledger count

- `05:05:26`   wl#wl-frontier-market-etfs#2026-28  UP
- `05:05:26`   wl#wl-global-commodities-prices#2026-28  UP
- `05:05:26`   wl#wl-foreign-exchange-reserves#2026-28  UP
- `05:05:26`   wl#wl-fed-powell-holding#2026-28  UP
## 2. Guarded run: crash gone or NAMED

- `05:06:49` ✅ index fresh (2026-07-13T05:05:27) — 24 firing
- `05:06:50` ✗ only 13 wl_ signals after full pagination
