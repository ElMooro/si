# ops 4921 -- floor-audit v2.0.0 whole market + call

**Status:** failure  
**Duration:** 61.7s  
**Finished:** 2026-08-20T01:48:51+00:00  

## Error

```
SystemExit: 1
```

## Data

| alerts | as_of | deep | frame | g0_ok | g1 | g2 | g3 | max_deep | memory | min_mcap_usd | prescreen_min_cov | resolved | screened | timeout |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | PASS |  |  |  | 2048 |  |  |  |  | 900 |
|  |  |  |  |  |  | PASS |  | 120 |  | 15000000.0 | 0.4 |  |  |  |
| 9 | 2026-08-20T01:48:42+00:00 | 48 |  | 43 |  |  | PASS |  |  |  |  |  | 8 |  |
|  |  |  | shares |  |  |  |  |  |  |  |  | CY2026Q3I |  |  |
|  |  |  | prices |  |  |  |  |  |  |  |  | 2026-08-19 |  |  |
|  |  |  | cash |  |  |  |  |  |  |  |  | CY2026Q3I |  |  |
|  |  |  | cash_ifrs |  |  |  |  |  |  |  |  | None |  |  |
|  |  |  | st_inv |  |  |  |  |  |  |  |  | CY2026Q2I |  |  |
|  |  |  | lt_inv |  |  |  |  |  |  |  |  | CY2026Q2I |  |  |
|  |  |  | crypto |  |  |  |  |  |  |  |  | CY2026Q2I |  |  |
|  |  |  | debt_nc |  |  |  |  |  |  |  |  | CY2026Q2I |  |  |
|  |  |  | debt_c |  |  |  |  |  |  |  |  | CY2026Q2I |  |  |
|  |  |  | st_borrow |  |  |  |  |  |  |  |  | CY2026Q3I |  |  |
|  |  |  | rpo |  |  |  |  |  |  |  |  | CY2026Q2I |  |  |

## Log
## G1 deploy

- `01:47:49`   zip: 117020 bytes
## 1. Lambda

- `01:47:49`   Lambda exists — updating
- `01:47:55` ✅   ✓ updated justhodl-floor-audit
## 3. Smoke test

- `01:47:55`   invoking justhodl-floor-audit…
## G2 config reset

## G3 fresh run

## G4 market breadth

- `01:48:51` FAIL G4: screened=8 tiers={'mega': 0, 'large': 0, 'mid': 1, 'small': 3, 'micro': 3, 'nano': 1} -- the sweep did not reach the whole market
