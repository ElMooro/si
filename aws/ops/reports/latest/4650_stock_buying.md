# ops 4650 — stock-buying screener

**Status:** failure  
**Duration:** 184.5s  
**Finished:** 2026-08-13T17:14:23+00:00  

## Error

```
SystemExit: 1
```

## Data

| candidates | census_rows | closes_tickers | fn_error | gate_census | has_SPY | missing |
|---|---|---|---|---|---|---|
|  | 0 |  |  |  |  |  |
|  |  | 1200 |  |  | True |  |
|  |  |  | Unhandled |  |  |  |
| None |  |  |  | {} |  | {} |

## Log
## input-store key evidence

## deploy (create-capable) + settle + schedule

- `17:11:20` ✅   [deploy] v1.0.0 live (created=False)
## run + screener truth

- `17:11:22` ✗   [pipeline] CONTRACT MISS — universe None -> under-SMA None -> RSI None -> passed None
- `17:11:22` ✗   [rows] CONTRACT MISS — None candidates; top row scored + why-linked
## edge

- `17:14:23` ✗   [edge] CONTRACT MISS — page + payload at the edge
## verdict

- `17:14:23` ✗ stock-buying: 3 red
