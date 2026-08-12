# ops 4638 — integrity + grammar + NQ door

**Status:** failure  
**Duration:** 196.5s  
**Finished:** 2026-08-12T15:49:22+00:00  

## Error

```
SystemExit: 1
```

## Data

| barometer | gauge | glabel | n_stale | resolved | trend_capable |
|---|---|---|---|---|---|
| 0.0 | 50.0 | TRENDING | 0 | 2 | 2 |

## Log
## NQ egress worker (Cloudflare)

- `15:46:07` ⚠ worker deploy: HTTP Error 404: Not Found
## deploy-settle both engines

- `15:46:39` ✅   [deploy] liq v1.1.0 + blackswan v1.9.0
## run + audited truth

- `15:46:42` EURONEXT:BANK                res=None  state=None       z=None  
- `15:46:42` FRED:TREASURY                res=None  state=None       z=None  
- `15:46:42` 1/(FRED:TOTLL/FRED:M2SL)     res=None  state=None       z=None  
- `15:46:42` TVC:DE10Y-TVC:IT10Y          res=None  state=None       z=None  
- `15:46:42` 1-FRED:BAMLC0A0CMEY          res=None  state=None       z=None  
- `15:46:42` FX:EURUSD                    res=None  state=None       z=None  
- `15:46:42` SAXO:JPYEUR                  res=None  state=None       z=None  
- `15:46:42` NASDAQ:NQEU3010              res=None  state=None       z=None  
- `15:46:42` ✅   [integrity] EURONEXT:BANK no longer wears the Nasdaq value (res=None via=None)
- `15:46:42` ✗   [stale-guard] CONTRACT MISS — discontinued TREASURY excluded from dials (stale=None)
- `15:46:42` ✗   [grammar] CONTRACT MISS — 0/3 grammar-v2 composites live (constants/parens/EZ-tenor legs)
- `15:46:42` ✗   [fx-prefixes] CONTRACT MISS — 0/2 extended-prefix FX resolved
- `15:46:42` ⚠ nq-unlock skipped (no worker) — 0 NQ rows
- `15:46:42` ✅   [dials] gauge 50.0 (TRENDING) on stale-clean rows
## edge

- `15:49:22` ✗   [edge] CONTRACT MISS — edge serves the audited payload
## verdict

- `15:49:22` ✗ audit gate: 4 red
