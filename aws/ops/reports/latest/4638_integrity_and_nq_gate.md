# ops 4638 — integrity + grammar + NQ door

**Status:** failure  
**Duration:** 47.1s  
**Finished:** 2026-08-12T18:08:52+00:00  

## Error

```
SystemExit: 1
```

## Data

| barometer | fn_error | gauge | glabel | invoke_secs | invoke_status | n_stale | resolved | trend_capable |
|---|---|---|---|---|---|---|---|---|
|  | Unhandled |  |  | 21.8 | 200 |  |  |  |
| 0.0 |  | 50.0 | TRENDING |  |  | 0 | 2 | 2 |

## Log
## direct code deploy (ops-side)

- `18:08:10` ✅   [code-deploy] justhodl-liquidity-reversal pushed from checkout
- `18:08:16` ✅   [code-deploy] justhodl-blackswan-watch pushed from checkout
## NQ egress worker (Cloudflare)

- `18:08:17` ✅   [nq-door] reusing live worker from lambda env (smoke PASS, 142 bytes)
- `18:08:29` ✅   [nq-door] worker live + env injected on both engines
## deploy-settle both engines

- `18:08:29` ✅   [deploy] liq v1.3.0 (restored base) + blackswan v1.9.0
## run + audited truth

- `18:08:51` EURONEXT:BANK                res=None  state=None       z=None  
- `18:08:51` FRED:TREASURY                res=None  state=None       z=None  
- `18:08:51` 1/(FRED:TOTLL/FRED:M2SL)     res=None  state=None       z=None  
- `18:08:51` TVC:DE10Y-TVC:IT10Y          res=None  state=None       z=None  
- `18:08:51` 1-FRED:BAMLC0A0CMEY          res=None  state=None       z=None  
- `18:08:51` FX:EURUSD                    res=None  state=None       z=None  
- `18:08:51` SAXO:JPYEUR                  res=None  state=None       z=None  
- `18:08:51` NASDAQ:NQEU3010              res=None  state=None       z=None  
- `18:08:51` ✅   [integrity] EURONEXT:BANK no longer wears the Nasdaq value (res=None via=None)
- `18:08:51` ✗   [stale-guard] CONTRACT MISS — discontinued TREASURY excluded from dials (stale=None)
- `18:08:51` ✗   [grammar] CONTRACT MISS — 0/3 grammar-v2 composites live (constants/parens/EZ-tenor legs)
- `18:08:51` ✗   [fx-prefixes] CONTRACT MISS — 0/2 extended-prefix FX resolved
- `18:08:51` ✗   [nq-unlock] CONTRACT MISS — 0 NQ bank-family rows via the Cloudflare door: []
- `18:08:51` ✅   [dials] gauge 50.0 (TRENDING) on stale-clean rows
## edge

- `18:08:52` ✅   [edge] edge serves the audited payload
## verdict

- `18:08:52` ✗ audit gate: 4 red
