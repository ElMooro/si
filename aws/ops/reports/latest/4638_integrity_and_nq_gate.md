# ops 4638 — integrity + grammar + NQ door

**Status:** failure  
**Duration:** 141.6s  
**Finished:** 2026-08-12T18:03:39+00:00  

## Error

```
SystemExit: 1
```

## Data

| barometer | fn_error | gauge | glabel | invoke_secs | invoke_status | n_stale | resolved | trend_capable |
|---|---|---|---|---|---|---|---|---|
|  | Unhandled |  |  | 112.9 | 200 |  |  |  |
| 0.0 |  | 50.0 | TRENDING |  |  | 0 | 2 | 2 |

## Log
## direct code deploy (ops-side)

- `18:01:23` ✅   [code-deploy] justhodl-liquidity-reversal pushed from checkout
- `18:01:29` ✅   [code-deploy] justhodl-blackswan-watch pushed from checkout
## NQ egress worker (Cloudflare)

- `18:01:31` ✅   [nq-door] reusing live worker from lambda env (smoke PASS, 142 bytes)
- `18:01:42` ✅   [nq-door] worker live + env injected on both engines
## deploy-settle both engines

- `18:01:43` ✅   [deploy] liq v1.3.0 (restored base) + blackswan v1.9.0
## run + audited truth

- `18:03:36` CW: [ERROR] ZeroDivisionError: division by zero
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 1528, in lambda_handler
    rows.appe
- `18:03:37` EURONEXT:BANK                res=None  state=None       z=None  
- `18:03:37` FRED:TREASURY                res=None  state=None       z=None  
- `18:03:37` 1/(FRED:TOTLL/FRED:M2SL)     res=None  state=None       z=None  
- `18:03:37` TVC:DE10Y-TVC:IT10Y          res=None  state=None       z=None  
- `18:03:37` 1-FRED:BAMLC0A0CMEY          res=None  state=None       z=None  
- `18:03:37` FX:EURUSD                    res=None  state=None       z=None  
- `18:03:37` SAXO:JPYEUR                  res=None  state=None       z=None  
- `18:03:37` NASDAQ:NQEU3010              res=None  state=None       z=None  
- `18:03:37` ✅   [integrity] EURONEXT:BANK no longer wears the Nasdaq value (res=None via=None)
- `18:03:37` ✗   [stale-guard] CONTRACT MISS — discontinued TREASURY excluded from dials (stale=None)
- `18:03:37` ✗   [grammar] CONTRACT MISS — 0/3 grammar-v2 composites live (constants/parens/EZ-tenor legs)
- `18:03:37` ✗   [fx-prefixes] CONTRACT MISS — 0/2 extended-prefix FX resolved
- `18:03:37` ✗   [nq-unlock] CONTRACT MISS — 0 NQ bank-family rows via the Cloudflare door: []
- `18:03:37` ✅   [dials] gauge 50.0 (TRENDING) on stale-clean rows
## edge

- `18:03:39` ✅   [edge] edge serves the audited payload
## verdict

- `18:03:39` ✗ audit gate: 4 red
