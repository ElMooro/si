# ops 4638 — integrity + grammar + NQ door

**Status:** failure  
**Duration:** 359.1s  
**Finished:** 2026-08-12T18:22:06+00:00  

## Error

```
SystemExit: 1
```

## Data

| barometer | fn_error | gauge | glabel | invoke_secs | invoke_status | n_stale | resolved | trend_capable |
|---|---|---|---|---|---|---|---|---|
|  | None |  |  | 168.5 | 200 |  |  |  |
| 14.7 |  | None | None |  |  | 0 | 691 | None |

## Log
## direct code deploy (ops-side)

- `18:16:12` ✅   [code-deploy] justhodl-liquidity-reversal pushed from checkout
- `18:16:18` ✅   [code-deploy] justhodl-blackswan-watch pushed from checkout
## NQ egress worker (Cloudflare)

- `18:16:22` ✅   [nq-door] reusing live worker from lambda env (smoke PASS, 142 bytes)
- `18:16:34` ✅   [nq-door] worker live + env injected on both engines
## deploy-settle both engines

- `18:16:34` ✅   [deploy] liq v1.3.0 (restored base) + blackswan v1.9.0
## run + audited truth

- `18:19:24` EURONEXT:BANK                res=True  state=CALM       z=None  
- `18:19:24` FRED:TREASURY                res=True  state=CALM       z=1.05  
- `18:19:24` 1/(FRED:TOTLL/FRED:M2SL)     res=True  state=CALM       z=0.57  
- `18:19:24` TVC:DE10Y-TVC:IT10Y          res=True  state=CALM       z=0.27  
- `18:19:24` 1-FRED:BAMLC0A0CMEY          res=True  state=CALM       z=1.37  
- `18:19:24` FX:EURUSD                    res=True  state=CALM       z=0.24  
- `18:19:24` SAXO:JPYEUR                  res=True  state=CALM       z=0.14  
- `18:19:24` NASDAQ:NQEU3010              res=False state=UNRESOLVED z=None  
- `18:19:24` ✅   [integrity] EURONEXT:BANK no longer wears the Nasdaq value (res=True via=None)
- `18:19:24` ✗   [stale-guard] CONTRACT MISS — discontinued TREASURY excluded from dials (stale=None)
- `18:19:24` ✅   [grammar] 3/3 grammar-v2 composites live (constants/parens/EZ-tenor legs)
- `18:19:24` ✅   [fx-prefixes] 2/2 extended-prefix FX resolved
- `18:19:24` ✗   [nq-unlock] CONTRACT MISS — 0 NQ bank-family rows via the Cloudflare door: []
- `18:19:24` ✗   [dials] CONTRACT MISS — gauge None (None) on stale-clean rows
## edge

- `18:22:06` ✗   [edge] CONTRACT MISS — edge serves the audited payload
## verdict

- `18:22:06` ✗ audit gate: 4 red
