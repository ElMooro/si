# ops 4638 — integrity + grammar + NQ door

**Status:** failure  
**Duration:** 201.1s  
**Finished:** 2026-08-12T18:32:59+00:00  

## Error

```
SystemExit: 1
```

## Data

| barometer | first_errors | fn_error | gauge | glabel | invoke_secs | invoke_status | n_stale | resolved | row_errors | trend_capable |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  | None |  |  | 172.4 | 200 |  |  |  |  |
|  | [] |  |  |  |  |  |  |  | None |  |
| 15.2 |  |  | None | None |  |  | 17 | 685 |  | None |

## Log
## direct code deploy (ops-side)

- `18:29:44` ✅   [code-deploy] justhodl-liquidity-reversal pushed from checkout
- `18:29:50` ✅   [code-deploy] justhodl-blackswan-watch pushed from checkout
## NQ egress worker (Cloudflare)

- `18:29:52` proxy body[:220]: {"data":null,"message":null,"status":{"rCode":400,"bCodeMessage":[{"code":1001,"errorMessage":"Symbol not exists."}],"developerMessage":null}}
- `18:29:52` ✅   [nq-door] reusing live worker from lambda env (smoke PASS, 142 bytes)
- `18:30:04` ✅   [nq-door] worker live + env injected on both engines
## deploy-settle both engines

- `18:30:05` ✅   [deploy] liq v1.3.0 (restored base) + blackswan v1.9.0
## run + audited truth

- `18:32:58` EURONEXT:BANK                res=True  state=CALM       z=None  
- `18:32:58` FRED:TREASURY                res=True  state=STALE      z=1.05  
- `18:32:58` 1/(FRED:TOTLL/FRED:M2SL)     res=True  state=CALM       z=0.57  
- `18:32:58` TVC:DE10Y-TVC:IT10Y          res=True  state=CALM       z=0.27  
- `18:32:58` 1-FRED:BAMLC0A0CMEY          res=True  state=CALM       z=1.37  
- `18:32:58` FX:EURUSD                    res=True  state=CALM       z=0.24  
- `18:32:58` SAXO:JPYEUR                  res=True  state=CALM       z=0.14  
- `18:32:58` NASDAQ:NQEU3010              res=False state=UNRESOLVED z=None  
- `18:32:58` ✅   [integrity] EURONEXT:BANK no longer wears the Nasdaq value (res=True via=None)
- `18:32:58` ✅   [stale-guard] discontinued TREASURY excluded from dials (stale=True)
- `18:32:58` ✅   [grammar] 3/3 grammar-v2 composites live (constants/parens/EZ-tenor legs)
- `18:32:58` ✅   [fx-prefixes] 2/2 extended-prefix FX resolved
- `18:32:58` ⚠ NQ via door: 0 — probe body above decides wall vs bug; route stays armed
- `18:32:58` ✅   [nq-unlock] 0 NQ rows (armed; body forensic logged)
- `18:32:58` ✗   [dials] CONTRACT MISS — TREND None (None) · REVERSAL None (None)
## edge

- `18:32:59` ✅   [edge] edge serves the audited payload
## verdict

- `18:32:59` ✗ audit gate: 1 red
