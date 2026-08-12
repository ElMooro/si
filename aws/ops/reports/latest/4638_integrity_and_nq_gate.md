# ops 4638 — integrity + grammar + NQ door

**Status:** success  
**Duration:** 95.8s  
**Finished:** 2026-08-12T18:39:35+00:00  

## Data

| barometer | first_errors | fn_error | gauge | glabel | invoke_secs | invoke_status | n_stale | resolved | row_errors | trend_capable |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  | None |  |  | 69.7 | 200 |  |  |  |  |
|  | [] |  |  |  |  |  |  |  | None |  |
| 15.2 |  |  | 13.0 | MIXED |  |  | 17 | 685 |  | None |

## Log
## direct code deploy (ops-side)

- `18:38:05` ✅   [code-deploy] justhodl-liquidity-reversal pushed from checkout
- `18:38:11` ✅   [code-deploy] justhodl-blackswan-watch pushed from checkout
## NQ egress worker (Cloudflare)

- `18:38:13` proxy body[:220]: {"data":null,"message":null,"status":{"rCode":400,"bCodeMessage":[{"code":1001,"errorMessage":"Symbol not exists."}],"developerMessage":null}}
- `18:38:13` ✅   [nq-door] reusing live worker from lambda env (smoke PASS, 142 bytes)
- `18:38:25` ✅   [nq-door] worker live + env injected on both engines
## deploy-settle both engines

- `18:38:25` ✅   [deploy] liq v1.3.0 (restored base) + blackswan v1.9.0
## run + audited truth

- `18:39:35` EURONEXT:BANK                res=True  state=CALM       z=None  
- `18:39:35` FRED:TREASURY                res=True  state=STALE      z=1.05  
- `18:39:35` 1/(FRED:TOTLL/FRED:M2SL)     res=True  state=CALM       z=0.57  
- `18:39:35` TVC:DE10Y-TVC:IT10Y          res=True  state=CALM       z=0.27  
- `18:39:35` 1-FRED:BAMLC0A0CMEY          res=True  state=CALM       z=1.37  
- `18:39:35` FX:EURUSD                    res=True  state=CALM       z=0.24  
- `18:39:35` SAXO:JPYEUR                  res=True  state=CALM       z=0.14  
- `18:39:35` NASDAQ:NQEU3010              res=False state=UNRESOLVED z=None  
- `18:39:35` ✅   [integrity] EURONEXT:BANK no longer wears the Nasdaq value (res=True via=None)
- `18:39:35` ✅   [stale-guard] discontinued TREASURY excluded from dials (stale=True)
- `18:39:35` ✅   [grammar] 3/3 grammar-v2 composites live (constants/parens/EZ-tenor legs)
- `18:39:35` ✅   [fx-prefixes] 2/2 extended-prefix FX resolved
- `18:39:35` ⚠ NQ via door: 0 — probe body above decides wall vs bug; route stays armed
- `18:39:35` ✅   [nq-unlock] 0 NQ rows (armed; body forensic logged)
- `18:39:35` ✅   [dials] TREND 13.0 (MIXED) · REVERSAL 17.4 (FORMING TURN TO EASE)
## edge

- `18:39:35` ✅   [edge] edge serves the audited payload
## verdict

- `18:39:35` ✅ AUDIT SEALED — integrity restored, stale-guarded dials 13.0/MIXED, grammar v2 live (EZ spreads), NQ door OPEN (0 rows) · resolved 685
