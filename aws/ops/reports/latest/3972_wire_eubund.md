# ops 3972 — wire EUBUND (ECB) + refresh barometers

**Status:** success  
**Duration:** 476.5s  
**Finished:** 2026-07-27T05:49:04+00:00  

## Data

| after_live | before_gen | before_live | coverage | ledger_as_of | ledger_prev_n | statuses | total_voting | vault_marker_settled |
|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | True |
|  | 2026-07-27T03:02:23.908602+00:00 | 454 |  |  |  |  |  |  |
| 455 |  |  | 81.1 |  |  | {"META": 1, "LIVE": 455, "DISCONTINUED": 2, "NO_FREE_SOURCE": 103} |  |  |
|  |  |  |  | 2026-07-27 | 454 |  | 204 |  |

## Log
## A. settle the vault by marker

- `05:41:08`   [0] justhodl-tradingview busy
## B. invoke the vault (async — it runs long)

- `05:49:01` ✅   vault refreshed after ~460s
- `05:49:01`   EUBUND: status=LIVE value=3.2247 src=ecb:YC asof=ecb:2026-07-23
## C. refresh the barometers so it actually votes

- `05:49:04`   payload={"ok": true, "n_symbols": 561, "barometers": {"MACRO": 50.3, "LIQUIDITY": 33.2, "RISK": 34.8}, "own_notes_pct": 74.5}
- `05:49:04`   EUBUND in barometers: domain=RISK status=LIVE pol=0 tier=T2
- `05:49:04`   MACRO     50.3 NEUTRAL voting=96 of 325
- `05:49:04`   LIQUIDITY 33.2 TIGHTENING voting=47 of 101
- `05:49:04`   RISK      34.8 TIGHTENING voting=61 of 135
- `05:49:04` ✅   vault settled with EUBUND marker
- `05:49:04` ✅   EUBUND is LIVE
- `05:49:04` ✅   EUBUND carries a real value
- `05:49:04` ✅   vault coverage did not regress
- `05:49:04` ✅   barometers invoke clean
- `05:49:04` ✅   EUBUND classified and live in the barometers
- `05:49:04` ✅   ledger now carries prior values for change derivation
- `05:49:04` ✅ PASS_ALL — EUBUND 3.2247 LIVE from ECB; vault 455 live; 204 indicators voting; ledger prev n=454
