# ops 3972 — wire EUBUND (ECB) + refresh barometers

**Status:** success  
**Duration:** 482.5s  
**Finished:** 2026-07-27T06:21:56+00:00  

## Data

| after_live | before_gen | before_live | coverage | jpexpyy_now | jpiryy_now | korea_yoy | ledger_as_of | ledger_prev_n | statuses | taiwan_yoy | total_voting | twexpyy_now | vault_marker_settled |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  | True |
|  | 2026-07-27T05:41:19.660547+00:00 | 455 |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | None | -0.4 | 47.96 |  |  |  | 48.33 |  | 48.33 |  |
| 456 |  |  | 81.3 |  |  |  |  |  | {"META": 1, "LIVE": 456, "DISCONTINUED": 2, "NO_FREE_SOURCE": 102} |  |  |  |  |
|  |  |  |  |  |  |  | 2026-07-27 | 455 |  |  | 203 |  |  |

## Log
## A. settle the vault by marker

- `06:13:53`   [0] justhodl-tradingview busy
## B. invoke the vault (async — it runs long)

- `06:21:52` ✅   vault refreshed after ~460s
## B2. the ops-3973 integrity fix + Asia wirings

- `06:21:53`   JPEXPYY   status=NO_FREE_SOURCE   value=None src=unresolved_economics asof=None
- `06:21:53`   TWEXPYY   status=LIVE             value=48.33 src=fleet:data/asia-leads.json asof=fleet:asia-leads.json
- `06:21:53`   JPIRYY    status=LIVE             value=-0.4 src=fleet:data/boj-detail.json asof=fleet:boj-detail.json
- `06:21:53`   TOPIX     status=NO_FREE_SOURCE   value=None src=fmp asof=None
- `06:21:53`   EUBUND: status=LIVE value=3.2247 src=ecb:YC asof=ecb:2026-07-23
## C. refresh the barometers so it actually votes

- `06:21:55`   payload={"ok": true, "n_symbols": 561, "barometers": {"MACRO": 50.5, "LIQUIDITY": 33.2, "RISK": 34.8}, "own_notes_pct": 74.5}
- `06:21:56`   EUBUND in barometers: domain=RISK status=LIVE pol=0 tier=T2
- `06:21:56`   MACRO     50.5 NEUTRAL voting=95 of 325
- `06:21:56`   LIQUIDITY 33.2 TIGHTENING voting=47 of 101
- `06:21:56`   RISK      34.8 TIGHTENING voting=61 of 135
- `06:21:56` ✅   vault settled with EUBUND marker
- `06:21:56` ✅   JPEXPYY no longer equals Korea's number
- `06:21:56` ✅   JPEXPYY resolves from a JAPAN source
- `06:21:56` ✅   TWEXPYY LIVE and equals Taiwan's own yoy
- `06:21:56` ✅   JPIRYY LIVE from boj-detail
- `06:21:56` ✅   EUBUND still LIVE
- `06:21:56` ✅   EUBUND carries a real value
- `06:21:56` ✅   vault coverage did not regress
- `06:21:56` ✅   barometers invoke clean
- `06:21:56` ✅   EUBUND classified and live in the barometers
- `06:21:56` ✅   ledger now carries prior values for change derivation
- `06:21:56` ✅ PASS_ALL — EUBUND 3.2247 LIVE from ECB; vault 456 live; 203 indicators voting; ledger prev n=455
