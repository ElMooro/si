# ops 4639 — schema realignment

**Status:** success  
**Duration:** 356.1s  
**Finished:** 2026-08-12T19:00:01+00:00  

## Data

| barometer | fn_error | resolved | reversal | rlabel | tlabel | trend |
|---|---|---|---|---|---|---|
|  | None |  |  |  |  |  |
| 15.2 |  | 685 | 17.4 | FORMING TURN TO EASE | MIXED | 13.0 |

## Log
## deploy (ops-side) + settle

- `18:57:27` ✅   [deploy] v1.3.4 live
## run + row-schema truth

- `18:58:20` FRED:WALCL               chg=+0.15% (WoW)     z=0.79  trend=UP    rev=NONE
- `18:58:20` FRED:DGS10               chg=+1.51% (DoD)     z=1.49  trend=UP    rev=NONE
- `18:58:20` AMEX:JNK                 chg=-0.17% (DoD)     z=0.68  trend=DOWN  rev=NONE
- `18:58:20` TVC:DE10Y-TVC:IT10Y      chg=-3.60% (MoM)     z=0.27  trend=UP    rev=REVERSAL_UP
- `18:58:20` ✅   [row-schema] 4/4 sample rows carry chg+trend+reversal(alias)
- `18:58:20` ✅   [dials] TREND 13.0 (MIXED) · REV 17.4 (FORMING TURN TO EASE)
## edge page/payload

- `19:00:01` ✅   [edge] page reads evolved keys; payload rows render-ready
## verdict

- `19:00:01` ✅ SCHEMA ALIGNED — page and payload speak one language again: TREND 13.0 (MIXED) · REVERSAL 17.4 (FORMING TURN TO EASE) · 685 resolved rows fully rendered
