# ops 4643 — DXY predict-the-future engine

**Status:** failure  
**Duration:** 196.4s  
**Finished:** 2026-08-13T00:21:14+00:00  

## Error

```
SystemExit: 1
```

## Data

| candidates | fn_error | list | members | polarity_rows | resolved | reversal | rlabel | tlabel | trend |
|---|---|---|---|---|---|---|---|---|---|
| [["10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL", 2], ["Bank Reserves - Great Barometer of Liquidity in the System", 2], ["Banking Sector : Banks = Liquidity Proxy Everywhere. Bank stocks follow bitcoin and  liquidity trends.", 129], ["Bitcoin - Global Liquidity: GOLD ALWAYS BOTTOM BEFORE BITCOIN", 248], ["Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 437], ["Bond Global High Yield EX USA : Global Liquidity Barometer", 1], ["Buybacks: great barometer of global liquidity. they increase during high liquidity & easy access to capital markets &vic", 29], ["China Liquidity", 5]] |  |  |  |  |  |  |  |  |  |
|  | None |  |  |  |  |  |  |  |  |
|  |  | LIQUIDITY INDICATORS (2 lists) | 202 | 12 | 153 | 25.0 | FORMING TURN TO EASE | MIXED | 0.0 |

## Log
## pre-dump: liquidity-indicator list candidates

- `00:17:58` ✅   [list-exists] liquidity-indicator list present: [('10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL', 2), ('Bank Reserves - Great Barometer of Liquidity in the System', 2), ('Banking Sector : Banks = Liquidity Proxy Everywhere. Bank stocks follow bitcoin and  liquidity trends.', 129)]
## deploy (ops-side) + settle + schedule

- `00:17:59` ✅   [deploy] v1.0.0 live (created=False)
## run + dxy truth

- `00:18:12` FRED:CPFF                  pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.59
- `00:18:12` FRED:CASACBW027SBOG        pol=+1 trend=UP    rev=NONE           z=1.55
- `00:18:12` FRED:WLCFLL                pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.29
- `00:18:12` FRED:NFCILEVERAGE          pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.0
- `00:18:12` FRED:BAMLH0A0HYM2          pol=-1 trend=UP    rev=NONE           z=0.67
- `00:18:12` TVC:MOVE                   pol=-1 trend=UP    rev=REVERSAL_DOWN  z=0.6
- `00:18:12` FRED:TOTBKCR               pol=+1 trend=UP    rev=NONE           z=0.52
- `00:18:12` FRED:TOTRESNS              pol=+1 trend=DOWN  rev=NONE           z=0.45
- `00:18:12` FRED:BOGMBASE              pol=+1 trend=UP    rev=NONE           z=0.42
- `00:18:12` FRED:RRPONTSYD             pol=-1 trend=DOWN  rev=REVERSAL_UP    z=0.28
- `00:18:12` FRED:NFCI                  pol=-1 trend=DOWN  rev=NONE           z=0.19
- `00:18:12` FRED:WTREGEN               pol=-1 trend=DOWN  rev=NONE           z=0.09
- `00:18:12` ✅   [list-found] list 'LIQUIDITY INDICATORS (2 lists)' (202 members)
- `00:18:12` ✅   [resolution] 153/202 resolved (shared cache pool)
- `00:18:12` ✅   [polarity] 12 mechanically-signed rows
- `00:18:12` ✅   [dials] LIQ TREND 0.0 (MIXED) · REVERSAL 25.0 (FORMING TURN TO EASE)
## edge

- `00:21:14` ✗   [edge] CONTRACT MISS — page + payload at the edge
## verdict

- `00:21:14` ✗ liq-indicators: 1 red
