# ops 4643 — DXY predict-the-future engine

**Status:** failure  
**Duration:** 195.3s  
**Finished:** 2026-08-13T00:15:59+00:00  

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

- `00:12:44` ✅   [list-exists] liquidity-indicator list present: [('10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL', 2), ('Bank Reserves - Great Barometer of Liquidity in the System', 2), ('Banking Sector : Banks = Liquidity Proxy Everywhere. Bank stocks follow bitcoin and  liquidity trends.', 129)]
## deploy (ops-side) + settle + schedule

- `00:12:45` ✅   [deploy] v1.0.0 live (created=False)
- `00:12:46` hourly schedule created
## run + dxy truth

- `00:12:57` FRED:CPFF                  pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.59
- `00:12:57` FRED:CASACBW027SBOG        pol=+1 trend=UP    rev=NONE           z=1.55
- `00:12:57` FRED:WLCFLL                pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.29
- `00:12:57` FRED:NFCILEVERAGE          pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.0
- `00:12:57` FRED:BAMLH0A0HYM2          pol=-1 trend=UP    rev=NONE           z=0.67
- `00:12:57` TVC:MOVE                   pol=-1 trend=UP    rev=REVERSAL_DOWN  z=0.6
- `00:12:57` FRED:TOTBKCR               pol=+1 trend=UP    rev=NONE           z=0.52
- `00:12:57` FRED:TOTRESNS              pol=+1 trend=DOWN  rev=NONE           z=0.45
- `00:12:57` FRED:BOGMBASE              pol=+1 trend=UP    rev=NONE           z=0.42
- `00:12:57` FRED:RRPONTSYD             pol=-1 trend=DOWN  rev=REVERSAL_UP    z=0.28
- `00:12:57` FRED:NFCI                  pol=-1 trend=DOWN  rev=NONE           z=0.19
- `00:12:57` FRED:WTREGEN               pol=-1 trend=DOWN  rev=NONE           z=0.09
- `00:12:57` ✅   [list-found] list 'LIQUIDITY INDICATORS (2 lists)' (202 members)
- `00:12:57` ✅   [resolution] 153/202 resolved (shared cache pool)
- `00:12:57` ✅   [polarity] 12 mechanically-signed rows
- `00:12:57` ✅   [dials] LIQ TREND 0.0 (MIXED) · REVERSAL 25.0 (FORMING TURN TO EASE)
## edge

- `00:12:57` edge 1: HTTP Error 404: Not Found
- `00:13:17` edge 2: HTTP Error 404: Not Found
- `00:13:37` edge 3: HTTP Error 404: Not Found
- `00:13:57` edge 4: HTTP Error 404: Not Found
- `00:14:18` edge 5: HTTP Error 404: Not Found
- `00:14:38` edge 6: HTTP Error 404: Not Found
- `00:14:58` edge 7: HTTP Error 404: Not Found
- `00:15:59` ✗   [edge] CONTRACT MISS — page + payload at the edge
## verdict

- `00:15:59` ✗ dxy-predict: 1 red
