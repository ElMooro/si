# ops 4643 — DXY predict-the-future engine

**Status:** failure  
**Duration:** 194.0s  
**Finished:** 2026-08-13T00:26:34+00:00  

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

- `00:23:21` ✅   [list-exists] liquidity-indicator list present: [('10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL', 2), ('Bank Reserves - Great Barometer of Liquidity in the System', 2), ('Banking Sector : Banks = Liquidity Proxy Everywhere. Bank stocks follow bitcoin and  liquidity trends.', 129)]
## deploy (ops-side) + settle + schedule

- `00:23:22` ✅   [deploy] v1.0.0 live (created=False)
## run + dxy truth

- `00:23:33` FRED:CPFF                  pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.59
- `00:23:33` FRED:CASACBW027SBOG        pol=+1 trend=UP    rev=NONE           z=1.55
- `00:23:33` FRED:WLCFLL                pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.29
- `00:23:33` FRED:NFCILEVERAGE          pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.0
- `00:23:33` FRED:BAMLH0A0HYM2          pol=-1 trend=UP    rev=NONE           z=0.67
- `00:23:33` TVC:MOVE                   pol=-1 trend=UP    rev=REVERSAL_DOWN  z=0.6
- `00:23:33` FRED:TOTBKCR               pol=+1 trend=UP    rev=NONE           z=0.52
- `00:23:33` FRED:TOTRESNS              pol=+1 trend=DOWN  rev=NONE           z=0.45
- `00:23:33` FRED:BOGMBASE              pol=+1 trend=UP    rev=NONE           z=0.42
- `00:23:33` FRED:RRPONTSYD             pol=-1 trend=DOWN  rev=REVERSAL_UP    z=0.28
- `00:23:33` FRED:NFCI                  pol=-1 trend=DOWN  rev=NONE           z=0.19
- `00:23:33` FRED:WTREGEN               pol=-1 trend=DOWN  rev=NONE           z=0.09
- `00:23:33` ✅   [list-found] list 'LIQUIDITY INDICATORS (2 lists)' (202 members)
- `00:23:33` ✅   [resolution] 153/202 resolved (shared cache pool)
- `00:23:33` ✅   [polarity] 12 mechanically-signed rows
- `00:23:33` ✅   [dials] LIQ TREND 0.0 (MIXED) · REVERSAL 25.0 (FORMING TURN TO EASE)
## edge

- `00:23:33` edge probe 1: page_ok=False len=6412 head=<!DOCTYPE html> <html lang="en"><head><script src="/jh-chart | pay_ok=True keys=['schema_version', 'engine', 'liquidity', 'as_of', 'list_name', 'family_lists']
- `00:23:53` edge probe 2: page_ok=False len=6412 head=<!DOCTYPE html> <html lang="en"><head><script src="/jh-chart | pay_ok=True keys=['schema_version', 'engine', 'liquidity', 'as_of', 'list_name', 'family_lists']
- `00:24:13` edge probe 3: page_ok=False len=6412 head=<!DOCTYPE html> <html lang="en"><head><script src="/jh-chart | pay_ok=True keys=['schema_version', 'engine', 'liquidity', 'as_of', 'list_name', 'family_lists']
- `00:24:34` edge probe 4: page_ok=False len=6412 head=<!DOCTYPE html> <html lang="en"><head><script src="/jh-chart | pay_ok=True keys=['schema_version', 'engine', 'liquidity', 'as_of', 'list_name', 'family_lists']
- `00:24:54` edge probe 5: page_ok=False len=6412 head=<!DOCTYPE html> <html lang="en"><head><script src="/jh-chart | pay_ok=True keys=['schema_version', 'engine', 'liquidity', 'as_of', 'list_name', 'family_lists']
- `00:25:14` edge probe 6: page_ok=False len=6412 head=<!DOCTYPE html> <html lang="en"><head><script src="/jh-chart | pay_ok=True keys=['schema_version', 'engine', 'liquidity', 'as_of', 'list_name', 'family_lists']
- `00:25:34` edge probe 7: page_ok=False len=6412 head=<!DOCTYPE html> <html lang="en"><head><script src="/jh-chart | pay_ok=True keys=['schema_version', 'engine', 'liquidity', 'as_of', 'list_name', 'family_lists']
- `00:25:54` edge probe 8: page_ok=False len=6412 head=<!DOCTYPE html> <html lang="en"><head><script src="/jh-chart | pay_ok=True keys=['schema_version', 'engine', 'liquidity', 'as_of', 'list_name', 'family_lists']
- `00:26:14` edge probe 9: page_ok=False len=6412 head=<!DOCTYPE html> <html lang="en"><head><script src="/jh-chart | pay_ok=True keys=['schema_version', 'engine', 'liquidity', 'as_of', 'list_name', 'family_lists']
- `00:26:34` ✗   [edge] CONTRACT MISS — page + payload at the edge
## verdict

- `00:26:34` ✗ liq-indicators: 1 red
