# ops 4643 — DXY predict-the-future engine

**Status:** success  
**Duration:** 20.2s  
**Finished:** 2026-08-13T01:19:45+00:00  

## Data

| candidates | fn_error | list | members | polarity_rows | resolved | reversal | rlabel | tlabel | trend |
|---|---|---|---|---|---|---|---|---|---|
| [["10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL", 2], ["Bank Reserves - Great Barometer of Liquidity in the System", 2], ["Banking Sector : Banks = Liquidity Proxy Everywhere. Bank stocks follow bitcoin and  liquidity trends.", 129], ["Bitcoin - Global Liquidity: GOLD ALWAYS BOTTOM BEFORE BITCOIN", 248], ["Bitcoin : Nikkei TOP and Bottom in USD ALWAYS MARKED Bitcoin and Crypto Top and Bottom.", 437], ["Bond Global High Yield EX USA : Global Liquidity Barometer", 1], ["Buybacks: great barometer of global liquidity. they increase during high liquidity & easy access to capital markets &vic", 29], ["China Liquidity", 5]] |  |  |  |  |  |  |  |  |  |
|  | None |  |  |  |  |  |  |  |  |
|  |  | LIQUIDITY INDICATORS (2 lists) | 202 | 12 | 153 | 25.0 | FORMING TURN TO EASE | MIXED | 0.0 |

## Log
## pre-dump: liquidity-indicator list candidates

- `01:19:25` ✅   [list-exists] liquidity-indicator list present: [('10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL', 2), ('Bank Reserves - Great Barometer of Liquidity in the System', 2), ('Banking Sector : Banks = Liquidity Proxy Everywhere. Bank stocks follow bitcoin and  liquidity trends.', 129)]
## deploy (ops-side) + settle + schedule

- `01:19:26` ✅   [deploy] v1.0.0 live (created=False)
## run + dxy truth

- `01:19:38` FRED:CPFF                  pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.59
- `01:19:38` FRED:CASACBW027SBOG        pol=+1 trend=UP    rev=NONE           z=1.55
- `01:19:38` FRED:WLCFLL                pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.29
- `01:19:38` FRED:NFCILEVERAGE          pol=-1 trend=UP    rev=REVERSAL_DOWN  z=1.0
- `01:19:38` FRED:BAMLH0A0HYM2          pol=-1 trend=UP    rev=NONE           z=0.67
- `01:19:38` TVC:MOVE                   pol=-1 trend=UP    rev=REVERSAL_DOWN  z=0.6
- `01:19:38` FRED:TOTBKCR               pol=+1 trend=UP    rev=NONE           z=0.52
- `01:19:38` FRED:TOTRESNS              pol=+1 trend=DOWN  rev=NONE           z=0.45
- `01:19:38` FRED:BOGMBASE              pol=+1 trend=UP    rev=NONE           z=0.42
- `01:19:38` FRED:RRPONTSYD             pol=-1 trend=DOWN  rev=REVERSAL_UP    z=0.28
- `01:19:38` FRED:NFCI                  pol=-1 trend=DOWN  rev=NONE           z=0.19
- `01:19:38` FRED:WTREGEN               pol=-1 trend=DOWN  rev=NONE           z=0.09
- `01:19:38` ✅   [list-found] list 'LIQUIDITY INDICATORS (2 lists)' (202 members)
- `01:19:38` ✅   [resolution] 153/202 resolved (shared cache pool)
- `01:19:38` ✅   [polarity] 12 mechanically-signed rows
- `01:19:38` ✅   [dials] LIQ TREND 0.0 (MIXED) · REVERSAL 25.0 (FORMING TURN TO EASE)
## edge (with CF purge)

- `01:19:38` CF purge: True
- `01:19:43` CONTROL dxy: len=6612 has_json=True
- `01:19:44` LIQ body: len=6426 count(liq)=8 fetch@3208 liq@326
- `01:19:44` slice@fetch: :'dim')); fetch('data/liq-indicators.json?cb='+Date.now()).then(r=>r.json()) .then(d=>{ document.getElementById('asof').
- `01:19:44` slice@liq: al" href="https://justhodl.ai/liq-indicators.html"> <meta property="og:title" content="Liquidity Indicators — JustHodl.A
- `01:19:45` ✅   [edge] page + payload at the edge
## verdict

- `01:19:45` ✅ LIQ INDICATORS LIVE — list 'LIQUIDITY INDICATORS (2 lists)': 153/202 resolved, 12 signed rows · TREND 0.0 (MIXED) · REVERSAL 25.0 (FORMING TURN TO EASE) · https://justhodl.ai/liq-indicators.html
