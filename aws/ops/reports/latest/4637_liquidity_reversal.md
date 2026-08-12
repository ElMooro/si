# ops 4637 — global liquidity trend reversal

**Status:** failure  
**Duration:** 129.5s  
**Finished:** 2026-08-12T15:29:31+00:00  

## Error

```
SystemExit: 1
```

## Data

| confirmed | family | liquidity_candidates | list | members | n_lists | n_polarity | resolved | reversal | reversal_label | statistical | trend | trend_label |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | ['10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL', 'Bank Reserves - Great Barometer of Liquidity in the System', 'Banking Sector : Banks = Liquidity Proxy Everywhere. Bank stocks follow bitcoin and  liquidity trends.', 'Bitcoin - Global Liquidity: GOLD ALWAYS BOTTOM BEFORE BITCOIN', 'Bond Global High Yield EX USA : Global Liquidity Barometer', 'Buybacks: great barometer of global liquidity. they increase during high liquidity & easy access to capital markets &vic', 'China Liquidity', 'Collateral: FED control liquidity, Credit and Money supply through Collateral'] |  |  | 491 |  |  |  |  |  |  |  |
| 2 | ["10 YR High Quality Market (HQM)  - PRE", "Bank Reserves - Great Barometer of Liq", "Banking Sector : Banks = Liquidity Pro", "Bitcoin - Global Liquidity: GOLD ALWAY", "Bond Global High Yield EX USA : Global", "Buybacks: great barometer of global li", "China Liquidity", "Collateral: FED control liq |  | LIQUIDITY FAMILY (45 lists) | 1086 |  | 14 | 142 | 14.3 | FORMING TURN TO EASE | 110 | 14.3 | MIXED |

## Log
## pre-dump: liquidity list candidates

- `15:27:22` ✅   [list-exists] liquidity FAMILY present: 45 lists, e.g. ['10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL', 'Bank Reserves - Great Barometer of Liquidity in the System', 'Banking Sector : Banks = Liquidity Proxy Everywhere. Bank stocks follow bitcoin and  liquidity trends.']
## deploy-settle + env + schedule

- `15:27:24` ✅   [deploy] liquidity-reversal v1.1.0 + signal v2.1.4
- `15:27:24` ✅   [env] TE key present
## run + dials truth

- `15:29:24` top reversals: [{"symbol": "FRED:NFCILEVERAGE", "dir": "EASING", "conf": "FORMING", "slope_now_pct": -21.847}, {"symbol": "FRED:RRPONTSYD", "dir": "TIGHTENING", "conf": "FORMING", "slope_now_pct": 8.327}, {"symbol": "FRED:KCFSI", "dir": "TIGHTENING", "conf": "CONFIRMED", "slope_now_pct": 2.039}, {"symbol": "TVC:MO
- `15:29:24` FRED:WFCDA                 trend=DOWN rev=REVERSAL_UP   conf=FORMING   slope=0.024
- `15:29:24` FRED:NFCILEVERAGE          trend=UP   rev=REVERSAL_DOWN conf=FORMING   slope=-21.847
- `15:29:24` FRED:CASACBW027SBOG        trend=UP   rev=NONE          conf=NONE      slope=0.443
- `15:29:24` FRED:HQMCB10YRP-TVC:US10Y  trend=DOWN rev=REVERSAL_DOWN conf=CONFIRMED slope=-0.905
- `15:29:24` FRED:BAMLC0A0CMEY          trend=UP   rev=NONE          conf=NONE      slope=0.09
- `15:29:24` FRED:WLCFLL                trend=UP   rev=REVERSAL_DOWN conf=FORMING   slope=-6.142
- `15:29:24` FRED:DPSACBW027SBOG        trend=UP   rev=NONE          conf=NONE      slope=0.048
- `15:29:24` FRED:TREASURY              trend=UP   rev=REVERSAL_UP   conf=CONFIRMED slope=25.233
- `15:29:24` FRED:NFCICREDIT            trend=DOWN rev=NONE          conf=NONE      slope=-10.86
- `15:29:24` TVC:US06MY-TVC:US03MY      trend=UP   rev=REVERSAL_DOWN conf=FORMING   slope=-0.695
- `15:29:24` FRED:TREAST                trend=UP   rev=NONE          conf=NONE      slope=0.141
- `15:29:24` NASDAQ:VCSH                trend=DOWN rev=NONE          conf=NONE      slope=-0.008
- `15:29:24` AMEX:HYEM                  trend=DOWN rev=NONE          conf=NONE      slope=-0.015
- `15:29:24` ✅   [list-found] list 'LIQUIDITY FAMILY (45 lists)' (1086 members)
- `15:29:24` ✗   [resolution] CONTRACT MISS — 142/1086 resolved (shared caches)
- `15:29:24` ✅   [trend-coverage] 91 rows carry trend/reversal states
- `15:29:24` ✅   [dials] TREND 14.3 (MIXED) · REVERSAL 14.3 (FORMING TURN TO EASE) on 14 polarity rows
## canary + edge

- `15:29:31` ✅   [canary] physical board carries {"state": "AMBER", "trend": "MIXED", "trend_score": 14.3, "reversal": "FORMING TURN TO EASE", "reversal_score": 14.3, "doctrine": 
- `15:29:31` ✅   [edge] page + payload at the edge
## verdict

- `15:29:31` ✗ liquidity-reversal: 1 red (pre-dump above is repair evidence)
