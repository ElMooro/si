# ops 4637 — global liquidity trend reversal

**Status:** failure  
**Duration:** 167.1s  
**Finished:** 2026-08-12T15:24:09+00:00  

## Error

```
SystemExit: 1
```

## Data

| confirmed | liquidity_candidates | list | members | n_lists | n_polarity | resolved | reversal | reversal_label | statistical | trend | trend_label |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  | ['10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL', 'Bank Reserves - Great Barometer of Liquidity in the System', 'Banking Sector : Banks = Liquidity Proxy Everywhere. Bank stocks follow bitcoin and  liquidity trends.', 'Bitcoin - Global Liquidity: GOLD ALWAYS BOTTOM BEFORE BITCOIN', 'Bond Global High Yield EX USA : Global Liquidity Barometer', 'Buybacks: great barometer of global liquidity. they increase during high liquidity & easy access to capital markets &vic', 'China Liquidity', 'Collateral: FED control liquidity, Credit and Money supply through Collateral'] |  |  | 491 |  |  |  |  |  |  |  |
| 0 |  | 10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL | 2 |  | 0 | 2 | None | NONE | 2 | None | UNKNOWN |

## Log
## pre-dump: liquidity list candidates

- `15:21:22` ✅   [list-exists] liquidity list present: ['10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL', 'Bank Reserves - Great Barometer of Liquidity in the System', 'Banking Sector : Banks = Liquidity Proxy Everywhere. Bank stocks follow bitcoin and  liquidity trends.']
## deploy-settle + env + schedule

- `15:21:52` ✅   [deploy] liquidity-reversal v1.0.0 + signal v2.1.4
- `15:21:58` ✅   [env] TE key present
## run + dials truth

- `15:22:00` top reversals: []
- `15:22:00` FRED:HQMCB10YR             trend=UP   rev=NONE          conf=NONE      slope=1.594
- `15:22:00` FRED:HQMCB10YRP            trend=UP   rev=NONE          conf=NONE      slope=1.623
- `15:22:00` ✅   [list-found] list '10 YR High Quality Market (HQM)  - PREDICT FUTURE LIQUIDITY TREND REVERSAL' (2 members)
- `15:22:00` ✗   [resolution] CONTRACT MISS — 2/2 resolved (shared caches)
- `15:22:00` ✗   [trend-coverage] CONTRACT MISS — 2 rows carry trend/reversal states
- `15:22:00` ✗   [dials] CONTRACT MISS — TREND None (UNKNOWN) · REVERSAL None (NONE) on 0 polarity rows
## canary + edge

- `15:22:08` ✅   [canary] physical board carries {"state": "CALM", "trend": "UNKNOWN", "trend_score": null, "reversal": "NONE", "reversal_score": null, "doctrine": "global liquidi
- `15:22:08` edge 1: HTTP Error 404: Not Found
- `15:22:28` edge 2: HTTP Error 404: Not Found
- `15:22:48` edge 3: HTTP Error 404: Not Found
- `15:23:08` edge 4: HTTP Error 404: Not Found
- `15:23:28` edge 5: HTTP Error 404: Not Found
- `15:23:48` edge 6: HTTP Error 404: Not Found
- `15:24:09` ✅   [edge] page + payload at the edge
## verdict

- `15:24:09` ✗ liquidity-reversal: 3 red (pre-dump above is repair evidence)
