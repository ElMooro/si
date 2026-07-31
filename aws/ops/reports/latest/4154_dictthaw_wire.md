# ops 4153 — COT feed + vault wire

**Status:** failure  
**Duration:** 528.6s  
**Finished:** 2026-07-31T02:35:11+00:00  

## Error

```
SystemExit: 1
```

## Data

| cot_adapter | elapsed | feed_err | out | resolved | total_live | wanted |
|---|---|---|---|---|---|---|
|  |  | None | {"resolved": 228, "wanted": 298} |  |  |  |
|  | 10.7 |  |  | 228 |  | 298 |
| 227 |  |  |  |  | 3667 |  |

## Log
- `02:26:33` ✅   justhodl-cot-feed settled at loop 1
- `02:26:45`   miss sample: [["132741_FO_TOR_S", "no-column-or-market"], ["132741_F_TAM_SPREAD", "no-column-or-market"], ["132741_FO_NCP_L_OLD", "unmapped-field"], ["146021_F_NRP_L_OLD", "unmapped-field"], ["132741_F_TC_L", "unmapped-field"], ["132741_F_CP_L_OLD", "unmapped-field"], ["132741_F_TAM_L", "no-column-or-market"], [
- `02:26:45`   spot 020601_FO_AMP_SPREAD: v=266327.0 col=asset_mgr_positions_spread asof=2026-07-21
- `02:26:45`   spot 132741_FO_TAM_S: v=0.0 col=traders_asset_mgr_short_all asof=2023-06-13
- `02:26:45`   whale 132741 probe (yw9f): [{"id":"220913132741C","market_and_exchange_names":"EURODOLLARS-3M - CHICAGO MERCANTILE EXCHANGE","report_date_as_yyyy_mm_dd":"2022-09-13T00:00:00.000","yyyy_report_week_ww":"2022 
- `02:26:55` ✅   justhodl-tradingview settled at loop 1
- `02:35:11` ✅   artifact after ~480s
- `02:35:11` ✅   schedule exists
- `02:35:11` ✅   cot-feed settled
- `02:35:11` ✗   resolved >= 235
- `02:35:11` ✅   spot 020601_FO_AMP_SPREAD plausible
- `02:35:11` ✗   spot 132741_FO_TAM_S plausible
- `02:35:11` ✅   vault v3.21.0 settled
- `02:35:11` ✅   feed:cot >= 180
- `02:35:11` ✅   total LIVE >= 3450
- `02:35:11` ✗ FAILED: ['resolved >= 235', 'spot 132741_FO_TAM_S plausible']
