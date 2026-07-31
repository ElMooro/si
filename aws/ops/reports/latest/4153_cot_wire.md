# ops 4153 — COT feed + vault wire

**Status:** failure  
**Duration:** 543.5s  
**Finished:** 2026-07-31T02:24:29+00:00  

## Error

```
SystemExit: 1
```

## Data

| cot_adapter | elapsed | feed_err | out | resolved | total_live | wanted |
|---|---|---|---|---|---|---|
|  |  | None | {"resolved": 224, "wanted": 298} |  |  |  |
|  | 11.2 |  |  | 224 |  | 298 |
| 129 |  |  |  |  | 3496 |  |

## Log
- `02:15:36` ✅   justhodl-cot-feed settled at loop 1
- `02:15:48`   miss sample: [["132741_FO_TOR_S", "no-column-or-market"], ["132741_F_TAM_SPREAD", "no-column-or-market"], ["132741_FO_NCP_L_OLD", "unmapped-field"], ["132741_F_CON_NET_LE_8_L", "unmapped-field"], ["132741_F_CON_NET_LE_8_S", "unmapped-field"], ["132741_F_CON_NET_LE_4_S", "unmapped-field"], ["132741_F_CON_NET_LE_4
- `02:15:48`   spot 099741_F_DP_L: got=None want=51686.0
- `02:15:48`   spot 067651_F_MMP_L: got=None want=187469.0
- `02:16:07` ✅   justhodl-tradingview settled at loop 2
- `02:24:28` ✅   artifact after ~480s
- `02:24:29` ✅   schedule cot-feed-daily cron(45 11)
- `02:24:29` ✅   cot-feed settled
- `02:24:29` ✅   resolved >= 220
- `02:24:29` ✗   spot 099741_F_DP_L exact
- `02:24:29` ✗   spot 067651_F_MMP_L exact
- `02:24:29` ✅   vault v3.21.0 settled
- `02:24:29` ✗   feed:cot >= 200
- `02:24:29` ✗   total LIVE >= 3500
- `02:24:29` ✗ FAILED: ['spot 099741_F_DP_L exact', 'spot 067651_F_MMP_L exact', 'feed:cot >= 200', 'total LIVE >= 3500']
