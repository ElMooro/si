# ops 4124 — phase stream

**Status:** failure  
**Duration:** 434.8s  
**Finished:** 2026-07-30T14:48:54+00:00  

## Error

```
SystemExit: 1
```

## Data

| total_live |
|---|
| 1400 |

## Log
- `14:41:40` ✅   update accepted (attempt 0)
- `14:41:49` ✅   settled at loop 1
## live phase timeline

- `14:42:50`   [tv-vault][phase] start t+0.0s
- `14:42:50`   [tv-vault] tradingview-vault v3.16.0 ops4129 ladder-wall
- `14:42:50`   [tv-vault][phase] brain-loaded t+0.5s
- `14:42:50`   [tv-vault][phase] registry n=561 t+0.6s
- `14:42:50`   [tv-vault][phase] pre-admission t+0.6s
- `14:42:50`   [tv-vault][phase] fmp-bulk-done n=352 t+3.8s
- `14:42:50`   [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `14:42:50`   [tv-vault][phase] families-preflight-done t+3.8s
- `14:42:50`   [tv-vault][phase] cache age_h=16.8 fresh=True slot=0 t+3.8s
- `14:42:50`   [tv-vault][phase] main-loop-start t+3.8s
- `14:42:50`   [tv-vault][phase] ladder 1 sym=TWEXPYY 0.1s spent=0s t+34.4s
- `14:43:50`   [tv-vault][phase] ladder 2 sym=GE1! 0.0s spent=0s t+72.3s
- `14:43:50`   [tv-vault][phase] ladder 3 sym=GE2! 0.0s spent=0s t+72.3s
- `14:43:50`   [tv-vault][phase] row 500/10059 live=413 lad=3(0s) rev=89s t+93.2s
- `14:43:50`   [tv-vault][phase] ladder 4 sym=BRINTR 0.3s spent=0s t+98.4s
- `14:43:50`   [tv-vault][phase] ladder 5 sym=BTCUSD 3.7s spent=4s t+103.0s
- `14:43:50`   [tv-vault][phase] ladder 6 sym=CRML 3.6s spent=8s t+106.7s
- `14:43:50`   [tv-vault][phase] ladder 7 sym=UUUU 3.7s spent=11s t+110.4s
- `14:44:51`   [tv-vault][phase] ladder 8 sym=ETHM 3.8s spent=15s t+114.1s
- `14:44:51`   [tv-vault][phase] ladder 9 sym=USAR 3.6s spent=19s t+117.8s
- `14:44:51`   [tv-vault][phase] ladder 10 sym=TSMX 3.6s spent=22s t+121.3s
- `14:44:51`   [tv-vault][phase] ladder 11 sym=JBHT 3.6s spent=26s t+125.0s
- `14:44:51`   [tv-vault][phase] ladder 12 sym=QCOM 3.7s spent=30s t+128.7s
- `14:46:52`   [tv-vault][phase] ladder 50 sym=DFM 3.8s spent=188s t+287.4s
- `14:47:53`   [tv-vault][phase] row 1000/10059 live=749 lad=66(242s) rev=103s t+348.7s
- `14:47:53`   [tv-vault][phase] row 1500/10059 live=817 lad=66(242s) rev=111s t+356.3s
- `14:48:53`   [tv-vault][phase] row 2000/10059 live=862 lad=66(242s) rev=120s t+365.2s
- `14:48:53`   [tv-vault][phase] row 2500/10059 live=878 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 3000/10059 live=937 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 3500/10059 live=1018 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 4000/10059 live=1042 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 4500/10059 live=1091 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 5000/10059 live=1149 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 5500/10059 live=1155 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 6000/10059 live=1185 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 6500/10059 live=1206 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 7000/10059 live=1210 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 7500/10059 live=1227 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 8000/10059 live=1246 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 8500/10059 live=1246 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 9000/10059 live=1319 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 9500/10059 live=1323 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] row 10000/10059 live=1395 lad=66(242s) rev=120s t+366.1s
- `14:48:53`   [tv-vault][phase] pre-write t+366.2s
- `14:48:53`   [tv-vault] DONE 366.2s live=1400/10059 cached=926 fred_calls=194
- `14:48:54` ✅   ★ ARTIFACT WROTE v3.15.4 at cycle 6
- `14:48:54`   spot ECONOMICS:BRINTR: got=None src=None
- `14:48:54`   spot ECONOMICS:PEINTR: got=None src=None
- `14:48:54`   spot ECONOMICS:BRFER: got=None src=None
- `14:48:54` ✗   INTR >=25
- `14:48:54` ✗   FER >=80
- `14:48:54` ✗   WB trio >=250
- `14:48:54` ✗   spot ECONOMICS:BRINTR
- `14:48:54` ✗   spot ECONOMICS:PEINTR
- `14:48:54` ✗   spot ECONOMICS:BRFER
- `14:48:54` ✗ FAILED: ['INTR >=25', 'FER >=80', 'WB trio >=250', 'spot ECONOMICS:BRINTR', 'spot ECONOMICS:PEINTR', 'spot ECONOMICS:BRFER']
