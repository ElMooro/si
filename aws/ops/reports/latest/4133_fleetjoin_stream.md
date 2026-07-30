# ops 4124 — phase stream

**Status:** failure  
**Duration:** 687.7s  
**Finished:** 2026-07-30T15:29:23+00:00  

## Error

```
SystemExit: 1
```

## Data

| family:INTR | fleet:finviz | total_live |
|---|---|---|
| 17 | 818 | 2837 |

## Log
- `15:17:55`   EXC ResourceConflictException: An error occurred (ResourceConflictException) when calling the UpdateFunctionCode operation: The ope
- `15:18:06` ✅   update accepted (attempt 1)
- `15:18:15` ✅   settled at loop 1
## live phase timeline

- `15:19:15`   [tv-vault][phase] start t+0.0s
- `15:19:15`   [tv-vault] tradingview-vault v3.17.0 ops4133 fleet-join
- `15:19:15`   [tv-vault][phase] brain-loaded t+0.5s
- `15:19:15`   [tv-vault][phase] registry n=561 t+0.6s
- `15:19:15`   [tv-vault][phase] pre-admission t+0.6s
- `15:19:15`   [tv-vault][phase] fmp-bulk-done n=352 t+4.0s
- `15:19:15`   [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `15:19:15`   [tv-vault][phase] families-preflight-done t+4.0s
- `15:19:15`   [tv-vault] fleet-prices n=11525 asof=2026-07-30
- `15:19:15`   [tv-vault][phase] fleet-prices-preflight-done t+4.6s
- `15:19:15`   [tv-vault][phase] cache age_h=0.3 fresh=True slot=0 t+4.6s
- `15:19:15`   [tv-vault][phase] main-loop-start t+4.6s
- `15:19:15`   [tv-vault][phase] ladder 1 sym=TWEXPYY 0.1s spent=0s t+33.9s
- `15:20:16`   [tv-vault][phase] ladder 2 sym=GE1! 0.0s spent=0s t+72.1s
- `15:20:16`   [tv-vault][phase] ladder 3 sym=GE2! 0.0s spent=0s t+72.1s
- `15:20:16`   [tv-vault][phase] row 500/10059 live=413 lad=3(0s) rev=88s t+92.7s
- `15:21:17`   [tv-vault][phase] slow sym=NOINTR via=norges:IR:KPRA 19.3s t+115.2s
- `15:21:17`   [tv-vault][phase] row 1000/10059 live=777 lad=3(0s) rev=120s t+125.0s
- `15:21:17`   [tv-vault][phase] ladder 4 sym=VWRL 4.2s spent=4s t+129.2s
- `15:21:17`   [tv-vault][phase] ladder 5 sym=ABD4 3.6s spent=8s t+132.8s
- `15:21:17`   [tv-vault][phase] ladder 6 sym=ABD6 3.8s spent=12s t+136.6s
- `15:21:17`   [tv-vault][phase] ladder 7 sym=EMSM 3.8s spent=15s t+140.4s
- `15:21:17`   [tv-vault][phase] ladder 8 sym=JYJ0 3.5s spent=19s t+143.9s
- `15:21:17`   [tv-vault][phase] ladder 9 sym=RVX 3.8s spent=23s t+147.6s
- `15:21:17`   [tv-vault][phase] ladder 10 sym=FRNO 3.7s spent=26s t+151.3s
- `15:21:17`   [tv-vault][phase] ladder 11 sym=FGGE 4.0s spent=30s t+155.3s
- `15:21:17`   [tv-vault][phase] ladder 12 sym=SEMH 3.7s spent=34s t+159.0s
- `15:23:18`   [tv-vault][phase] ladder 50 sym=CN10 4.0s spent=144s t+269.0s
- `15:24:19`   [tv-vault][phase] ladder 100 sym=000028 3.6s spent=216s t+341.3s
- `15:27:21`   [tv-vault][phase] ladder 150 sym=NDXT/CBOE:SIXU 0.0s spent=358s t+483.4s
- `15:29:22`   [tv-vault][phase] row 1500/10059 live=958 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 2000/10059 live=1174 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 2500/10059 live=1260 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 3000/10059 live=1427 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 3500/10059 live=1532 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 4000/10059 live=1576 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 4500/10059 live=1657 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 5000/10059 live=1770 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 5500/10059 live=1904 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 6000/10059 live=2057 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 6500/10059 live=2198 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 7000/10059 live=2259 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 7500/10059 live=2339 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 8000/10059 live=2405 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 8500/10059 live=2432 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 9000/10059 live=2524 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 9500/10059 live=2569 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] row 10000/10059 live=2821 lad=186(480s) rev=120s t+605.4s
- `15:29:22`   [tv-vault][phase] pre-write t+605.5s
- `15:29:22`   [tv-vault] DONE 605.5s live=2837/10059 cached=1562 fred_calls=235
- `15:29:22` ✅   ★ ARTIFACT WROTE v3.15.4 at cycle 10
- `15:29:23`   equity AAPL: LIVE v=331.8 src=fmp
- `15:29:23`   equity NVDA: LIVE v=194.11011 src=fmp
- `15:29:23`   equity MSFT: LIVE v=444.28 src=fmp
- `15:29:23`   spot ECONOMICS:BRINTR: got=14.25 src=bcb-brazil
- `15:29:23`   spot ECONOMICS:PEINTR: got=4.25 src=bcrp-peru
- `15:29:23`   spot ECONOMICS:BRFER: got=369310.0 src=bcb-brazil
- `15:29:23` ✅   INTR >=12 (majors native)
- `15:29:23` ✗   FER >=80
- `15:29:23` ✗   WB trio >=250
- `15:29:23` ✗   fleet:finviz >= 3000
- `15:29:23` ✗   total LIVE >= 5200
- `15:29:23` ✅   equity AAPL LIVE
- `15:29:23` ✅   equity NVDA LIVE
- `15:29:23` ✅   equity MSFT LIVE
- `15:29:23` ✅   spot ECONOMICS:BRINTR
- `15:29:23` ✅   spot ECONOMICS:PEINTR
- `15:29:23` ✅   spot ECONOMICS:BRFER
- `15:29:23` ✗ FAILED: ['FER >=80', 'WB trio >=250', 'fleet:finviz >= 3000', 'total LIVE >= 5200']
