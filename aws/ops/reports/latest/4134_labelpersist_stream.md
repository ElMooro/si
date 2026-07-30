# ops 4124 — phase stream

**Status:** failure  
**Duration:** 692.8s  
**Finished:** 2026-07-30T15:46:19+00:00  

## Error

```
SystemExit: 1
```

## Data

| family:INTR | fleet:finviz | total_live |
|---|---|---|
| 17 | 818 | 2863 |

## Log
- `15:34:47` ✅   update accepted (attempt 0)
- `15:35:06` ✅   settled at loop 2
## live phase timeline

- `15:36:07`   [tv-vault][phase] start t+0.0s
- `15:36:07`   [tv-vault] tradingview-vault v3.17.1 ops4134 label-persist
- `15:36:07`   [tv-vault][phase] brain-loaded t+0.5s
- `15:36:07`   [tv-vault][phase] registry n=561 t+0.6s
- `15:36:07`   [tv-vault][phase] pre-admission t+0.6s
- `15:36:07`   [tv-vault][phase] fmp-bulk-done n=352 t+4.0s
- `15:36:07`   [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `15:36:07`   [tv-vault][phase] families-preflight-done t+4.1s
- `15:36:07`   [tv-vault] fleet-prices n=11525 asof=2026-07-30
- `15:36:07`   [tv-vault][phase] fleet-prices-preflight-done t+4.7s
- `15:36:07`   [tv-vault][phase] cache age_h=0.3 fresh=True slot=0 t+4.7s
- `15:36:07`   [tv-vault][phase] main-loop-start t+4.7s
- `15:36:07`   [tv-vault][phase] ladder 1 sym=TWEXPYY 0.1s spent=0s t+36.3s
- `15:37:08`   [tv-vault][phase] ladder 2 sym=GE1! 0.0s spent=0s t+74.1s
- `15:37:08`   [tv-vault][phase] ladder 3 sym=GE2! 0.0s spent=0s t+74.1s
- `15:37:08`   [tv-vault][phase] row 500/10059 live=413 lad=3(0s) rev=90s t+94.6s
- `15:38:09`   [tv-vault][phase] row 1000/10059 live=777 lad=3(0s) rev=110s t+114.5s
- `15:38:09`   [tv-vault][phase] ladder 4 sym=VSPBONDETF 3.5s spent=4s t+123.7s
- `15:38:09`   [tv-vault][phase] ladder 5 sym=XISB 4.4s spent=8s t+128.1s
- `15:38:09`   [tv-vault][phase] ladder 6 sym=LYQS 3.9s spent=12s t+132.0s
- `15:38:09`   [tv-vault][phase] ladder 7 sym=FEDFUNDS-TVC:US10Y 0.0s spent=12s t+132.0s
- `15:38:09`   [tv-vault][phase] ladder 8 sym=FEDFUNDS-TVC:US02Y 0.0s spent=12s t+132.0s
- `15:38:09`   [tv-vault][phase] ladder 9 sym=NQG4040 3.8s spent=16s t+135.8s
- `15:38:09`   [tv-vault][phase] ladder 10 sym=GBKX 3.6s spent=19s t+139.4s
- `15:38:09`   [tv-vault][phase] ladder 11 sym=QAUTO 4.1s spent=23s t+143.5s
- `15:38:09`   [tv-vault][phase] ladder 12 sym=USTVS 3.7s spent=27s t+147.2s
- `15:40:11`   [tv-vault][phase] ladder 50 sym=US02Y-TVC:US10Y) 0.0s spent=117s t+238.2s
- `15:40:11`   [tv-vault][phase] ladder 100 sym=VOL.UPTK.NY 0.0s spent=155s t+275.9s
- `15:41:12`   [tv-vault][phase] row 1500/10059 live=970 lad=140(210s) rev=118s t+332.9s
- `15:42:14`   [tv-vault][phase] ladder 150 sym=DEIPMM 3.6s spent=246s t+369.0s
- `15:45:17`   [tv-vault][phase] ladder 200 sym=DWTY 3.5s spent=419s t+542.1s
- `15:46:19`   [tv-vault][phase] row 2000/10059 live=1200 lad=217(487s) rev=121s t+612.7s
- `15:46:19`   [tv-vault][phase] row 2500/10059 live=1286 lad=217(487s) rev=121s t+612.7s
- `15:46:19`   [tv-vault][phase] row 3000/10059 live=1453 lad=217(487s) rev=121s t+612.7s
- `15:46:19`   [tv-vault][phase] row 3500/10059 live=1558 lad=217(487s) rev=121s t+612.7s
- `15:46:19`   [tv-vault][phase] row 4000/10059 live=1602 lad=217(487s) rev=121s t+612.7s
- `15:46:19`   [tv-vault][phase] row 4500/10059 live=1683 lad=217(487s) rev=121s t+612.7s
- `15:46:19`   [tv-vault][phase] row 5000/10059 live=1796 lad=217(487s) rev=121s t+612.7s
- `15:46:19`   [tv-vault][phase] row 5500/10059 live=1930 lad=217(487s) rev=121s t+612.7s
- `15:46:19`   [tv-vault][phase] row 6000/10059 live=2083 lad=217(487s) rev=121s t+612.7s
- `15:46:19`   [tv-vault][phase] row 6500/10059 live=2224 lad=217(487s) rev=121s t+612.7s
- `15:46:19`   [tv-vault][phase] row 7000/10059 live=2285 lad=217(487s) rev=121s t+612.7s
- `15:46:19`   [tv-vault][phase] row 7500/10059 live=2365 lad=217(487s) rev=121s t+612.8s
- `15:46:19`   [tv-vault][phase] row 8000/10059 live=2431 lad=217(487s) rev=121s t+612.8s
- `15:46:19`   [tv-vault][phase] row 8500/10059 live=2458 lad=217(487s) rev=121s t+612.8s
- `15:46:19`   [tv-vault][phase] row 9000/10059 live=2550 lad=217(487s) rev=121s t+612.8s
- `15:46:19`   [tv-vault][phase] row 9500/10059 live=2595 lad=217(487s) rev=121s t+612.8s
- `15:46:19`   [tv-vault][phase] row 10000/10059 live=2847 lad=217(487s) rev=121s t+612.8s
- `15:46:19`   [tv-vault][phase] pre-write t+612.8s
- `15:46:19`   [tv-vault] DONE 612.8s live=2863/10059 cached=1751 fred_calls=255
- `15:46:19` ✅   ★ ARTIFACT WROTE v3.15.4 at cycle 10
- `15:46:19`   equity AAPL: LIVE v=332.02 src=fmp
- `15:46:19`   equity NVDA: LIVE v=193.43 src=fmp
- `15:46:19`   equity MSFT: LIVE v=446.265 src=fmp
- `15:46:19`   spot ECONOMICS:BRINTR: got=14.25 src=bcb-brazil
- `15:46:19`   spot ECONOMICS:PEINTR: got=4.25 src=bcrp-peru
- `15:46:19`   spot ECONOMICS:BRFER: got=369310.0 src=bcb-brazil
- `15:46:19` ✅   INTR >=12 (majors native)
- `15:46:19` ✗   FER >=80
- `15:46:19` ✗   WB trio >=250
- `15:46:19` ✅   fleet:finviz >= 600 (US subset of a GLOBAL book)
- `15:46:19` ✅   total LIVE >= 2750
- `15:46:19` ✅   equity AAPL LIVE
- `15:46:19` ✅   equity NVDA LIVE
- `15:46:19` ✅   equity MSFT LIVE
- `15:46:19` ✅   spot ECONOMICS:BRINTR
- `15:46:19` ✅   spot ECONOMICS:PEINTR
- `15:46:19` ✅   spot ECONOMICS:BRFER
- `15:46:19` ✗ FAILED: ['FER >=80', 'WB trio >=250']
