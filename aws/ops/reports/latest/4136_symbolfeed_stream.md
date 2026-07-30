# ops 4124 — phase stream

**Status:** success  
**Duration:** 697.7s  
**Finished:** 2026-07-30T16:04:35+00:00  

## Data

| family:INTR | feed:symbol | fleet:finviz | sf_err | sf_out | src-family:bis | src-family:imf | src-family:wb | total_live |
|---|---|---|---|---|---|---|---|---|
|  |  |  | None | {"ok": 425, "err": 193, "resolved": 817} |  |  |  |  |
| 17 | 288 | 818 |  |  | 17 | 114 | 443 | 3160 |

## Log
- `15:52:58` ✅   update accepted (attempt 0)
- `15:53:26` ✅   settled at loop 3
## A2. symbol-feed: create/settle/invoke

## live phase timeline

- `15:55:18`   [tv-vault][phase] start t+0.0s
- `15:55:18`   [tv-vault] tradingview-vault v3.18.0 ops4136 symbol-feed
- `15:55:18`   [tv-vault][phase] brain-loaded t+0.4s
- `15:55:18`   [tv-vault][phase] registry n=561 t+0.5s
- `15:55:18`   [tv-vault][phase] pre-admission t+0.5s
- `15:55:18`   [tv-vault][phase] fmp-bulk-done n=352 t+3.7s
- `15:55:18`   [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `15:55:18`   [tv-vault][phase] families-preflight-done t+3.8s
- `15:55:18`   [tv-vault] fleet-prices n=11525 asof=2026-07-30
- `15:55:18`   [tv-vault][phase] fleet-prices-preflight-done t+4.5s
- `15:55:18`   [tv-vault][phase] cache age_h=0.3 fresh=True slot=0 t+4.5s
- `15:55:18`   [tv-vault][phase] main-loop-start t+4.5s
- `15:55:18`   [tv-vault][phase] ladder 1 sym=TWEXPYY 0.1s spent=0s t+32.7s
- `15:56:20`   [tv-vault][phase] ladder 2 sym=GE1! 0.0s spent=0s t+72.6s
- `15:56:20`   [tv-vault][phase] ladder 3 sym=GE2! 0.0s spent=0s t+72.6s
- `15:56:20`   [tv-vault][phase] row 500/10059 live=413 lad=3(0s) rev=88s t+92.6s
- `15:57:21`   [tv-vault][phase] row 1000/10059 live=777 lad=3(0s) rev=107s t+112.0s
- `15:57:21`   [tv-vault][phase] row 1500/10059 live=970 lad=3(0s) rev=116s t+120.6s
- `15:57:21`   [tv-vault] symbol-feed n=1010
- `15:57:21`   [tv-vault][phase] ladder 4 sym=OMXH25 3.6s spent=4s t+125.5s
- `15:57:21`   [tv-vault][phase] ladder 5 sym=ICERATES1100USD1Y 3.5s spent=7s t+129.1s
- `15:57:21`   [tv-vault][phase] ladder 6 sym=BAMLC8A0C15PYEY-TVC:US10Y 0.0s spent=7s t+129.1s
- `15:57:21`   [tv-vault][phase] ladder 7 sym=SOFR-TVC:US03MY 0.0s spent=7s t+129.1s
- `15:57:21`   [tv-vault][phase] ladder 8 sym=OMRXTOT 3.5s spent=11s t+133.3s
- `15:57:21`   [tv-vault][phase] ladder 9 sym=DECPR 3.7s spent=14s t+137.0s
- `15:57:21`   [tv-vault][phase] ladder 10 sym=USCPR 3.7s spent=18s t+140.7s
- `15:57:21`   [tv-vault][phase] ladder 11 sym=USBI 3.7s spent=22s t+144.4s
- `15:57:21`   [tv-vault][phase] ladder 12 sym=NQUSB50206010 3.8s spent=26s t+148.9s
- `15:59:25`   [tv-vault][phase] ladder 50 sym=IBOV 3.8s spent=131s t+255.8s
- `16:04:35` ✅   ★ ARTIFACT WROTE v3.15.4 at cycle 9
- `16:04:35`   equity AAPL: LIVE v=331.64999 src=fmp
- `16:04:35`   equity NVDA: LIVE v=192.925 src=fmp
- `16:04:35`   equity MSFT: LIVE v=449.415 src=fmp
- `16:04:35`   spot ECONOMICS:BRINTR: got=14.25 src=bcb-brazil
- `16:04:35`   spot ECONOMICS:PEINTR: got=4.25 src=bcrp-peru
- `16:04:35`   spot ECONOMICS:BRFER: got=369310.0 src=bcb-brazil
- `16:04:35` ✅   INTR family-src >=12
- `16:04:35` ✅   FER family-src >=80
- `16:04:35` ✅   WB trio via src >=250
- `16:04:35` ✅   fleet:finviz >= 600 (US subset of a GLOBAL book)
- `16:04:35` ✅   symbol-feed >= 250
- `16:04:35` ✅   total LIVE >= 3000
- `16:04:35` ✅   equity AAPL LIVE
- `16:04:35` ✅   equity NVDA LIVE
- `16:04:35` ✅   equity MSFT LIVE
- `16:04:35` ✅   spot ECONOMICS:BRINTR
- `16:04:35` ✅   spot ECONOMICS:PEINTR
- `16:04:35` ✅   spot ECONOMICS:BRFER
- `16:04:35` ✅ PASS_ALL — WROTE + families {'feed:symbol': 288, 'fleet:finviz': 818, 'src-family:imf': 114, 'src-family:wb': 443, 'family:INTR': 17, 'src-family:bis': 17} + total LIVE 3160
