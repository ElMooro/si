# ops 5126 -- every symbol from the warehouse; no TradingView widget in AUTO mode (re-run)

**Status:** failure  
**Duration:** 123.2s  
**Finished:** 2026-09-02T14:49:49+00:00  

## Error

```
SystemExit: 1
```

## Data

| banked | families | step | symdir_version |
|---|---|---|---|
|  |  | S1 | 1.2.0 |
| 7 | 15 | S2 |  |

## Log
## S1 deploy tv-bars v1.1 + symdir v1.2.0

- `14:47:46`   zip: 107298 bytes
## 1. Lambda

- `14:47:46`   Lambda exists — updating
- `14:47:52` ✅   ✓ updated justhodl-tv-bars
- `14:47:56`   zip: 132664 bytes
## 1. Lambda

- `14:47:56`   Lambda exists — updating
- `14:47:59` ✅   ✓ updated justhodl-symdir
## S2 /series across symbol families (bank on first open, then S3)

- `14:48:15`   TVC:VIX            n=  9262 first=1990-01-02 last=2026-08-28    662ms ohlc=0 src=warehouse:fred-scoped/Financial_Indicators via=fred:VIXCLS err=
- `14:48:18`   AAPL               n=   169 first=1984-12-01 last=2026-09-02   2709ms ohlc=169 src=warehouse:tv-bars (banked just now) · yahoo-chart:AAPL via=None err=
- `14:48:20`   NASDAQ:AAPL        n=   169 first=1984-12-01 last=2026-09-02   1696ms ohlc=169 src=warehouse:tv-bars (banked just now) · yahoo-chart:AAPL via=None err=
- `14:48:22`   SSE:000001         n=   351 first=1997-07-31 last=2026-09-02   1672ms ohlc=351 src=warehouse:tv-bars (banked just now) · yahoo-chart:000001.SS via=None err=
- `14:48:25`   FX:EURUSD          n=   275 first=2003-12-01 last=2026-09-02   2692ms ohlc=275 src=warehouse:tv-bars (banked just now) · yahoo-chart:EURUSD=X via=None err=
- `14:48:27`   X:BTCUSD           n=   145 first=2014-10-01 last=2026-09-02   1630ms ohlc=145 src=warehouse:tv-bars (banked just now) · yahoo-chart:BTC-USD via=None err=
- `14:48:29`   COINBASE:BTCUSD    n=   145 first=2014-10-01 last=2026-09-02   1604ms ohlc=145 src=warehouse:tv-bars (banked just now) · yahoo-chart:BTC-USD via=None err=
- `14:48:31`   CME_MINI:ES1!      n=   267 first=2000-11-01 last=2026-09-02   1703ms ohlc=267 src=warehouse:tv-bars (banked just now) · yahoo-chart:ES=F via=None err=
- `14:48:33`   HKEX:700           n=   268 first=2004-06-30 last=2026-09-02   1757ms ohlc=268 src=warehouse:tv-bars (banked just now) · yahoo-chart:0700.HK via=None err=
- `14:48:34` ✗   ECONOMICS:DEUR: HTTP Error 500: Internal Server Error
- `14:48:36`   I:SPX              n=   169 first=1984-12-01 last=2026-09-02   1783ms ohlc=169 src=warehouse:tv-bars (banked just now) · yahoo-chart:^GSPC via=None err=
- `14:48:38`   BRK.B              n=   365 first=1996-06-01 last=2026-09-02   1787ms ohlc=365 src=warehouse:tv-bars (banked just now) · yahoo-chart:BRK-B via=None err=
- `14:48:40`   AMEX:SPY           n=   405 first=1993-02-01 last=2026-09-02   1758ms ohlc=405 src=warehouse:tv-bars (banked just now) · yahoo-chart:SPY via=None err=
- `14:48:42` ✗   OTC:AAAIF: HTTP Error 500: Internal Server Error
- `14:48:44`   XETR:DAX           n=   157 first=1987-11-30 last=2026-09-02   1653ms ohlc=157 src=warehouse:tv-bars (banked just now) · yahoo-chart:^GDAXI via=None err=
- `14:48:46`   quote TVC:VIX          ok=True last=14.43 @2026-08-28 chg%=-0.551 err=None
- `14:48:46`   quote AAPL             ok=True last=325.0299987792969 @2026-09-02 chg%=-0.031 err=None
- `14:48:46`   quote ECONOMICS:DEUR   ok=False last=None @None chg%=None err=not banked yet and the bank pull failed: RuntimeError: no bars for ECONOMICS:DEUR (tv:all endpoints 
- `14:48:46`   quote fred:DGS10       ok=True last=4.69 @2026-08-06 chg%=1.296 err=None
- `14:48:46`   universe index: n_symbols=12 failures=17
## S3 live page: native charts, no widget paywall

- `14:49:13`   page TVC:VIX: {"active": "TVC:VIX", "meta": "FRED \u00b7 14.43 Index -0.55% \u00b7 9,262 obs \u00b7 1990-01-02\u21922026-08-28 \u00b7 D \u00b7 warehouse", "loading": "Loading TVC:VIX \u2014 full history\u2026", "iframe": false, "paywall": false, "name": "CBOE Volatility Index: VIX"}
- `14:49:22`   page AAPL: {"active": "AAPL", "meta": "$325.03 -0.03% \u00b7 169d", "loading": "Loading AAPL from Polygon\u2026", "iframe": false, "paywall": false, "name": ""}
- `14:49:31`   page SSE:000001: {"active": "SSE:000001", "meta": "TradingView \u00b7 3,941.4 -0.97% \u00b7 351 obs \u00b7 1997-07-31\u21922026-09-02 \u00b7 D \u00b7 warehouse", "loading": "Loading SSE:000001 \u2014 full history\u2026", "iframe": false, "paywall": false, "name": "000001 (SSE)"}
- `14:49:40`   page ECONOMICS:DEUR: {"active": "ECONOMICS:DEUR", "meta": "SERIES \u00b7 loading full history\u2026", "loading": "No observations for ECONOMICS:DEUR \u2014 not banked yet and the bank pull failed: RuntimeError: no bars for ECONOMICS:DEUR (tv:all endpoints refused: data.tradingview.com/socket.io/websocket?from= )", "iframe": false, "paywall": false, "name": ""}
- `14:49:49`   page NVDA: {"active": "NVDA", "meta": "$223.54 +2.81% \u00b7 250d", "loading": "Loading NVDA from Polygon\u2026", "iframe": false, "paywall": false, "name": ""}
## verdict

- `14:49:49` ✗ AAPL: chart did not render natively: {"active": "AAPL", "meta": "$325.03 -0.03% \u00b7 169d", "loading": "Loading AAPL from Polygon\u2026", "iframe": false, "paywall": false, "name": ""}
- `14:49:49` ✗ ECONOMICS:DEUR: chart did not render natively: {"active": "ECONOMICS:DEUR", "meta": "SERIES \u00b7 loading full history\u2026", "loading": "No observations for ECONOMICS:DEUR \u2014 not banked yet and the ba
- `14:49:49` ✗ NVDA: chart did not render natively: {"active": "NVDA", "meta": "$223.54 +2.81% \u00b7 250d", "loading": "Loading NVDA from Polygon\u2026", "iframe": false, "paywall": false, "name": ""}
