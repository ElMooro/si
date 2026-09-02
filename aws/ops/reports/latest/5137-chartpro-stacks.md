# ops 5137 -- chart-pro: where do the residual LightweightCharts errors originate

**Status:** failure  
**Duration:** 86.7s  
**Finished:** 2026-09-02T17:50:37+00:00  

## Error

```
SystemExit: 1
```

## Log
## S1 static content live?

- `17:49:12`   live: {"data": true, "plumbing": true, "chart": true}
- `17:49:12` ✅   data.html static inventory: 57 providers, as_of 2026-09-02T16:48:53+00:00, totals {'providers': 57, 'datasets': 811792, 'keys': 1922373, 'gb': 530.41}
## S2 chart-pro native charts (headless Chrome)

- `17:49:43`   errors after initial load (NVDA): []
- `17:49:52`   AAPL: {"active": "AAPL", "meta": "$325.03 +0.16% \u00b7 250 bars \u00b7 JustHodl warehouse \u00b7 since 1980", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1980"}
- `17:50:01`   NVDA: {"active": "NVDA", "meta": "$223.54 -1.32% \u00b7 250 bars \u00b7 JustHodl warehouse \u00b7 since 1999", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `17:50:10`   SSE:000001: {"active": "SSE:000001", "meta": "TradingView \u00b7 3,941.4 -0.97% \u00b7 7,186 obs \u00b7 1997-07-02\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `17:50:19`   TVC:VIX: {"active": "TVC:VIX", "meta": "FRED \u00b7 14.43 Index -0.55% \u00b7 9,262 obs \u00b7 1990-01-02\u21922026-08-28 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `17:50:28`   XETR:DAX: {"active": "XETR:DAX", "meta": "TradingView \u00b7 25,846.9 -0.47% \u00b7 9,825 obs \u00b7 1987-11-30\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `17:50:37`   X:BTCUSD: {"active": "X:BTCUSD", "meta": "equity \u00b7 77,213.7 -0.25% \u00b7 4,369 obs \u00b7 2014-09-17\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `17:50:37`   page error (AAPL): Value is null | user frames: [] | lwc: ["at f (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.produc", "at Candlestick (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standal", "at Hs (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.produ", "at ne (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.produ", "at <anonymous> (https:
- `17:50:37`   signal feeds loaded: {"cascade": true, "insider": true, "options": true, "live": 1}
## verdict

- `17:50:37` ✗ page errors 16: [{"msg": "Value is null", "sym": "AAPL", "frames": [], "lwc_frames": ["at f (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.produc", "at Candlestick (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standal", "at Hs (https://unpkg.com/lightweight-char
