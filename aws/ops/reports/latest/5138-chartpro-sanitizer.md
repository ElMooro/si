# ops 5138 -- chart-pro: sanitized candles, disposed panes, deep stacks

**Status:** failure  
**Duration:** 290.4s  
**Finished:** 2026-09-02T17:59:47+00:00  

## Error

```
SystemExit: 1
```

## Log
## S1 static content live?

- `17:54:58`   live: {"data": true, "plumbing": true, "chart": false}
- `17:55:28`   live: {"data": true, "plumbing": true, "chart": false}
- `17:55:59`   live: {"data": true, "plumbing": true, "chart": false}
- `17:56:30`   live: {"data": true, "plumbing": true, "chart": false}
- `17:57:01`   live: {"data": true, "plumbing": true, "chart": false}
- `17:57:32`   live: {"data": true, "plumbing": true, "chart": false}
- `17:58:02`   live: {"data": true, "plumbing": true, "chart": false}
- `17:58:33`   live: {"data": true, "plumbing": true, "chart": true}
- `17:58:33` ✅   data.html static inventory: 57 providers, as_of 2026-09-02T17:48:53+00:00, totals {'providers': 57, 'datasets': 811797, 'keys': 1922378, 'gb': 530.42}
## S2 chart-pro native charts (headless Chrome)

- `17:58:53`   errors after initial load (NVDA): []
- `17:59:02`   AAPL: {"active": "AAPL", "meta": "$325.03 -0.03% \u00b7 250 bars \u00b7 JustHodl warehouse \u00b7 since 1980", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1980"}
- `17:59:11`   NVDA: {"active": "NVDA", "meta": "$223.54 +2.81% \u00b7 250 bars \u00b7 JustHodl warehouse \u00b7 since 1999", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `17:59:20`   SSE:000001: {"active": "SSE:000001", "meta": "TradingView \u00b7 3,941.4 -0.97% \u00b7 7,186 obs \u00b7 1997-07-02\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `17:59:29`   TVC:VIX: {"active": "TVC:VIX", "meta": "FRED \u00b7 14.43 Index -0.55% \u00b7 9,262 obs \u00b7 1990-01-02\u21922026-08-28 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `17:59:38`   XETR:DAX: {"active": "XETR:DAX", "meta": "TradingView \u00b7 25,846.9 -0.47% \u00b7 9,825 obs \u00b7 1987-11-30\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `17:59:47`   X:BTCUSD: {"active": "X:BTCUSD", "meta": "equity \u00b7 77,213.7 -0.25% \u00b7 4,369 obs \u00b7 2014-09-17\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `17:59:47`   page error (NVDA): Object is disposed | user frames: [] | lwc: ["at get (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.prod", "at get (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.prod", "at es (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.produ", "at fp (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.produ", "at fp (https://unpkg.c
- `17:59:47`   signal feeds loaded: {"cascade": true, "insider": true, "options": true, "live": 1}
## verdict

- `17:59:47` ✗ page errors 1: [{"msg": "Object is disposed", "sym": "NVDA", "frames": [], "lwc_frames": ["at get (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.prod", "at get (https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.prod", "at es (https://unpkg.com/lightweight
