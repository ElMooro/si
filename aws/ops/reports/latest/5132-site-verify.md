# ops 5132 -- site verification after the pages.yml repair

**Status:** failure  
**Duration:** 198.4s  
**Finished:** 2026-09-02T15:44:13+00:00  

## Error

```
SystemExit: 1
```

## Log
## S1 static content live?

- `15:40:57`   live: {"data": false, "plumbing": false, "chart": false}
- `15:41:28`   live: {"data": false, "plumbing": false, "chart": false}
- `15:41:59`   live: {"data": false, "plumbing": false, "chart": false}
- `15:42:30`   live: {"data": true, "plumbing": true, "chart": true}
- `15:42:30` ✅   data.html static inventory: 57 providers, as_of 2026-09-02T14:48:53+00:00, totals {'providers': 57, 'datasets': 811171, 'keys': 1921752, 'gb': 530.39}
## S2 chart-pro native charts (headless Chrome)

- `15:43:27`   AAPL: {"active": "AAPL", "meta": "$325.03 +0.16% \u00b7 250 bars \u00b7 JustHodl warehouse \u00b7 since 1980", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1980"}
- `15:43:36`   NVDA: {"active": "NVDA", "meta": "$223.54 -1.32% \u00b7 250 bars \u00b7 JustHodl warehouse \u00b7 since 1999", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `15:43:45`   SSE:000001: {"active": "SSE:000001", "meta": "TradingView \u00b7 3,941.4 -0.97% \u00b7 7,186 obs \u00b7 1997-07-02\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `15:43:54`   TVC:VIX: {"active": "TVC:VIX", "meta": "FRED \u00b7 14.43 Index -0.55% \u00b7 9,262 obs \u00b7 1990-01-02\u21922026-08-28 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `15:44:03`   XETR:DAX: {"active": "XETR:DAX", "meta": "TradingView \u00b7 25,846.9 -0.47% \u00b7 9,825 obs \u00b7 1987-11-30\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `15:44:13`   X:BTCUSD: {"active": "X:BTCUSD", "meta": "equity \u00b7 77,213.7 -0.25% \u00b7 4,369 obs \u00b7 2014-09-17\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `15:44:13`   page errors: ['Value is null', 'Value is null', 'Value is null']
## verdict

- `15:44:13` ✗ AAPL: only 250 bars -- not daily history
- `15:44:13` ✗ NVDA: only 250 bars -- not daily history
- `15:44:13` ✗ page errors ['Value is null', 'Value is null']
