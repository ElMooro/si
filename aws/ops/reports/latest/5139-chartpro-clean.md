# ops 5139 -- chart-pro: zero page errors on native renders

**Status:** success  
**Duration:** 279.0s  
**Finished:** 2026-09-02T18:08:04+00:00  

## Log
## S1 static content live?

- `18:03:27`   live: {"data": true, "plumbing": true, "chart": false}
- `18:03:59`   live: {"data": true, "plumbing": true, "chart": false}
- `18:04:31`   live: {"data": true, "plumbing": true, "chart": false}
- `18:05:02`   live: {"data": true, "plumbing": true, "chart": false}
- `18:05:34`   live: {"data": true, "plumbing": true, "chart": false}
- `18:06:05`   live: {"data": true, "plumbing": true, "chart": false}
- `18:06:36`   live: {"data": true, "plumbing": true, "chart": true}
- `18:06:36` ✅   data.html static inventory: 57 providers, as_of 2026-09-02T17:48:53+00:00, totals {'providers': 57, 'datasets': 811797, 'keys': 1922378, 'gb': 530.42}
## S2 chart-pro native charts (headless Chrome)

- `18:07:10`   errors after initial load (NVDA): []
- `18:07:19`   AAPL: {"active": "AAPL", "meta": "$325.03 -0.03% \u00b7 250 bars \u00b7 JustHodl warehouse \u00b7 since 1980", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1980"}
- `18:07:28`   NVDA: {"active": "NVDA", "meta": "$223.54 +2.81% \u00b7 250 bars \u00b7 JustHodl warehouse \u00b7 since 1999", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `18:07:37`   SSE:000001: {"active": "SSE:000001", "meta": "TradingView \u00b7 3,941.4 -0.97% \u00b7 7,186 obs \u00b7 1997-07-02\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `18:07:46`   TVC:VIX: {"active": "TVC:VIX", "meta": "FRED \u00b7 14.43 Index -0.55% \u00b7 9,262 obs \u00b7 1990-01-02\u21922026-08-28 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `18:07:55`   XETR:DAX: {"active": "XETR:DAX", "meta": "TradingView \u00b7 25,846.9 -0.47% \u00b7 9,825 obs \u00b7 1987-11-30\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `18:08:04`   X:BTCUSD: {"active": "X:BTCUSD", "meta": "equity \u00b7 77,213.7 -0.25% \u00b7 4,369 obs \u00b7 2014-09-17\u21922026-09-02 \u00b7 D \u00b7 warehouse", "iframe": false, "paywall": false, "nativeSrc": "JustHodl warehouse \u00b7 since 1999"}
- `18:08:04`   signal feeds loaded: {"cascade": true, "insider": true, "options": true, "live": 1}
## verdict

- `18:08:04` ✅ PASS_ALL: data.html inventory static, plumbing weights corrected, chart-pro charts every symbol from the warehouse with daily history
