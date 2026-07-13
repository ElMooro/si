# ops 3223 — traced run, env confirmed first

**Status:** success  
**Duration:** 129.5s  
**Finished:** 2026-07-13T06:06:55+00:00  

## Data

| active_now | evidence_lines | n_fails | n_warns | verdict | wl_trace_env |
|---|---|---|---|---|---|
|  |  |  |  |  | set |
|  | 46 |  |  |  |  |
| 117 |  |  |  |  |  |
|  |  | 0 | 0 | PASS |  |

## Log
- `06:06:55`   [trace] stamp=2026-07-13T05:58:55 fresh_cut=2026-07-07T06:04:47 todo=1346
- `06:06:55`   [trace] TVC:ES10Y-TVC:IT10Y: need=True cache_pre=False todo=True
- `06:06:55`   [trace] ECONOMICS:GBDIR: need=True cache_pre=True todo=False
- `06:06:55`   [trace] ECONOMICS:EUDIR: need=True cache_pre=False todo=True
- `06:06:55`   [trace] TVC:DE10Y-TVC:IT10Y: need=True cache_pre=True todo=False
- `06:06:55`   [trace] TVC:FR10Y-TVC:IT10Y: need=True cache_pre=True todo=False
- `06:06:55`   [series_source] COINMETRICS:eth|FeeTotUSD failed: HTTP Error 403: Forbidden
- `06:06:55`   [series_source] COINGECKO:total2 failed: HTTP Error 404: Not Found
- `06:06:55`   [series_source] COINMETRICS:btc|FeeTotUSD failed: HTTP Error 403: Forbidden
- `06:06:55`   [series_source] COINGECKO:c failed: HTTP Error 401: Unauthorized
- `06:06:55`   [series_source] COINGECKO:btcshorts failed: HTTP Error 404: Not Found
- `06:06:55`   [series_source] COINGECKO:eth.d failed: HTTP Error 404: Not Found
- `06:06:55`   [series_source] COINGECKO:doge.d failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:others.d failed: HTTP Error 404: Not Found
- `06:06:55`   [series_source] COINGECKO:total3es failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:total3 failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:totales failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:total2es failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:totale50 failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:totale100 failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:total3esbtc failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:totale100.d failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:totaldefi.d failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:totale50.d failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:totales.d failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:totaldefi failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:others failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:bitcoin failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:.d failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:c.d failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:btc failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINMETRICS:matic|SplyCur failed: HTTP Error 400: Bad Request
- `06:06:55`   [series_source] COINGECKO:matic failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:crv failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:bitcoin failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINMETRICS:matic|AdrActCnt failed: HTTP Error 400: Bad Request
- `06:06:55`   [series_source] COINGECKO:shib.d failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:crv.d failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:stable.c.d failed: HTTP Error 429: Too Many Requests
- `06:06:55`   [series_source] COINGECKO:bitcoin failed: HTTP Error 401: Unauthorized
- `06:06:55`   [series_source] COINGECKO:ethereum failed: HTTP Error 401: Unauthorized
- `06:06:55`   [trace] TVC:ES10Y-TVC:IT10Y: weekly=0 zc=False
- `06:06:55`   [trace] ECONOMICS:GBDIR: weekly=9 zc=False
- `06:06:55`   [trace] ECONOMICS:EUDIR: weekly=0 zc=False
- `06:06:55`   [trace] TVC:DE10Y-TVC:IT10Y: weekly=422 zc=True
- `06:06:55`   [trace] TVC:FR10Y-TVC:IT10Y: weekly=422 zc=True
- `06:06:55`   Europe Liquidity :BTPBUND  measure fin DORMANT resolved=5
- `06:06:55`   Global Deposit Rates Which drains liqu DORMANT resolved=4
