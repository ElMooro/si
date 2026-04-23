# Why is fred-cache.json not being written?

**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-04-23T15:51:15+00:00  

## Log
## Cache existence check

- `15:51:14`   Cache missing: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
## Most recent complete run — full log

- `15:51:14` 
Stream 1: ...9c8346c1af41a2ab2bde24231ee868 (2.4 min ago)
- `15:51:14`   [V10] Start 2026-04-23T15:48:50.112083
- `15:51:14`   [FRED-CACHE] load err (non-fatal): An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `15:51:14`   [V10] FRED v3.2: skipped 0 via smart TTL ({}), fetching 233
- `15:51:14`   FRED batch 1: total 6 series
- `15:51:14`   FRED batch 6: total 34 series
- `15:51:14` 
Stream 2: ...c3b8d062cf4052b216ec57a4e12c46 (5.0 min ago)
- `15:51:14`   [V10] Start 2026-04-23T15:46:14.555226
- `15:51:14`   [FRED-CACHE] load err (non-fatal): An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `15:51:14`   [V10] FRED v3.2: skipped 0 via smart TTL ({}), fetching 233
- `15:51:14`   FRED batch 1: total 8 series
- `15:51:14`   FRED batch 6: total 42 series
- `15:51:14`   FRED batch 11: total 77 series
- `15:51:14`   FRED batch 16: total 116 series
- `15:51:14`   FRED batch 21: total 142 series
- `15:51:14`   FRED batch 26: total 166 series
- `15:51:14` 
Stream 3: ...98241bc6524cb5bdd91c1ad3310cce (7.4 min ago)
- `15:51:14`   [V10] Start 2026-04-23T15:43:50.082190
- `15:51:14`   [V10] FRED v3.2: skipped 0 via smart TTL ({}), fetching 233
- `15:51:14`   FRED batch 1: total 0 series
- `15:51:14`   FRED batch 6: total 0 series
- `15:51:14`   FRED batch 11: total 0 series
- `15:51:14`   FRED batch 16: total 0 series
- `15:51:14` 
Stream 4: ...4a421813bf4435952d8f9477b47995 (9.2 min ago)
- `15:51:14`   [V10] Start 2026-04-23T15:42:00.598719
- `15:51:14`   [ERROR] NameError: name 'timezone' is not defined
Traceback (most recent call last):
  File "/var/task/lambda_function.py", line 1616, in lambda_handler
    should_skip, reason = _should_skip_fetch(cached)
  File "/var/task/lambda_function.
- `15:51:14`   [V10] Start 2026-04-23T15:43:01.004843
- `15:51:14`   [V10] FRED v3.2: skipped 0 via smart TTL ({}), fetching 233
- `15:51:14`   FRED batch 1: total 0 series
- `15:51:14`   FRED batch 6: total 0 series
- `15:51:14`   FRED batch 11: total 0 series
- `15:51:14`   FRED batch 16: total 0 series
- `15:51:14`   FRED batch 21: total 0 series
- `15:51:14` 
Stream 5: ...67123296d24757b5d6a044463538da (25.2 min ago)
- `15:51:15`   [V10] Start 2026-04-23T15:28:49.353404
- `15:51:15`   [V10] FRED: 0/233 already fresh in cache, fetching 233
- `15:51:15`   FRED batch 1: total 8 series
- `15:51:15`   FRED batch 6: total 47 series
- `15:51:15`   FRED batch 11: total 81 series
- `15:51:15`   FRED batch 16: total 120 series
- `15:51:15`   FRED batch 21: total 147 series
- `15:51:15`   FRED batch 26: total 167 series
- `15:51:15`   [V10] FRED: 23 series from cache backstop
- `15:51:15`   [V10] FRED: 207/233 in 145.0s (skipped 0 fresh, backstop 23)
- `15:51:15`   [V10] Fetching 188 stocks...
- `15:51:15`   [V10] Stocks: 187/188
- `15:51:15`   [V10] Crypto...
- `15:51:15`   [V10] Crypto: 25 coins
- `15:51:15`   [V10] ECB CISS...
- `15:51:15`   ECB CISS error CISS.M.U2.Z0Z.4F.EC.SOV_CI.IDX: HTTP Error 404:
- `15:51:15`   [V10] ECB CISS: 6 series
- `15:51:15`   [V10] Financial News (NewsAPI + RSS)...
- `15:51:15`   NewsAPI error Business: HTTP Error 429: Too Many Requests
- `15:51:15`   NewsAPI error Markets: HTTP Error 429: Too Many Requests
- `15:51:15`   NewsAPI error Fed/Macro: HTTP Error 429: Too Many Requests
- `15:51:15`   NewsAPI error Deals/PE: HTTP Error 429: Too Many Requests
- `15:51:15`   NewsAPI error Crypto: HTTP Error 429: Too Many Requests
- `15:51:15`   NewsAPI error Commodities: HTTP Error 429: Too Many Requests
- `15:51:15`   [V10] NewsAPI: 0 headlines
- `15:51:15`   [V10] News total: 40 headlines
- `15:51:15`   [V10] Computing market flow...
- `15:51:15`   [V10] Flow: 112 buying, 75 selling, 23 sectors up
- `15:51:15`   [V10] ATH tracking...
- `15:51:15`   [V10] ATH: 7 new ATH, 19 near ATH, 188 tracked
- `15:51:15`   [V10] AI Analysis...
- `15:51:15`   [V10] DONE 236.1s: {"status": "published", "ki": 43, "regime": "BEAR", "fred": 207, "stocks": 187, "crypto": 25, "ecb_ciss": 6, "risk_composite": 69, "fetch_time": 236.1, "dxy": 118.0795, "hy_spread": 2.84, "vix": 18.92, "ath_new": 7, "ath_
- `15:51:15`   [V10] Start 2026-04-23T15:33:49.339849
- `15:51:15`   [V10] FRED: 0/233 already fresh in cache, fetching 233
- `15:51:15`   FRED batch 1: total 8 series
- `15:51:15`   FRED batch 6: total 47 series
- `15:51:15`   FRED batch 11: total 85 series
- `15:51:15`   FRED batch 16: total 121 series
- `15:51:15`   FRED batch 21: total 149 series
- `15:51:15`   FRED batch 26: total 176 series
- `15:51:15`   [V10] FRED: 14 series from cache backstop
- `15:51:15`   ... (truncated)
- `15:51:15` Done
