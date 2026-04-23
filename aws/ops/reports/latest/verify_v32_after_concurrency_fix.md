# After concurrency=1 fix — cache + smart TTL check

**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-04-23T15:59:03+00:00  

## Data

| check | list_shape | series | with_meta |
|---|---|---|---|
| cache | 207 | 207 | 207 |

## Log
## 1. fred-cache.json

- `15:59:02` ✅   Exists: 1,022,825 bytes, 207 series, 1.1 min old
- `15:59:02`   List shape: 207/207   With _meta stamps: 207/207
- `15:59:02`   WALCL: date=2026-04-15 meta=2026-04-23T15:47:29 cadence_gap=7d
- `15:59:02`   UNRATE: date=2026-03-01 meta=2026-04-23T15:48:13 cadence_gap=28d
- `15:59:02`   DGS10: date=2026-04-21 meta=2026-04-23T15:46:20 cadence_gap=1d
- `15:59:02`   CPIAUCSL: date=2026-03-01 meta=2026-04-23T15:54:12 cadence_gap=28d
- `15:59:02`   VIXCLS: date=2026-04-22 meta=2026-04-23T15:47:20 cadence_gap=1d
## 2. Most recent run's log summary

- `15:59:02` 
  Stream 1: ...ae1143a8b6726ff37be78a9d (3.7 min ago)
- `15:59:02`     [V10] Start 2026-04-23T15:55:16.657486
- `15:59:02`     [V10] FRED v3.2: skipped 205 via smart TTL ({'recent': 205}), fetching 28
- `15:59:02`     [V10] FRED: 207/233 in 13.7s (skipped 205 fresh, backstop 0)
- `15:59:02`     [V10] Fetching 188 stocks...
- `15:59:02`     [V10] Stocks: 187/188
- `15:59:02`     [V10] Crypto...
- `15:59:02`     [V10] Crypto: 25 coins
- `15:59:02`     [V10] ECB CISS...
- `15:59:02`     [V10] ECB CISS: 6 series
- `15:59:02`     [V10] Financial News (NewsAPI + RSS)...
- `15:59:02`     [V10] NewsAPI: 0 headlines
- `15:59:02`     [V10] News total: 40 headlines
- `15:59:02`     [V10] Computing market flow...
- `15:59:02`     [V10] Flow: 113 buying, 74 selling, 23 sectors up
- `15:59:02`     [V10] ATH tracking...
- `15:59:02`     [V10] ATH: 1 new ATH, 25 near ATH, 188 tracked
- `15:59:02`     [V10] AI Analysis...
- `15:59:02`     [V10] DONE 124.7s: {"status": "published", "ki": 43, "regime": "BEAR", "fred": 207, "stocks": 187, "crypto": 25, "ecb_ciss": 6, "risk_composite": 69, "fetch_time": 124.7, "dxy": 118.0795, "hy_spread": 2.84, "vix": 18.92,
- `15:59:02`     [V10] Start 2026-04-23T15:57:41.499422
- `15:59:02`     [V10] FRED v3.2: skipped 207 via smart TTL ({'recent': 207}), fetching 26
- `15:59:02`     [V10] FRED: 207/233 in 13.4s (skipped 207 fresh, backstop 0)
- `15:59:02`     [V10] Fetching 188 stocks...
- `15:59:02` 
  Stream 2: ...c1af41a2ab2bde24231ee868 (10.2 min ago)
- `15:59:03`     [V10] Start 2026-04-23T15:48:50.112083
- `15:59:03`     [V10] FRED v3.2: skipped 0 via smart TTL ({}), fetching 233
- `15:59:03`     [V10] FRED: 179/233 in 317.6s (skipped 0 fresh, backstop 0)
- `15:59:03`     [V10] Fetching 188 stocks...
- `15:59:03`     [V10] Stocks: 187/188
- `15:59:03`     [V10] Crypto...
- `15:59:03`     [V10] Crypto: 25 coins
- `15:59:03`     [V10] ECB CISS...
- `15:59:03`     [V10] ECB CISS: 6 series
- `15:59:03`     [V10] Financial News (NewsAPI + RSS)...
- `15:59:03`     [V10] NewsAPI: 0 headlines
- `15:59:03`     [V10] News total: 40 headlines
- `15:59:03`     [V10] Computing market flow...
- `15:59:03`     [V10] Flow: 112 buying, 75 selling, 23 sectors up
- `15:59:03`     [V10] ATH tracking...
- `15:59:03`     [V10] ATH: 2 new ATH, 24 near ATH, 188 tracked
- `15:59:03`     [V10] AI Analysis...
- `15:59:03`     [V10] DONE 429.4s: {"status": "published", "ki": 55, "regime": "NEUTRAL", "fred": 179, "stocks": 187, "crypto": 25, "ecb_ciss": 6, "risk_composite": 69, "fetch_time": 429.4, "dxy": null, "hy_spread": 2.84, "vix": 18.92, 
## 3. Duration + ConcurrentExecutions (since concurrency=1 at 15:54)

- `15:59:03`   Duration (Average):
- `15:59:03`     15:41:00: 262ms
- `15:59:03`     15:43:00: 624989ms
- `15:59:03`     15:46:00: 389804ms
- `15:59:03`     15:48:00: 429386ms
- `15:59:03`     15:53:00: 133296ms
- `15:59:03`     15:55:00: 124664ms
- `15:59:03`   ConcurrentExecutions (Maximum):
- `15:59:03`     15:41:00: 2
- `15:59:03`     15:43:00: 2
- `15:59:03`     15:46:00: 3
- `15:59:03`     15:48:00: 4
- `15:59:03`     15:53:00: 3
- `15:59:03`     15:55:00: 3
- `15:59:03`     15:57:00: 1
- `15:59:03`   Errors (Sum):
- `15:59:03`     15:41:00: 1
- `15:59:03`     15:43:00: 2
- `15:59:03`     15:46:00: 0
- `15:59:03`     15:48:00: 0
- `15:59:03`     15:53:00: 0
- `15:59:03`     15:55:00: 0
- `15:59:03`     15:56:00: 0
- `15:59:03`     15:58:00: 0
- `15:59:03`   Throttles (Sum):
- `15:59:03`     15:41:00: 0
- `15:59:03`     15:43:00: 0
- `15:59:03`     15:46:00: 0
- `15:59:03`     15:48:00: 0
- `15:59:03`     15:53:00: 0
- `15:59:03`     15:55:00: 5
- `15:59:03`     15:56:00: 1
- `15:59:03`     15:58:00: 1
- `15:59:03` Done
