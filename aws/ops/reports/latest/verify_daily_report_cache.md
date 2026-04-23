# Verify daily-report-v3 FRED cache patch is working

**Status:** success  
**Duration:** 2.4s  
**Finished:** 2026-04-23T15:36:25+00:00  

## Data

| age_min | check | fred_nulls | fred_with_values | series | size | walcl |
|---|---|---|---|---|---|---|
| 0.1 | fred-cache |  |  | 207 | 1010198 |  |
| 3.6 | report | 28 | 205 |  |  | None |

## Log
## 1. fred-cache.json (shared cache)

- `15:36:24`   Size: 1010198 bytes
- `15:36:24`   LastModified: 2026-04-23T15:36:16+00:00 (age: 0.1 min)
- `15:36:24`   Series count: 207
- `15:36:24`   Sample series:
- `15:36:24`     WALCL: 6705696.0 on 2026-04-15
- `15:36:24`     RRPONTSYD: 0.538 on 2026-04-22
- `15:36:24`     WTREGEN: 751354.0 on 2026-04-15
- `15:36:24`     VIXCLS: 18.92 on 2026-04-22
- `15:36:24`     NAPM: missing
- `15:36:24`     CPIAUCSL: 330.293 on 2026-03-01
## 2. data/report.json freshness

- `15:36:24`   Size: 1708824 bytes
- `15:36:24`   LastModified: 2026-04-23T15:32:46+00:00 (age: 3.6 min)
- `15:36:24`   version: V10
- `15:36:24`   generated_at: 2026-04-23T15:32:35.223325Z
- `15:36:24`   FRED series with values: 205
- `15:36:24`   FRED series with nulls: 28
- `15:36:24`   WALCL current: None
- `15:36:24`   RRPONTSYD current: None
- `15:36:24`   WTREGEN current: None
- `15:36:24`   Net liquidity: {"net": 5953804, "fed": 6705696, "tga": 751354, "rrp": 1}
## 3. Recent daily-report-v3 log groups (last 10 min)

- `15:36:24`   Stream: $LATEST]e967123296d24757b5d6a044463538da last event 10.4 min ago
- `15:36:25`     [V10] ECB CISS...
- `15:36:25`     [V10] ECB CISS: 6 series
- `15:36:25`     [V10] Financial News (NewsAPI + RSS)...
- `15:36:25`     NewsAPI error Business: HTTP Error 429: Too Many Requests
- `15:36:25`     NewsAPI error Markets: HTTP Error 429: Too Many Requests
- `15:36:25`     NewsAPI error Fed/Macro: HTTP Error 429: Too Many Requests
- `15:36:25`     NewsAPI error Deals/PE: HTTP Error 429: Too Many Requests
- `15:36:25`     NewsAPI error Crypto: HTTP Error 429: Too Many Requests
- `15:36:25`     NewsAPI error Commodities: HTTP Error 429: Too Many Requests
- `15:36:25`     [V10] NewsAPI: 0 headlines
- `15:36:25`     [V10] News total: 40 headlines
- `15:36:25`     [V10] Computing market flow...
- `15:36:25`     [V10] Flow: 112 buying, 75 selling, 23 sectors up
- `15:36:25`     [V10] ATH tracking...
- `15:36:25`     [V10] ATH: 7 new ATH, 19 near ATH, 188 tracked
- `15:36:25`     [V10] AI Analysis...
- `15:36:25`     [V10] DONE 236.1s: {"status": "published", "ki": 43, "regime": "BEAR", "fred": 207, "stocks": 187, "crypto": 25, "ecb_ciss": 6, "risk_composite": 69, "fetch_time": 236.1, "dxy": 118.0795, "hy_spread":
- `15:36:25`     [V10] Start 2026-04-23T15:33:49.339849
- `15:36:25`     [V10] FRED: 0/233 already fresh in cache, fetching 233
- `15:36:25`     FRED batch 1: total 8 series
- `15:36:25`     FRED batch 6: total 47 series
- `15:36:25`     FRED batch 11: total 85 series
- `15:36:25`     FRED batch 16: total 121 series
- `15:36:25`     FRED batch 21: total 149 series
- `15:36:25`     FRED batch 26: total 176 series
- `15:36:25`     [V10] FRED: 14 series from cache backstop
- `15:36:25`     [V10] FRED: 207/233 in 145.6s (skipped 0 fresh, backstop 14)
- `15:36:25`     [V10] Fetching 188 stocks...
## 4. daily-report-v3 metrics (last 15 min)

- `15:36:25`   Errors 2026-04-23T15:21:00+00:00: 0.0
- `15:36:25`   Errors 2026-04-23T15:26:00+00:00: 0.0
- `15:36:25`   Invocations 2026-04-23T15:21:00+00:00: 1.0
- `15:36:25`   Invocations 2026-04-23T15:26:00+00:00: 1.0
- `15:36:25`   Duration 2026-04-23T15:21:00+00:00: 232983.52ms
- `15:36:25`   Duration 2026-04-23T15:26:00+00:00: 236104.24ms
- `15:36:25` Done
