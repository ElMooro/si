- `10:35:11`     source: 23748 chars

# 1) Verify markers

- `10:35:11`       ✓ count=100
- `10:35:11`       ✓ EFTS full-text search abandoned
- `10:35:11`       ✓ <link>([^<]+)</link>

# 2) Force-deploy

- `10:35:15`     ✓ deployed at 2026-05-06T10:35:12.000+0000

# 3) Smoke invoke

- `10:35:17`     status: 200, dur: 2.0s
- `10:35:17`     body: {"statusCode": 200, "body": "{\"n_filings\": 1, \"n_classified\": 0, \"n_in_universe\": 0, \"n_new\": 0, \"n_multi_activist\": 0, \"duration_s\": 1.1}"}
- `10:35:17`       [activist] universe: 338 tickers
- `10:35:17`       [activist] RSS SC 13D: 1 entries
- `10:35:17`       [activist] RSS SC 13D/A: 1 entries
- `10:35:17`       [activist] RSS SC 13G: 0 entries
- `10:35:17`       [activist] RSS SC 13G/A: 0 entries
- `10:35:17`       [activist] RSS unique filings: 1
- `10:35:17`       [activist] total: 1, classified: 0, in_universe: 0
- `10:35:17`       [activist] new this run: 0 (TIER-A: 0)
- `10:35:17`       [activist] multi-activist tickers: 0
- `10:35:17`       [activist] wrote 1438b
- `10:35:17`       END RequestId: c1a1791c-7404-4b2c-b384-f0339d116e91
- `10:35:17`       REPORT RequestId: c1a1791c-7404-4b2c-b384-f0339d116e91	Duration: 1188.97 ms	Billed Duration: 1800 ms	Memory Size: 1024 MB	Max Memory Used: 103 MB	Init Duration: 610.32 ms

# 4) Inspect output

- `10:35:17`     generated_at: 2026-05-06T10:35:17+00:00
- `10:35:17`     stats: {"n_filings_total": 1, "n_classified_by_tier": 0, "n_in_universe": 0, "n_unique_tickers": 1, "n_multi_activist": 0, "n_new_filings": 0, "n_new_tier_a": 0, "n_new_tier_b": 0}
- `10:35:17`   
- `10:35:17`     ── ALL filings detected ──
- `10:35:17`       GNK     SC 13D      score=25   NOTABLE                 link=(no link)  filer=GENCO SHIPPING &amp; TRADING LTD
- `10:35:17`   
- `10:35:17`     ── In universe (most actionable) ──
- `10:35:17`       (none today)
- `10:35:17`   
- `10:35:17`     Note: this Lambda runs daily 12 UTC; coverage will increase substantially
- `10:35:17`     during US business hours when most 13D/G filings happen.