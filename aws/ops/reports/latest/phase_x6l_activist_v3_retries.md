- `10:20:02`     source: 24415 chars

# 1) Verify retry markers

- `10:20:02`       ✓ max_retries=3
- `10:20:02`       ✓ 500 retry
- `10:20:02`       ✓ max_retries - 1

# 2) Force-deploy + ensure env DAYS_BACK=30

- `10:20:06`     ✓ deployed at 2026-05-06T10:20:04.000+0000
- `10:20:06`     env DAYS_BACK=30

# 3) Smoke invoke (with retries should now succeed)

- `10:20:19`     status: 200, dur: 13.7s
- `10:20:19`     body: {"statusCode": 200, "body": "{\"n_filings\": 1, \"n_classified\": 0, \"n_in_universe\": 0, \"n_new\": 0, \"n_multi_activist\": 0, \"duration_s\": 12.6}"}
- `10:20:19`       [activist] RSS SC 13D/A: 1 entries
- `10:20:19`       [activist] RSS SC 13G: 0 entries
- `10:20:19`       [activist] RSS SC 13G/A: 0 entries
- `10:20:19`       [activist] RSS unique filings: 1
- `10:20:19`       [activist] EFTS SC 13D: 200 hits
- `10:20:19`       [activist] EFTS SC 13D/A: 200 hits
- `10:20:19`       [activist] EFTS SC 13G: 200 hits
- `10:20:19`       [activist] EFTS SC 13G/A: 200 hits
- `10:20:19`       [activist] EFTS filtered to last 30d: 0
- `10:20:19`       [activist] total: 1, classified: 0, in_universe: 0
- `10:20:19`       [activist] new this run: 0 (TIER-A: 0)
- `10:20:19`       [activist] multi-activist tickers: 0
- `10:20:19`       [activist] wrote 1439b
- `10:20:19`       END RequestId: ebb10974-138b-463d-81ba-c0ffab209a73
- `10:20:19`       REPORT RequestId: ebb10974-138b-463d-81ba-c0ffab209a73	Duration: 12683.33 ms	Billed Duration: 13277 ms	Memory Size: 1024 MB	Max Memory Used: 103 MB	Init Duration: 593.23 ms

# 4) Inspect output

- `10:20:20`     generated_at: 2026-05-06T10:20:19+00:00
- `10:20:20`     stats: {"n_filings_total": 1, "n_classified_by_tier": 0, "n_in_universe": 0, "n_unique_tickers": 1, "n_multi_activist": 0, "n_new_filings": 0, "n_new_tier_a": 0, "n_new_tier_b": 0}
- `10:20:20`   
- `10:20:20`     ── TOP 15 BY SCORE ──
- `10:20:20`       GNK     SC 13D      score=25   NOTABLE                 GENCO SHIPPING &amp; TRADING LTD
- `10:20:20`           subject: GENCO SHIPPING & TRADING LTD (GNK) date: 2026-05-04
- `10:20:20`   
- `10:20:20`     ── TIER-A activist filings ──
- `10:20:20`       (none today)
- `10:20:20`   
- `10:20:20`     ── IN-UNIVERSE filings (most actionable) ──
- `10:20:20`       (none today)
- `10:20:20`   
- `10:20:20`     ── Multi-activist setups ──
- `10:20:20`       (none today)