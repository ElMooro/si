- `10:08:21`     source: 23468 chars

# 1) Verify v3 markers

- `10:08:21`       ✓ v3.0 starting
- `10:08:21`       ✓ activist_filings_v3_atom_plus_efts
- `10:08:21`       ✓ fetch_efts_search
- `10:08:21`       ✓ parse_efts_hit
- `10:08:21`       ✓ fetch_atom_feed

# 2) Force-deploy

- `10:08:27`     ✓ deployed at 2026-05-06T10:08:23.000+0000

# 3) Smoke invoke (~30-60s — fetches RSS + EFTS)

- `10:08:35`     status: 200, dur: 7.9s
- `10:08:35`     body: {"statusCode": 200, "body": "{\"n_filings\": 1, \"n_classified\": 0, \"n_in_universe\": 0, \"n_new\": 0, \"n_multi_activist\": 0, \"duration_s\": 6.8}"}
- `10:08:35`       [activist] RSS unique filings: 1
- `10:08:35`       [activist] efts SC 13D p0 failed: HTTP Error 500: Internal Server Error
- `10:08:35`       [activist] EFTS SC 13D: 0 hits
- `10:08:35`       [activist] efts SC 13D/A p0 failed: HTTP Error 500: Internal Server Error
- `10:08:35`       [activist] EFTS SC 13D/A: 0 hits
- `10:08:35`       [activist] efts SC 13G p0 failed: HTTP Error 500: Internal Server Error
- `10:08:35`       [activist] EFTS SC 13G: 0 hits
- `10:08:35`       [activist] EFTS SC 13G/A: 200 hits
- `10:08:35`       [activist] EFTS filtered to last 5d: 0
- `10:08:35`       [activist] total: 1, classified: 0, in_universe: 0
- `10:08:35`       [activist] new this run: 0 (TIER-A: 0)
- `10:08:35`       [activist] multi-activist tickers: 0
- `10:08:35`       [activist] wrote 1438b
- `10:08:35`       END RequestId: 7371df9e-60c1-4658-b179-c48087195cc2
- `10:08:35`       REPORT RequestId: 7371df9e-60c1-4658-b179-c48087195cc2	Duration: 6890.23 ms	Billed Duration: 7500 ms	Memory Size: 1024 MB	Max Memory Used: 103 MB	Init Duration: 609.75 ms

# 4) Inspect output

- `10:08:35`     generated_at: 2026-05-06T10:08:35+00:00
- `10:08:35`     stats: {"n_filings_total": 1, "n_classified_by_tier": 0, "n_in_universe": 0, "n_unique_tickers": 1, "n_multi_activist": 0, "n_new_filings": 0, "n_new_tier_a": 0, "n_new_tier_b": 0}
- `10:08:35`   
- `10:08:35`     ── TOP 12 FILINGS BY SCORE ──
- `10:08:35`       GNK     SC 13D    25     NOTABLE                 filer=GENCO SHIPPING &amp; TRADING LTD
- `10:08:35`              subject: GENCO SHIPPING & TRADING LTD  date: 2026-05-04
- `10:08:35`   
- `10:08:35`     ── TIER-A (HOT) classified filings ──
- `10:08:35`   
- `10:08:35`     ── IN-UNIVERSE filings (most actionable) ──
- `10:08:35`   
- `10:08:35`     ── MULTI-ACTIVIST tickers ──

# 5) Schedule daily 12 UTC

- `10:08:36`     ✓ permission added