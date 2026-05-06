- `09:28:41`     source: 18904 chars
- `09:28:41`       ✓ ACTIVIST_TIERS
- `09:28:41`       ✓ TIER_S_LEGENDARY
- `09:28:41`       ✓ icahn
- `09:28:41`       ✓ berkshire hathaway
- `09:28:41`       ✓ starboard
- `09:28:41`       ❌ import urllib.parse
- `09:28:41`       ✓ fetch_atom_feed
- `09:28:41`       ✓ cik_to_ticker_map

# 1) Build zip + create Lambda

- `09:28:41`     zip: 19362b
- `09:28:41`     creating new
- `09:28:45`     ✓ deployed at 2026-05-06T09:28:41.558+0000, mem=512MB to=300s

# 2) Schedule daily 12:00 UTC

- `09:28:45`     ✓ permission added

# 3) Smoke invoke

- `09:28:47`     status: 200, dur: 1.8s
- `09:28:47`     body: {"statusCode": 200, "body": "{\"n_filings\": 1, \"n_new\": 1, \"n_new_tier_a\": 0, \"n_multi_activist\": 0, \"duration_s\": 1.1}"}
- `09:28:47`       [activist] fetching SC 13D/A...
- `09:28:47`       [activist]   got 1 entries
- `09:28:47`       [activist] fetching SC 13G...
- `09:28:47`       [activist]   got 0 entries
- `09:28:47`       [activist] fetching SC 13G/A...
- `09:28:47`       [activist]   got 0 entries
- `09:28:47`       [activist] 1 unique filings
- `09:28:47`       [activist] total filings: 1
- `09:28:47`       [activist] new filings this run: 1
- `09:28:47`       [activist] new TIER-A: 0
- `09:28:47`       [activist] new TIER-B: 0
- `09:28:47`       [activist] multi-activist tickers: 0
- `09:28:47`       [activist] wrote 1466b
- `09:28:47`       END RequestId: edd8174a-1db0-4e73-bde2-1139b07de8f3
- `09:28:47`       REPORT RequestId: edd8174a-1db0-4e73-bde2-1139b07de8f3	Duration: 1133.02 ms	Billed Duration: 1633 ms	Memory Size: 512 MB	Max Memory Used: 102 MB	Init Duration: 499.89 ms

# 4) Inspect output

- `09:28:47`     generated_at: 2026-05-06T09:28:47+00:00
- `09:28:47`     stats: {"n_filings_total": 1, "n_unique_tickers": 0, "n_multi_activist": 0, "n_new_filings": 1, "n_new_tier_a": 0, "n_new_tier_b": 0, "n_in_universe": 0}
- `09:28:47`   
- `09:28:47`     ── TOP 15 RECENT FILINGS ──
- `09:28:47`       ?      SC 13D      SC 13D/A - GENCO SHIPPING &amp;   filer_tier=                    score= 25