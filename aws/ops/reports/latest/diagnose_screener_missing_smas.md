# Diagnose: why are 70% of stocks missing SMA data?

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-04-25T23:09:06+00:00  

## Data

| n_errors | n_events | n_fails | n_histeod_errors |
|---|---|---|---|
| 1731 | 1746 | 0 | 352 |

## Log
## A. Find most recent log stream

- `23:09:05`   2026/04/25/[$LATEST]38fa1efed8ce4b4e824cd09b8cf303a1         last=2026-04-25 23:05:13
- `23:09:05`   2026/04/25/[$LATEST]62220b4aded044a88f0709df98646047         last=2026-04-25 22:51:30
- `23:09:05`   2026/04/25/[$LATEST]39e3ed8b1d4a48b68edaf5883a113855         last=2026-04-25 19:29:24
- `23:09:05` 
  Using: 2026/04/25/[$LATEST]38fa1efed8ce4b4e824cd09b8cf303a1
## B. Pull events

- `23:09:06`   Pulled 1746 log events across 2 pages
## C. Error patterns

- `23:09:06`   Total ERR lines: 1731
- `23:09:06` 
- `23:09:06`   By endpoint:
- `23:09:06`      352× historical-price-eod/full
- `23:09:06`      349× ratios-ttm
- `23:09:06`      347× profile
- `23:09:06`      342× financial-growth
- `23:09:06`      341× key-metrics-ttm
## D. historical-price-eod/full error reasons (this is the key one)

- `23:09:06`      352× HTTP 429 (rate limit)
- `23:09:06` 
  Sample raw error messages:
- `23:09:06`     HTTP Error 429: Too Many Requests
- `23:09:06`     HTTP Error 429: Too Many Requests
- `23:09:06`     HTTP Error 429: Too Many Requests
- `23:09:06`     HTTP Error 429: Too Many Requests
- `23:09:06`     HTTP Error 429: Too Many Requests
- `23:09:06`     HTTP Error 429: Too Many Requests
## E. Per-stock FAIL events (from process() catch-all)

- `23:09:06`   Total FAIL events: 0
## F. Diagnosis

- `23:09:06`   352 historical-price-eod/full errors out of 503 stocks (70.0%)
- `23:09:06` ⚠   → ~70% mismatch — confirms rate-limit OR endpoint issue
- `23:09:06` Done
