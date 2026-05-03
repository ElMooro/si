# Create/update justhodl-13f-positions Lambda + EB rule

**Status:** success  
**Duration:** 52.1s  
**Finished:** 2026-05-03T16:51:49+00:00  

## Log
- `16:50:57`   zip: 8831 bytes
## 1. Lambda

- `16:50:57`   Lambda missing — creating
- `16:51:02` ✅   ✓ created justhodl-13f-positions
- `16:51:02` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `16:51:02` ✅   ✓ created rule justhodl-13f-positions-6h
- `16:51:02` ✅   ✓ target → justhodl-13f-positions
- `16:51:03` ✅   ✓ added invoke permission
## 3. Smoke test

- `16:51:03`   invoking justhodl-13f-positions…
- `16:51:49` ✅   ✓ smoke test passed
- `16:51:49`     ok                       True
- `16:51:49`     funds_parsed             3
- `16:51:49`     funds_failed             14
- `16:51:49`     tickers_aggregated       2410
