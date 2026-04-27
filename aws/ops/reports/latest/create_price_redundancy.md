# Create/update justhodl-price-redundancy Lambda + EB rule

**Status:** success  
**Duration:** 9.4s  
**Finished:** 2026-04-27T18:49:14+00:00  

## Log
- `18:49:04`   zip: 3413 bytes
## 1. Lambda

- `18:49:04`   Lambda exists — updating
- `18:49:09` ✅   ✓ updated justhodl-price-redundancy
- `18:49:10` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `18:49:10`   rule already correct: justhodl-price-redundancy-15min (rate(15 minutes))
- `18:49:10` ✅   ✓ target → justhodl-price-redundancy
- `18:49:10` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:49:10`   invoking justhodl-price-redundancy…
- `18:49:14` ✅   ✓ smoke test passed
- `18:49:14`     tickers_total            23
- `18:49:14`     tickers_ok               22
- `18:49:14`     tickers_failed           1
- `18:49:14`     stooq_success_rate       0.0
- `18:49:14`     yahoo_success_rate       0.957
- `18:49:14`     fetch_duration_s         2.0
