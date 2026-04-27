# Create/update justhodl-price-redundancy Lambda + EB rule

**Status:** success  
**Duration:** 6.8s  
**Finished:** 2026-04-27T18:44:14+00:00  

## Log
- `18:44:07`   zip: 3380 bytes
## 1. Lambda

- `18:44:08`   Lambda missing — creating
- `18:44:12` ✅   ✓ created justhodl-price-redundancy
- `18:44:12` ✅   ✓ reserved concurrency = 1
- `18:44:13` ✅   ✓ Function URL: https://knljbfum6vevjilh3tdrnagzta0sbwhi.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `18:44:13` ✅   ✓ created rule justhodl-price-redundancy-15min
- `18:44:13` ✅   ✓ target → justhodl-price-redundancy
- `18:44:13` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:44:13`   invoking justhodl-price-redundancy…
- `18:44:14` ✅   ✓ smoke test passed
- `18:44:14`     tickers_total            0
- `18:44:14`     tickers_ok               0
- `18:44:14`     tickers_failed           0
- `18:44:14`     stooq_success_rate       0
- `18:44:14`     yahoo_success_rate       0
- `18:44:14`     fetch_duration_s         0.0
