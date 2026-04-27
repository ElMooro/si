# Create/update justhodl-exchange-flows Lambda + EB rule

**Status:** success  
**Duration:** 9.0s  
**Finished:** 2026-04-27T22:36:03+00:00  

## Log
- `22:35:54`   zip: 3940 bytes
## 1. Lambda

- `22:35:55`   Lambda exists — updating
- `22:36:01` ✅   ✓ updated justhodl-exchange-flows
- `22:36:01` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `22:36:01`   rule already correct: justhodl-exchange-flows-6h (rate(6 hours))
- `22:36:01` ✅   ✓ target → justhodl-exchange-flows
- `22:36:02` ✅   ✓ added invoke permission
## 3. Smoke test

- `22:36:02`   invoking justhodl-exchange-flows…
- `22:36:03` ✅   ✓ smoke test passed
- `22:36:03`     ok                       True
- `22:36:03`     btc_regime               neutral
- `22:36:03`     eth_regime               neutral
