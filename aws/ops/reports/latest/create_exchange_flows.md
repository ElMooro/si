# Create/update justhodl-exchange-flows Lambda + EB rule

**Status:** success  
**Duration:** 7.4s  
**Finished:** 2026-04-27T22:10:17+00:00  

## Log
- `22:10:10`   zip: 3571 bytes
## 1. Lambda

- `22:10:10`   Lambda missing — creating
- `22:10:15` ✅   ✓ created justhodl-exchange-flows
- `22:10:15` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `22:10:16` ✅   ✓ created rule justhodl-exchange-flows-6h
- `22:10:16` ✅   ✓ target → justhodl-exchange-flows
- `22:10:16` ✅   ✓ added invoke permission
## 3. Smoke test

- `22:10:16`   invoking justhodl-exchange-flows…
- `22:10:17` ✅   ✓ smoke test passed
- `22:10:17`     ok                       True
