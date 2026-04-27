# Create/update justhodl-liquidity-flow Lambda + EB rule

**Status:** success  
**Duration:** 8.1s  
**Finished:** 2026-04-27T22:10:26+00:00  

## Log
- `22:10:18`   zip: 3714 bytes
## 1. Lambda

- `22:10:18`   Lambda missing — creating
- `22:10:21` ✅   ✓ created justhodl-liquidity-flow
- `22:10:21` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `22:10:21` ✅   ✓ created rule justhodl-liquidity-flow-daily
- `22:10:21` ✅   ✓ target → justhodl-liquidity-flow
- `22:10:21` ✅   ✓ added invoke permission
## 3. Smoke test

- `22:10:21`   invoking justhodl-liquidity-flow…
- `22:10:26` ✅   ✓ smoke test passed
- `22:10:26`     ok                       True
- `22:10:26`     regime                   draining
- `22:10:26`     net_liquidity_b          -999260.9
