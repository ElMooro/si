# Create/update justhodl-liquidity-flow Lambda + EB rule

**Status:** success  
**Duration:** 7.0s  
**Finished:** 2026-04-27T22:12:37+00:00  

## Log
- `22:12:30`   zip: 4052 bytes
## 1. Lambda

- `22:12:30`   Lambda exists — updating
- `22:12:33` ✅   ✓ updated justhodl-liquidity-flow
- `22:12:33` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `22:12:33`   rule already correct: justhodl-liquidity-flow-daily (rate(1 day))
- `22:12:33` ✅   ✓ target → justhodl-liquidity-flow
- `22:12:34` ✅   ✓ added invoke permission
## 3. Smoke test

- `22:12:34`   invoking justhodl-liquidity-flow…
- `22:12:37` ✅   ✓ smoke test passed
- `22:12:37`     ok                       True
- `22:12:37`     regime                   draining
- `22:12:37`     net_liquidity_b          5701.1
