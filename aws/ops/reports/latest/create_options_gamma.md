# Create/update justhodl-options-gamma Lambda + EB rule

**Status:** success  
**Duration:** 5.3s  
**Finished:** 2026-04-27T22:36:09+00:00  

## Log
- `22:36:04`   zip: 4795 bytes
## 1. Lambda

- `22:36:04`   Lambda exists — updating
- `22:36:07` ✅   ✓ updated justhodl-options-gamma
- `22:36:07` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `22:36:08`   rule already correct: justhodl-options-gamma-30min (rate(30 minutes))
- `22:36:08` ✅   ✓ target → justhodl-options-gamma
- `22:36:08` ✅   ✓ added invoke permission
## 3. Smoke test

- `22:36:08`   invoking justhodl-options-gamma…
- `22:36:09` ✅   ✓ smoke test passed
- `22:36:09`     ok                       True
- `22:36:09`     market_closed            True
