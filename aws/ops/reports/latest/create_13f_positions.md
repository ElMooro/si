# Create/update justhodl-13f-positions Lambda + EB rule

**Status:** success  
**Duration:** 76.5s  
**Finished:** 2026-05-03T17:04:36+00:00  

## Log
- `17:03:20`   zip: 9157 bytes
## 1. Lambda

- `17:03:20`   Lambda exists — updating
- `17:03:25` ✅   ✓ updated justhodl-13f-positions
- `17:03:25` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `17:03:25`   rule already correct: justhodl-13f-positions-6h (rate(6 hours))
- `17:03:25` ✅   ✓ target → justhodl-13f-positions
- `17:03:25` ✅   ✓ added invoke permission
## 3. Smoke test

- `17:03:25`   invoking justhodl-13f-positions…
- `17:04:36` ✗   ✗ invoke: An error occurred (TooManyRequestsException) when calling the Invoke operation (reached max retries: 4): Rate Exceeded.
