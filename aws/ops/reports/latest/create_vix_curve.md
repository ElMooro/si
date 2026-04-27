# Create/update justhodl-vix-curve Lambda + EB rule

**Status:** success  
**Duration:** 7.0s  
**Finished:** 2026-04-27T22:10:33+00:00  

## Log
- `22:10:26`   zip: 3480 bytes
## 1. Lambda

- `22:10:26`   Lambda missing — creating
- `22:10:29` ✅   ✓ created justhodl-vix-curve
- `22:10:29` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `22:10:29` ✅   ✓ created rule justhodl-vix-curve-4h
- `22:10:30` ✅   ✓ target → justhodl-vix-curve
- `22:10:30` ✅   ✓ added invoke permission
## 3. Smoke test

- `22:10:30`   invoking justhodl-vix-curve…
- `22:10:33` ✅   ✓ smoke test passed
- `22:10:33`     ok                       True
- `22:10:33`     regime                   steep_contango
- `22:10:33`     vix_30d                  18.02
- `22:10:33`     vix_3m                   20.77
