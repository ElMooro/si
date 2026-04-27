# Create/update justhodl-redflag-alerter Lambda + EB rule

**Status:** success  
**Duration:** 6.3s  
**Finished:** 2026-04-27T21:56:33+00:00  

## Log
- `21:56:27`   zip: 3301 bytes
## 1. Lambda

- `21:56:27`   Lambda missing — creating
- `21:56:30` ✅   ✓ created justhodl-redflag-alerter
- `21:56:30` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `21:56:30` ✅   ✓ created rule justhodl-redflag-alerter-30min
- `21:56:30` ✅   ✓ target → justhodl-redflag-alerter
- `21:56:31` ✅   ✓ added invoke permission
## 3. Smoke test

- `21:56:31`   invoking justhodl-redflag-alerter…
- `21:56:33` ✅   ✓ smoke test passed
- `21:56:33`     ok                       True
- `21:56:33`     alerts_sent              2
- `21:56:33`     alerts_skipped           0
