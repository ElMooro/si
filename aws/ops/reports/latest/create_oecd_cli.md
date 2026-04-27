# Create/update justhodl-oecd-cli Lambda + EB rule

**Status:** success  
**Duration:** 12.1s  
**Finished:** 2026-04-27T18:49:04+00:00  

## Log
- `18:48:52`   zip: 3548 bytes
## 1. Lambda

- `18:48:52`   Lambda exists — updating
- `18:48:55` ✅   ✓ updated justhodl-oecd-cli
- `18:48:55` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `18:48:55`   rule already correct: justhodl-oecd-cli-weekly (rate(7 days))
- `18:48:55` ✅   ✓ target → justhodl-oecd-cli
- `18:48:55` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:48:55`   invoking justhodl-oecd-cli…
- `18:49:04` ✅   ✓ smoke test passed
- `18:49:04`     ok                       True
- `18:49:04`     period                   2024-01-01
- `18:49:04`     global_avg               99.7
- `18:49:04`     us_cli                   99.85
