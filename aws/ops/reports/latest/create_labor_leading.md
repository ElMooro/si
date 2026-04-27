# Create/update justhodl-labor-leading Lambda + EB rule

**Status:** success  
**Duration:** 8.7s  
**Finished:** 2026-04-27T18:43:37+00:00  

## Log
- `18:43:29`   zip: 3052 bytes
## 1. Lambda

- `18:43:29`   Lambda missing — creating
- `18:43:34` ✅   ✓ created justhodl-labor-leading
- `18:43:34` ✅   ✓ reserved concurrency = 1
- `18:43:34` ✅   ✓ Function URL: https://ah6r4w6ee4xrjqm4h6ndgvkhpm0crmtn.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `18:43:34` ✅   ✓ created rule justhodl-labor-leading-daily
- `18:43:34` ✅   ✓ target → justhodl-labor-leading
- `18:43:35` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:43:35`   invoking justhodl-labor-leading…
- `18:43:37` ✅   ✓ smoke test passed
- `18:43:37`     ok                       True
- `18:43:37`     regime                   loosening
