# Create/update justhodl-oecd-cli Lambda + EB rule

**Status:** success  
**Duration:** 7.8s  
**Finished:** 2026-04-27T18:43:51+00:00  

## Log
- `18:43:44`   zip: 3229 bytes
## 1. Lambda

- `18:43:44`   Lambda missing — creating
- `18:43:48` ✅   ✓ created justhodl-oecd-cli
- `18:43:48` ✅   ✓ reserved concurrency = 1
- `18:43:49` ✅   ✓ Function URL: https://smwpjxkplu6jvcdarggwj3v5xy0unbwt.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `18:43:49` ✅   ✓ created rule justhodl-oecd-cli-weekly
- `18:43:49` ✅   ✓ target → justhodl-oecd-cli
- `18:43:49` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:43:49`   invoking justhodl-oecd-cli…
- `18:43:51` ✅   ✓ smoke test passed
- `18:43:51`     error                    OECD fetch failed: HTTP Error 404: Not Found
