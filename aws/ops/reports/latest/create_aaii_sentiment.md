# Create/update justhodl-aaii-sentiment Lambda + EB rule

**Status:** success  
**Duration:** 7.3s  
**Finished:** 2026-04-27T18:43:21+00:00  

## Log
- `18:43:14`   zip: 3156 bytes
## 1. Lambda

- `18:43:14`   Lambda missing — creating
- `18:43:19` ✅   ✓ created justhodl-aaii-sentiment
- `18:43:19` ✅   ✓ reserved concurrency = 1
- `18:43:19` ✅   ✓ Function URL: https://t2hqpzrcow5dmqcap4wrs7za2i0alvoe.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `18:43:20` ✅   ✓ created rule justhodl-aaii-sentiment-daily
- `18:43:20` ✅   ✓ target → justhodl-aaii-sentiment
- `18:43:20` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:43:20`   invoking justhodl-aaii-sentiment…
- `18:43:21` ✅   ✓ smoke test passed
- `18:43:21`     error                    AAII fetch failed: HTTP Error 403: Forbidden
