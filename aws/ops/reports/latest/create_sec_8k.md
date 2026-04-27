# Create/update justhodl-sec-8k Lambda + EB rule

**Status:** success  
**Duration:** 26.8s  
**Finished:** 2026-04-27T18:45:04+00:00  

## Log
- `18:44:37`   zip: 3311 bytes
## 1. Lambda

- `18:44:37`   Lambda missing — creating
- `18:44:42` ✅   ✓ created justhodl-sec-8k
- `18:44:42` ✅   ✓ reserved concurrency = 1
- `18:44:42` ✅   ✓ Function URL: https://m45q3c6w2hlramagf6wvqoig3u0jglrk.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `18:44:43` ✅   ✓ created rule justhodl-sec-8k-30min
- `18:44:43` ✅   ✓ target → justhodl-sec-8k
- `18:44:43` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:44:43`   invoking justhodl-sec-8k…
- `18:45:04` ✅   ✓ smoke test passed
- `18:45:04`     error                    SEC atom failed: The read operation timed out
