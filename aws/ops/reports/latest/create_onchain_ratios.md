# Create/update justhodl-onchain-ratios Lambda + EB rule

**Status:** success  
**Duration:** 8.2s  
**Finished:** 2026-04-27T18:44:00+00:00  

## Log
- `18:43:52`   zip: 2951 bytes
## 1. Lambda

- `18:43:52`   Lambda missing — creating
- `18:43:57` ✅   ✓ created justhodl-onchain-ratios
- `18:43:57` ✅   ✓ reserved concurrency = 1
- `18:43:57` ✅   ✓ Function URL: https://ragrpcwoywwsfvkv7hgpcuv6km0iyybq.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `18:43:57` ✅   ✓ created rule justhodl-onchain-ratios-6h
- `18:43:57` ✅   ✓ target → justhodl-onchain-ratios
- `18:43:58` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:43:58`   invoking justhodl-onchain-ratios…
- `18:44:00` ✅   ✓ smoke test passed
- `18:44:00`     ok                       True
