# Create/update justhodl-sec-10kq Lambda + EB rule

**Status:** success  
**Duration:** 14.4s  
**Finished:** 2026-04-27T18:44:29+00:00  

## Log
- `18:44:14`   zip: 2614 bytes
## 1. Lambda

- `18:44:15`   Lambda missing — creating
- `18:44:19` ✅   ✓ created justhodl-sec-10kq
- `18:44:20` ✅   ✓ reserved concurrency = 1
- `18:44:20` ✅   ✓ Function URL: https://udfjmuuo2dcx4i2mag5bhlkara0zkjgl.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `18:44:20` ✅   ✓ created rule justhodl-sec-10kq-4h
- `18:44:20` ✅   ✓ target → justhodl-sec-10kq
- `18:44:20` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:44:20`   invoking justhodl-sec-10kq…
- `18:44:29` ✅   ✓ smoke test passed
- `18:44:29`     total                    124
- `18:44:29`     total_10k                28
- `18:44:29`     total_10q                96
- `18:44:29`     total_10k_amended        0
- `18:44:29`     total_10q_amended        0
- `18:44:29`     fetch_duration_s         7.4
