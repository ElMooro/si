# Create/update justhodl-options-gamma Lambda + EB rule

**Status:** success  
**Duration:** 6.8s  
**Finished:** 2026-04-27T18:44:07+00:00  

## Log
- `18:44:00`   zip: 4121 bytes
## 1. Lambda

- `18:44:00`   Lambda missing — creating
- `18:44:05` ✅   ✓ created justhodl-options-gamma
- `18:44:05` ✅   ✓ reserved concurrency = 1
- `18:44:05` ✅   ✓ Function URL: https://fmgo6obv33snuh6llbgu4vgtm40zwgzu.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `18:44:06` ✅   ✓ created rule justhodl-options-gamma-30min
- `18:44:06` ✅   ✓ target → justhodl-options-gamma
- `18:44:06` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:44:06`   invoking justhodl-options-gamma…
- `18:44:07` ✅   ✓ smoke test passed
- `18:44:07`     error                    Empty options snapshot
