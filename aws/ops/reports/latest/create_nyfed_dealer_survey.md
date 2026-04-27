# Create/update justhodl-nyfed-dealer-survey Lambda + EB rule

**Status:** success  
**Duration:** 5.7s  
**Finished:** 2026-04-27T18:43:43+00:00  

## Log
- `18:43:38`   zip: 2528 bytes
## 1. Lambda

- `18:43:38`   Lambda missing — creating
- `18:43:40` ✅   ✓ created justhodl-nyfed-dealer-survey
- `18:43:40` ✅   ✓ reserved concurrency = 1
- `18:43:41` ✅   ✓ Function URL: https://6horzq6leer2k633cbztktwybu0ltioz.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `18:43:41` ✅   ✓ created rule justhodl-nyfed-dealer-survey-weekly
- `18:43:41` ✅   ✓ target → justhodl-nyfed-dealer-survey
- `18:43:41` ✅   ✓ added invoke permission
## 3. Smoke test

- `18:43:41`   invoking justhodl-nyfed-dealer-survey…
- `18:43:43` ✅   ✓ smoke test passed
- `18:43:43`     ok                       True
- `18:43:43`     new_survey               True
