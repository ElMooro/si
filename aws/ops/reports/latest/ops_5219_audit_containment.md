# ops 5219 -- audit 2026-09-08 Release A: containment (SSM managed config, owner binding, guest-namespace migration)

**Status:** failure  
**Duration:** 911.2s  
**Finished:** 2026-09-08T21:54:29+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. canonical SSM parameters

- `21:39:26` scanned 883 functions' environments
- `21:39:26` ⚠ /justhodl/fmp/api-key: 2 distinct live values across the fleet (majority 193 functions) -- rotation must reconcile these
- `21:39:26` ✅ /justhodl/fmp/api-key CREATED from the live env majority value (ww…xb, 193 functions)
- `21:39:26` ⚠ /justhodl/polygon/api-key: 2 distinct live values across the fleet (majority 132 functions) -- rotation must reconcile these
- `21:39:26` ✅ /justhodl/polygon/api-key exists and matches the fleet majority value (zv…_d, 132 functions)
- `21:39:26` ⚠ /justhodl/fred/api-key: 2 distinct live values across the fleet (majority 186 functions) -- rotation must reconcile these
- `21:39:26` ✅ /justhodl/fred/api-key exists and matches the fleet majority value (2f…89, 186 functions)
- `21:39:26` ✅ /justhodl/cmc/api-key CREATED from the live env majority value (17…97, 49 functions)
- `21:39:26` ✅ /justhodl/telegram/bot_token exists and matches the fleet majority value (86…Gs, 121 functions)
- `21:39:26` ✅ /justhodl/newsapi/api-key CREATED from the live env majority value (17…40, 18 functions)
- `21:39:27` ✅ /justhodl/census/api-key CREATED from the live env majority value (84…15, 3 functions)
## 2. worker v2.1.0 + owner binding + guest migration

## 3. re-dispatch deploy-workers (secrets from the parameters above)

- `21:54:29` ⚠ workflow dispatch -> HTTP 403 b'{"message":"Resource not accessible by personal access token","documentation_url":"https://docs.gith' (secrets attach on the next worker push instead)
## verdict

- `21:54:29` ✗ data-proxy is None after 901s (expected 2.1.0)
- `21:54:29` RED: 1 failure(s)
