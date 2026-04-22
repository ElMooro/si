# Zero-Touch Smoke Test

**Status:** success  
**Duration:** 2.7s  
**Finished:** 2026-04-22T22:48:59+00:00  

## Data

| check | detail | status |
|---|---|---|
| ssm-token | len=43 | pass |
| lambda-url-auth | NVDA: $202.50 | +1.31% | Vol 107.1M shares | pass |
| worker-proxy | HTTP 403 | fail |

## Log
- `22:48:56` Starting checks
- `22:48:56` ✅ SSM token readable (length: 43)
- `22:48:59` ✅ Lambda direct: NVDA: $202.50 | +1.31% | Vol 107.1M shares
- `22:48:59` ✗ Worker proxy failed: HTTP 403 — error code: 1010
- `22:48:59` All checks complete
