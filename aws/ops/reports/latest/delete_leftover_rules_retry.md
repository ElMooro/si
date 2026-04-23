# Retry EB rule deletions (IAM should be propagated now)

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-04-23T00:03:16+00:00  

## Data

| attempt | rule | status |
|---|---|---|
| 1 | lambda-warmer-system3 | deleted |
| 1 | lambda-warmer-system3-frequent | deleted |
| 1 | DailyEmailReportsV2_8AMET | deleted |

## Log
## Attempt 1

- `00:03:15`   lambda-warmer-system3:
- `00:03:15`     removed 1 target(s)
- `00:03:15` ✅     Rule deleted
- `00:03:15`   lambda-warmer-system3-frequent:
- `00:03:15`     removed 1 target(s)
- `00:03:15` ✅     Rule deleted
- `00:03:15`   DailyEmailReportsV2_8AMET:
- `00:03:16`     removed 1 target(s)
- `00:03:16` ✅     Rule deleted
- `00:03:16` ✅ All rules handled successfully
## Final verification

- `00:03:16`   lambda-warmer-system3: ✓ gone
- `00:03:16`   lambda-warmer-system3-frequent: ✓ gone
- `00:03:16`   DailyEmailReportsV2_8AMET: ✓ gone
- `00:03:16`   DailyEmailReportsV2: ✓ present
- `00:03:16` Done
