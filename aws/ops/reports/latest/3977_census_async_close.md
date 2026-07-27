# ops 3977 — data-census async close + schedule + page edge

**Status:** failure  
**Duration:** 730.5s  
**Finished:** 2026-07-27T18:17:55+00:00  

## Error

```
SystemExit: 1
```

## Log
## A. does the crashed invoke's output already exist?

- `18:05:45`   no artifact yet
## B. async invoke + poll (the 3972 pattern)

- `18:07:47`   [5] still waiting
- `18:09:49`   [11] still waiting
- `18:11:50`   [17] still waiting
- `18:13:52`   [23] still waiting
- `18:15:53`   [29] still waiting
- `18:17:55`   [35] still waiting
- `18:17:55` ✗ census never wrote — check CloudWatch for the lambda error
