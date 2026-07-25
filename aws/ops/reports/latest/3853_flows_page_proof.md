# ops 3853 — flows.html render fix proven on the edge, live feed

**Status:** failure  
**Duration:** 180.9s  
**Finished:** 2026-07-25T15:24:00+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. EDGE — poll until the new page is actually served

- `15:20:59`   attempt 1: stale (42,784 bytes, no marker)
- `15:21:19`   attempt 2: stale (42,784 bytes, no marker)
- `15:21:39`   attempt 3: stale (42,784 bytes, no marker)
- `15:21:59`   attempt 4: stale (42,784 bytes, no marker)
- `15:22:19`   attempt 5: stale (42,784 bytes, no marker)
- `15:22:40`   attempt 6: stale (42,784 bytes, no marker)
- `15:23:00`   attempt 7: stale (42,784 bytes, no marker)
- `15:23:20`   attempt 8: stale (42,784 bytes, no marker)
- `15:23:40`   attempt 9: stale (42,784 bytes, no marker)
- `15:24:00` ✗   marker JH_FLOWS_DIVFIX_3853 never reached the edge after 9 attempts
