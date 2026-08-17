# ops 4803 -- DTCC Wayback retry (backoff)

**Status:** success  
**Duration:** 340.5s  
**Finished:** 2026-08-17T00:53:05+00:00  

## Log
- `00:47:30` attempt 0: HTTPError: HTTP Error 503: Service Unavailable
- `00:47:45` attempt 1: HTTPError: HTTP Error 503: Service Unavailable
- `00:48:10` attempt 2: HTTPError: HTTP Error 503: Service Unavailable
- `00:48:55` attempt 3: HTTPError: HTTP Error 503: Service Unavailable
- `00:50:20` attempt 4: HTTPError: HTTP Error 503: Service Unavailable
- `00:53:05` attempt 5: HTTPError: HTTP Error 503: Service Unavailable
- `00:53:05` ⚠ CDX unreachable after backoff -- leave for the next scheduled attempt; live upsert continues daily regardless
