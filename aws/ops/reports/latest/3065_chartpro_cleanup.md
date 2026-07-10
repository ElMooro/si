## 1. Page live (post-CDN)

**Status:** failure  
**Duration:** 503.7s  
**Finished:** 2026-07-10T17:30:17+00:00  

## Error

```
SystemExit: 1
```

## Data

| live_after_s | n_fails | n_warns | verdict |
|---|---|---|---|
| 140 |  |  |  |
|  | 2 | 0 | FAIL |

## Log
## 2. Worker: full-history cap live

## 3. Index route (Yahoo path)

## verdict

- `17:30:17` FAIL: /ohlc days=9999 never exceeded 300 weekly bars (cap not live?)
- `17:30:17` FAIL: yf-ohlc ^GSPC: not enough arguments for format string
