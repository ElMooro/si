# ops 3943 — v3.5 SNB+IMF+gov-proxy

**Status:** failure  
**Duration:** 462.5s  
**Finished:** 2026-07-27T00:54:49+00:00  

## Error

```
SystemExit: 1
```

## Data

| coverage_pct | fer_live | n_live | statuses |
|---|---|---|---|
| 80.9 | 0 | 454 | {'META': 1, 'LIVE': 454, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 104} |

## Log
## BOJ api manual — zlib stream decompress, hunt the base URL

- `00:47:07`   no URLs in decompressed streams — manual may embed the base as styled text; try api_notice_en.pdf next
- `00:47:07` ✅   settled attempt 1 (strings in zip)
- `00:54:49` ✅   refreshed ~450s
- `00:54:49`   JP02Y: NO_FREE_SOURCE value=None src=unresolved_tv_only asof=None
- `00:54:49`   CH02Y: LIVE value=-0.083 src=snb asof=snb:2025-07
- `00:54:49`   CH03Y: LIVE value=-0.043 src=snb asof=snb:2025-07
- `00:54:49`   USFER: NO_FREE_SOURCE value=None src=unresolved_economics asof=None
- `00:54:49` ✅   v3.5 settled + strings in artifact
- `00:54:49` ✅   force run wrote
- `00:54:49` ✗   JP02Y LIVE via mof
- `00:54:49` ✅   CH02Y LIVE via snb
- `00:54:49` ✅   CH03Y LIVE via snb
- `00:54:49` ✗   USFER LIVE via imf
- `00:54:49` ✗   FER family >= 3 LIVE
- `00:54:49` ✗   n_live >= 458
- `00:54:49` ✅   zero bare UNRESOLVED
- `00:54:49` ✗ FAILED: ['JP02Y LIVE via mof', 'USFER LIVE via imf', 'FER family >= 3 LIVE', 'n_live >= 458']
