# ops 3146 — IR quadrant chip + overlay-source audit

**Status:** failure  
**Duration:** 76.0s  
**Finished:** 2026-07-12T05:48:32+00:00  

## Error

```
SystemExit: 1
```

## Data

| kill_overlap | kill_theses_n | mr_top_n | n_fails | n_warns | squeeze_n | squeeze_overlap | verdict |
|---|---|---|---|---|---|---|---|
| 0 | 0 | 25 |  |  | 60 | 0 |  |
|  |  |  | 1 | 1 |  |  | FAIL |

## Log
## 1. Overlay sources vs master-ranker top names

- `05:47:17` kill-theses tickers: 
- `05:47:17` master-ranker top:   ALL, AMD, FDX, FISV, FITB, FIX, FWDI, GL, GM, GOOG, HPE, KEYS, KMX, LRCX, MIDD, MU, NEM, OC, ORCL, OXY, PLTR, PWR, SNDK, SPG, STX
- `05:47:17` ✅ squeeze overlap 0 legitimate: high-SI board disjoint from quality leaders today
## 2. IR page chip on CDN (warn-only)

- `05:48:32` ⚠ CDN still on prior page — max-age=600 self-heals; chip data is already in the feed either way
- `05:48:32` ✗ kill-theses feed EMPTY — premortem engine needs a look
