## 1. IR deploy gate + invoke (flows join)

**Status:** failure  
**Duration:** 244.8s  
**Finished:** 2026-07-08T02:53:06+00:00  

## Error

```
SystemExit: 1
```

## Data

| dead | dead_list | err | flows_joined | ir_secs | registry_engines | sample | scheduled | scorecard_rows | skipped | stale | stale_list | verdicts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | None |  | 29.7 |  |  |  |  |  |  |  |  |
|  |  |  | 0 |  |  | [] |  | 40 |  |  |  |  |
|  |  |  |  |  | 584 |  |  |  |  |  |  |  |
| 9 | [["justhodl-engine-robustness", null], ["justhodl-trade-journal", 1475], ["justhodl-transcript-indexer", null], ["justhodl-eurostat-history", null], ["justhodl-watchlist", 1475], ["justhodl-transcript-query", null], ["justhodl-ici-flows", null], ["justhodl-feedback", 1550], ["justhodl-kill-switch",  |  |  |  |  |  |  |  | 11 | 11 | [["justhodl-history-api", 709.8], ["justhodl-nyfed-pd", 101.3], ["justhodl-forward-orders", 85.3], ["justhodl-beneish", 71.8], ["justhodl-edge-discovery", 63.8], ["justhodl-engine-contribution", 62.8], ["justhodl-stress-loadings", 62.8], ["justhodl-forward-returns", 62.8], ["justhodl-behavior-mirror", 61.8], ["justhodl-opportunity-calibrator", 59.3], ["justhodl-causality-scanner", 53.8]] |  |
|  |  |  |  |  |  |  | ["justhodl-engine-contribution", "justhodl-causality-scanner", "justhodl-behavior-mirror", "justhodl-stress-loadings", "justhodl-opportunity-calibrator", "justhodl-beneish"] |  |  |  |  | {"RESURRECTED_SCHEDULED": 6, "REFRESHED_HAS_RULE": 4, "NO_WRITE_AFTER_POKE": 9, "REGISTRY_GHOST_OR_ERR": 1} |

## Log
## 2. Registry load + freshness recompute

## 3. Poke + evidence + safe fixes

- `02:53:03` ✅   ✓ created rule engine-contribution-daily-resurrected
- `02:53:03` ✅   ✓ target → justhodl-engine-contribution
- `02:53:04` ✅   ✓ added invoke permission
- `02:53:04` ✅   ✓ created rule causality-scanner-daily-resurrected
- `02:53:04` ✅   ✓ target → justhodl-causality-scanner
- `02:53:04` ✅   ✓ added invoke permission
- `02:53:04` ✅   ✓ created rule behavior-mirror-daily-resurrected
- `02:53:04` ✅   ✓ target → justhodl-behavior-mirror
- `02:53:04` ✅   ✓ added invoke permission
- `02:53:05` ✅   ✓ created rule stress-loadings-daily-resurrected
- `02:53:05` ✅   ✓ target → justhodl-stress-loadings
- `02:53:05` ✅   ✓ added invoke permission
- `02:53:05` ✅   ✓ created rule opportunity-calibrator-daily-resurrected
- `02:53:05` ✅   ✓ target → justhodl-opportunity-calibrator
- `02:53:05` ✅   ✓ added invoke permission
- `02:53:05` ✅   ✓ created rule beneish-daily-resurrected
- `02:53:05` ✅   ✓ target → justhodl-beneish
- `02:53:06` ✅   ✓ added invoke permission
- `02:53:06` FAILS=1
