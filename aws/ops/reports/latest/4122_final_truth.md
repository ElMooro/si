# ops 4113 — family verify-only

**Status:** failure  
**Duration:** 2.0s  
**Finished:** 2026-07-30T04:25:17+00:00  

## Error

```
SystemExit: 1
```

## Data

| feed_counts | feed_elapsed | generated_at | marker |
|---|---|---|---|
|  |  | 2026-07-29T21:55:11.472980+00:00 | tradingview-vault v3.14.1 ops4099 wallclock-guard |
| {"INTR": 46, "FER": 183, "GDPYY": 261, "IRYY": 240, "UR": 234} | 1.1 |  |  |

## Log
## vault's own story (last 40 min)

- `04:25:17`   [03:50:40] [tv-vault] families: {'GDPYY': 261, 'IRYY': 240, 'UR': 234, 'FERWB': 183, 'INTR': 46, 'FER': 0} in 0.9s
- `04:25:17`   [03:53:33] REPORT RequestId: 6374f735-2227-4820-817c-c906319f7e94	Duration: 900000.00 ms	Billed Duration: 900486 ms	Memory Size: 1024 MB	Max Memory Used: 134 MB	Init Duration: 485.9
- `04:25:17`   [04:05:35] REPORT RequestId: e3e04958-c179-451e-ad09-2219fcde98a9	Duration: 900000.00 ms	Billed Duration: 900498 ms	Memory Size: 1024 MB	Max Memory Used: 134 MB	Init Duration: 497.8
- `04:25:17`   [04:06:37] [tv-vault] families: {'GDPYY': 261, 'IRYY': 240, 'UR': 234, 'FERWB': 183, 'INTR': 46, 'FER': 0} in 2.0s
- `04:25:17`   [04:10:22] [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `04:25:17` ✗ ARTIFACT NOT v3.15.0 — evidence above
