# ops 4113 — family verify-only

**Status:** failure  
**Duration:** 0.9s  
**Finished:** 2026-07-30T05:44:17+00:00  

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

- `05:44:17`   [05:27:08] REPORT RequestId: 48783754-0b39-410b-9309-e98dcc941529	Duration: 900000.00 ms	Billed Duration: 900000 ms	Memory Size: 2048 MB	Max Memory Used: 134 MB	Status: timeout
XRAY
- `05:44:17`   [05:27:16] REPORT RequestId: bcd24a90-7f84-4b98-8509-cd5cfe26d3d3	Duration: 900000.00 ms	Billed Duration: 900515 ms	Memory Size: 2048 MB	Max Memory Used: 134 MB	Init Duration: 514.3
- `05:44:17`   [05:29:20] [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `05:44:17`   [05:29:20] [tv-vault][phase] families-preflight-done t+3.9s
- `05:44:17`   [05:33:53] REPORT RequestId: 9993c5db-6f02-4185-a707-3a8ed9881df1	Duration: 900000.00 ms	Billed Duration: 900507 ms	Memory Size: 2048 MB	Max Memory Used: 134 MB	Init Duration: 506.4
- `05:44:17`   [05:34:56] [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `05:44:17`   [05:34:56] [tv-vault][phase] families-preflight-done t+4.2s
- `05:44:17` ✗ ARTIFACT NOT v3.15.0 — evidence above
