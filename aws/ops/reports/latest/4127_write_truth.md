# ops 4113 — family verify-only

**Status:** failure  
**Duration:** 2.2s  
**Finished:** 2026-07-30T05:34:35+00:00  

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

- `05:34:35`   [04:54:20] REPORT RequestId: bcd24a90-7f84-4b98-8509-cd5cfe26d3d3	Duration: 900000.00 ms	Billed Duration: 900496 ms	Memory Size: 2048 MB	Max Memory Used: 134 MB	Init Duration: 495.5
- `05:34:35`   [04:55:24] [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `05:34:35`   [04:56:10] [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `05:34:35`   [04:56:10] [tv-vault][phase] families-preflight-done t+4.6s
- `05:34:35`   [04:58:22] REPORT RequestId: 8d8ef73f-dc92-40d7-9870-9f293eac697a	Duration: 900000.00 ms	Billed Duration: 900545 ms	Memory Size: 2048 MB	Max Memory Used: 134 MB	Init Duration: 544.9
- `05:34:35`   [05:10:20] REPORT RequestId: bcd24a90-7f84-4b98-8509-cd5cfe26d3d3	Duration: 900000.00 ms	Billed Duration: 900000 ms	Memory Size: 2048 MB	Max Memory Used: 134 MB	Status: timeout
XRAY
- `05:34:35`   [05:11:05] REPORT RequestId: 48783754-0b39-410b-9309-e98dcc941529	Duration: 900000.00 ms	Billed Duration: 900480 ms	Memory Size: 2048 MB	Max Memory Used: 134 MB	Init Duration: 479.6
- `05:34:35`   [05:12:12] [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `05:34:35`   [05:12:12] [tv-vault][phase] families-preflight-done t+3.6s
- `05:34:35`   [05:12:20] [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `05:34:35`   [05:12:20] [tv-vault][phase] families-preflight-done t+3.6s
- `05:34:35`   [05:18:57] [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `05:34:35`   [05:18:57] [tv-vault][phase] families-preflight-done t+3.8s
- `05:34:35` ✗ ARTIFACT NOT v3.15.0 — evidence above
