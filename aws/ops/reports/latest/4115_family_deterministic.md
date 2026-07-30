# ops 4115 — deterministic family run

**Status:** failure  
**Duration:** 671.4s  
**Finished:** 2026-07-30T03:16:42+00:00  

## Error

```
SystemExit: 1
```

## Data

| marker_before |
|---|
| tradingview-vault v3.14.1 ops4099 wallclock-guard |

## Log
## A. raw vault log tail (last 75 min, unfiltered)

- `03:05:31`   [02:45:11] INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89
- `03:05:31`   [02:45:12] START RequestId: d10e0460-ca39-4a63-8bcb-d9b9ba55078c Version: $LATEST
- `03:05:31`   [02:45:12] [tv-vault] tradingview-vault v3.15.0 ops4112 family-adapters
- `03:05:31`   [02:45:13] [vault] full universe: +9468 watchlist symbols admitted
- `03:05:31`   [02:45:13] [vault] generated aliases loaded: 911
- `03:05:31`   [02:45:13] [vault] expansion: 911 aliases · re-admit 0 cached · 0 unseen · spending budget on 0
- `03:05:31`   [02:45:13] [vault] fmp batch capped to 400
- `03:05:31`   [03:00:12] END RequestId: d10e0460-ca39-4a63-8bcb-d9b9ba55078c
- `03:05:31`   [03:00:12] REPORT RequestId: d10e0460-ca39-4a63-8bcb-d9b9ba55078c	Duration: 900000.00 ms	Billed Duration: 900745 ms	Memory Size: 1024 MB	Max Memory Used: 134 MB	Init Durat
- `03:05:31`   [03:01:15] START RequestId: d10e0460-ca39-4a63-8bcb-d9b9ba55078c Version: $LATEST
- `03:05:31`   [03:01:15] [tv-vault] tradingview-vault v3.15.0 ops4112 family-adapters
- `03:05:31`   [03:01:16] [vault] full universe: +9468 watchlist symbols admitted
- `03:05:31`   [03:01:16] [vault] generated aliases loaded: 911
- `03:05:31`   [03:01:16] [vault] expansion: 911 aliases · re-admit 0 cached · 0 unseen · spending budget on 0
- `03:05:31`   [03:01:16] [vault] fmp batch capped to 400
## B. self-invoke + poll (11 min)

## C. post-attempt raw tail

- `03:16:42`   [03:05:32] INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89
- `03:16:42`   [03:05:32] START RequestId: 6374f735-2227-4820-817c-c906319f7e94 Version: $LATEST
- `03:16:42`   [03:05:32] [tv-vault] tradingview-vault v3.15.0 ops4112 family-adapters
- `03:16:42`   [03:05:33] [vault] full universe: +9468 watchlist symbols admitted
- `03:16:42`   [03:05:33] [vault] generated aliases loaded: 911
- `03:16:42`   [03:05:33] [vault] expansion: 911 aliases · re-admit 0 cached · 0 unseen · spending budget on 0
- `03:16:42`   [03:05:33] [vault] fmp batch capped to 400
- `03:16:42`   [03:16:15] END RequestId: d10e0460-ca39-4a63-8bcb-d9b9ba55078c
- `03:16:42`   [03:16:15] REPORT RequestId: d10e0460-ca39-4a63-8bcb-d9b9ba55078c	Duration: 900000.00 ms	Billed Duration: 900000 ms	Memory Size: 1024 MB	Max Memory Used: 134 MB	Status: ti
- `03:16:42` ✗ VAULT NEVER WROTE v3.15.0 — evidence above
