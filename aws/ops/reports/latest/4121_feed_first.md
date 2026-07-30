# ops 4121 — families-feed + vault feed-first

**Status:** failure  
**Duration:** 836.6s  
**Finished:** 2026-07-30T04:23:28+00:00  

## Error

```
SystemExit: 1
```

## Data

| FER | GDPYY | INTR | IRYY | UR | feed_counts | feed_elapsed_s | feed_fnerr |
|---|---|---|---|---|---|---|---|
|  |  |  |  |  | {"INTR": 46, "FER": 183, "GDPYY": 261, "IRYY": 240, "UR": 234} |  | None |
| 183 | 261 | 46 | 240 | 234 |  | 1.1 |  |

## Log
## A. families-feed: create/settle, invoke, verify

- `04:09:32`   update EXC: ResourceConflictException: An error occurred (ResourceConflictException) when calling the UpdateFunctionCode operation: The operation cannot be per
- `04:09:43` ✅   justhodl-families-feed update accepted (attempt 1)
- `04:09:53` ✅   justhodl-families-feed settled at loop 1
## B. vault: settle v3.15.3 + memory 2048

- `04:09:56` ✅   justhodl-tradingview update accepted (attempt 0)
- `04:10:06` ✅   justhodl-tradingview settled at loop 1
- `04:10:06` ✅   memory -> 2048 (2x CPU)
## C. vault run + gates

- `04:23:28` ✗ vault artifact never moved to v3.15.3
- `04:23:28` ✅   feed settled
- `04:23:28` ✅   feed INTR >=25
- `04:23:28` ✅   feed FER >=120
- `04:23:28` ✅   feed WB trio >=600
- `04:23:28` ✅   vault settled
