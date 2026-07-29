# ops 4098 — full TradingView universe on the vault page

**Status:** failure  
**Duration:** 917.6s  
**Finished:** 2026-07-29T21:52:13+00:00  

## Error

```
SystemExit: 1
```

## Log
## A. why are the cftc rows still NO_FREE_SOURCE?

- `21:36:55`   cot aliases in artifact: 65
- `21:36:55`   cot alias keys that match a vault symbol EXACTLY: 65
- `21:36:55`   sample alias key : 020601_FO_AMP_SPREAD
- `21:36:55`   in aliases map   : True
- `21:36:55`   alias value      : cftc:yw9f-hn96:020601:asset_mgr_positions_spread
- `21:36:56`   LIVE CFTC CALL   : [{'report_date_as_yyyy_mm_dd': '2026-07-21T00:00:00.000', 'asset_mgr_positions_spread': '266327'}]
- `21:36:56`   → if this returns a value, the API and column are fine and the fault is in how the vault reaches the alias
## B. deploy vault v3.14.0

- `21:37:07`   ✓ settled (attempt 2)
## C. run it (async + poll)

- `21:52:13` ✗ never moved
