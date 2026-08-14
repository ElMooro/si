# ops 4657 — v1.4.0 expansion

**Status:** failure  
**Duration:** 184.0s  
**Finished:** 2026-08-14T01:23:48+00:00  

## Error

```
SystemExit: 1
```

## Data

| f13 | f13_n | fc3 | fcfy | fn_error | golden | inv_rev | ladder | nbb | ps | warned | whale_rule |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 6522 |  |  |  |  |  |  |  |  |  | clone-alpha skill_score>=55, else top-3 positive-skill funds |
|  |  |  |  | Unhandled |  |  |  |  |  |  |  |
| None |  | 0 | 0 |  | 0 | 0 | 0 | 0 | 0 | 0 |  |

## Log
## evidence: 13F 't' sample + options discovery

- `01:20:44` 13F t[AAPL] = {"b": 3568974207, "s": 9007628327, "n": -5438654120, "wb": 794441234, "ws": 0, "wn": 794441234, "nf": 14, "na": 4, "tv": 78650336796, "fb": ["Two Sigma", "Duration Capital", "Renaissance Technologies"], "fs": ["Citadel Advisors", "Berkshire Hathaway"]}
- `01:21:45` options stores: ['data/crypto-options-history.json', 'data/crypto-options-surface-history.json', 'data/crypto-options-surface.json', 'data/crypto-options.json', 'data/engines/wl-fear-greed-in-mkt-put-call-ratio.json', 'data/engines/wl-index-option-settlement.json', 'data/options-analytics-iv-history.json', 'data/options-analytics.json']
## deploy + settle

- `01:21:46` ✅   [deploy] v1.4.0 live
## run + expansion truth

- `01:21:47` ✗   [ladder] CONTRACT MISS — 0/300-lane rows carry 5-SMA ladder; 0 golden
- `01:21:47` ✗   [binds] CONTRACT MISS — census binds live (nbb/ps/fcfy/fc3/inv >= 200 each)
- `01:21:47` ✗   [warnings] CONTRACT MISS — 0 rows carry red-flag warnings
- `01:21:47` ✗   [f13-join] CONTRACT MISS — 13F flows joined on None rows
## edge (page v5)

- `01:23:48` ✅   [page] v5 structural tokens at edge
## verdict

- `01:23:48` ✗ v1.4 expansion: 4 red
