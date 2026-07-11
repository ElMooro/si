## 0. Engine v3.5: invoke + adaptive delta

**Status:** failure  
**Duration:** 122.1s  
**Finished:** 2026-07-11T01:28:10+00:00  

## Error

```
SystemExit: 1
```

## Data

| accdist_soldiers | enriched_soldiers | live_after_s | n_fails | n_warns | need_fresh | rank_days_rows | rank_delta_rows | rank_note | rr_sample | rr_soldiers | sample_rd | sma20_rows | soldier_sample | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | 40 | 40 | ADAPTIVE: rank delta measured over the 3 sessions accrued so far; converges to 20d at 21 sessions (4/21) |  |  | [{"etf": "SMH", "rd": 12, "d": 3}, {"etf": "CIBR", "rd": 0, "d": 3}, {"etf": "XLK", "rd": 12, "d": 3}, {"etf": "XBI", "rd": -3, "d": 3}] |  |  |  |
|  | 54 |  |  |  |  |  |  |  |  |  |  |  | [{"ticker": "NVDA", "weight_pct": 19.79, "whale_musd": -4428}, {"ticker": "TSM", "weight_pct": 9.42, "whale_musd": -2774}, {"ticker": "AVGO", "weight_pct": 6.0, "whale_musd": -193}] |  |
| 4 |  |  |  |  |  |  |  |  | [] | 0 |  | 40 |  |  |
|  |  | 0 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 1 | 0 |  |  |  |  |  |  |  |  |  | FAIL |

## Log
## 1. Page live (this-push marker)

## 2. Rail hidden-by-default (site-wide)

## verdict

- `01:28:10` FAIL: rr on 0 soldiers (<10)
