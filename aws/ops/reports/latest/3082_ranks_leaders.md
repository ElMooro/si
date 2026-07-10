## 0. Engine v3.5: invoke + adaptive delta

**Status:** success  
**Duration:** 2.6s  
**Finished:** 2026-07-10T23:43:33+00:00  

## Data

| enriched_soldiers | live_after_s | n_fails | n_warns | need_fresh | rank_days_rows | rank_delta_rows | rank_note | sample_rd | soldier_sample | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | False |  |  |  |  |  |  |
|  |  |  |  |  | 40 | 40 | ADAPTIVE: rank delta measured over the 2 sessions accrued so far; converges to 20d at 21 sessions (3/21) | [{"etf": "SMH", "rd": 12, "d": 2}, {"etf": "CIBR", "rd": 0, "d": 2}, {"etf": "XLK", "rd": 12, "d": 2}, {"etf": "XBI", "rd": -3, "d": 2}] |  |  |
| 54 |  |  |  |  |  |  |  |  | [{"ticker": "NVDA", "weight_pct": 19.79, "whale_musd": -4428}, {"ticker": "TSM", "weight_pct": 9.42, "whale_musd": -2774}, {"ticker": "AVGO", "weight_pct": 6.0, "whale_musd": -193}] |  |
|  | 0 |  |  |  |  |  |  |  |  |  |
|  |  | 0 | 0 |  |  |  |  |  |  | PASS |

## Log
## 1. Page live (this-push marker)

## verdict

- `23:43:33` PASS
