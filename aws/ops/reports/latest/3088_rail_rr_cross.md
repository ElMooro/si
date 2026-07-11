## 0. Engine v3.5: invoke + adaptive delta

**Status:** success  
**Duration:** 41.3s  
**Finished:** 2026-07-11T01:52:26+00:00  

## Data

| accdist_soldiers | enriched_soldiers | live_after_s | n_fails | n_warns | need_fresh | rank_days_rows | rank_delta_rows | rank_note | rr_debug | rr_sample | rr_soldiers | sample_rd | sma20_rows | soldier_sample | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | 40 | 40 | ADAPTIVE: rank delta measured over the 3 sessions accrued so far; converges to 20d at 21 sessions (4/21) |  |  |  | [{"etf": "SMH", "rd": 12, "d": 3}, {"etf": "CIBR", "rd": 0, "d": 3}, {"etf": "XLK", "rd": 12, "d": 3}, {"etf": "XBI", "rd": -3, "d": 3}] |  |  |  |
|  | 54 |  |  |  |  |  |  |  |  |  |  |  |  | [{"ticker": "NVDA", "weight_pct": 19.79, "whale_musd": -4428, "rr": {"up_pct": 12.1, "down_pct": 1.0, "ratio": 12.1, "stop_basis": "50DMA"}}, {"ticker": "TSM", "weight_pct": 9.42, "whale_musd": -2774, "rr": {"up_pct": 10.3, "down_pct": 2.6, |  |
|  |  |  |  |  |  |  |  |  | null |  |  |  |  |  |  |
| 4 |  |  |  |  |  |  |  |  |  | [{"t": "NVDA", "up_pct": 12.1, "down_pct": 1.0, "ratio": 12.1, "stop_basis": "50DMA"}, {"t": "TSM", "up_pct": 10.3, "down_pct": 2.6, "ratio": 4.0, "stop_basis": "50DMA"}, {"t": "AVGO", "up_pct": 23.8, "down_pct": 10.4, "ratio": 2.3, "stop_basis": "200DMA"}] | 60 |  | 40 |  |  |
|  |  | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 0 | 0 |  |  |  |  |  |  |  |  |  |  | PASS |

## Log
## 1. Page live (this-push marker)

## 2. Rail hidden-by-default (site-wide)

## verdict

- `01:52:26` PASS
