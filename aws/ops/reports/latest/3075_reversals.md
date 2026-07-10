## 1. Deploy gate + invoke

**Status:** success  
**Duration:** 141.2s  
**Finished:** 2026-07-10T19:40:50+00:00  

## Data

| bottom_sample | buffer_days | n_bottoms | n_confirmed | n_fails | n_tops | n_warns | need_fresh | overlap | top_sample | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  | True |  |  |  |
| null | 226 | 0 |  |  | 15 |  |  | [] | {"ticker": "INTW", "class": "stock", "score": 60, "tier": "EARLY", "evidence": ["lost the 50DMA within 12 sessions", "50DMA slope rolled over", "BREAKDOWN below the 3-month range", "distribution cluster: 5 distribution days in 25 sessions (IBD >=5)"], "breakdown": true, "vol_confirm": false, "vol_ratio_today": 0.64, "distribution_days_25": 5, "obv_bear_div": false, "death_cross_sessions_ago": null |  |
|  |  |  | 0 |  |  |  |  |  |  |  |
|  |  |  |  | 0 |  | 1 |  |  |  | PASS |

## Log
## 2. Reversals schema + separation

## 3. Page live

## verdict

- `19:40:50` PASS
