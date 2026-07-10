## 1. Deploy gate + invoke

**Status:** success  
**Duration:** 141.1s  
**Finished:** 2026-07-10T23:08:53+00:00  

## Data

| bottom_sample | buffer_days | dn_sample | n_200dma_breaks | n_bottoms | n_confirmed | n_dn | n_fails | n_tops | n_up | n_vol_confirmed | n_warns | need_fresh | overlap | pct200_present | top_sample | up_sample | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |
| null | 226 |  |  | 0 |  |  |  | 15 |  |  |  |  | [] |  | {"ticker": "INTW", "class": "stock", "score": 80, "tier": "EARLY", "evidence": ["lost the 50DMA within 12 sessions", "BROKE BELOW the 200DMA within 12 sessions (Weinstein Stage 4 transition)", "50DMA slope rolled over", "BREAKDOWN below the 3-month range", "distribution cluster: 5 distribution days in 25 sessions (IBD >=5)"], "breakdown": true, "broke_200dma_down": true, "pct_vs_200dma": -72.3, "v |  |  |
|  |  |  | 12 |  |  |  |  |  |  |  |  |  |  | True |  |  |  |
|  |  |  |  |  | 0 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | {"ticker": "MDLZ", "class": "stock", "sessions_ago": 2, "vol_ratio_on_break": 0.79, "vol_confirm": false, "pct_vs_200dma": -0.2, "still_beyond": true, "phase": "MARKDOWN", "radar_flag": null} |  |  |  | 25 |  |  | 25 | 8 |  |  |  |  |  | {"ticker": "META", "class": "stock", "sessions_ago": 1, "vol_ratio_on_break": 0.84, "vol_confirm": false, "pct_vs_200dma": 4.0, "still_beyond": true, "phase": "MARKUP", "radar_flag": null} |  |
|  |  |  |  |  |  |  | 0 |  |  |  | 1 |  |  |  |  |  | PASS |

## Log
## 2. Reversals schema + separation

## 2b. dma200_breaks boards

## 3. Page live

## verdict

- `23:08:53` PASS
