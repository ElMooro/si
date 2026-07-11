## 0. Engine v3.5: invoke + adaptive delta

**Status:** success  
**Duration:** 164.3s  
**Finished:** 2026-07-11T02:17:00+00:00  

## Data

| accdist_soldiers | bb_rows | enriched_soldiers | extreme_rows | live_after_s | macd_rows | n_fails | n_warns | need_fresh | pe_soldiers | peg_sample | peg_soldiers | rank_days_rows | rank_delta_rows | rank_note | rr_debug | rr_sample | rr_soldiers | rsi_rows | sample_rd | sma20_rows | soldier_sample | squeezes | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 40 | 40 | ADAPTIVE: rank delta measured over the 3 sessions accrued so far; converges to 20d at 21 sessions (4/21) |  |  |  |  | [{"etf": "SMH", "rd": 12, "d": 3}, {"etf": "CIBR", "rd": 0, "d": 3}, {"etf": "XLK", "rd": 12, "d": 3}, {"etf": "XBI", "rd": -3, "d": 3}] |  |  |  |  |
|  |  | 54 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | [{"ticker": "NVDA", "weight_pct": 19.79, "whale_musd": -4428, "fwd_pe": 16.7, "eps_cagr_pct": 20.5, "peg_fwd": 0.81, "rr": {"up_pct": 12.1, "down_pct": 1.0, "ratio": 12.1, "stop_basis": "50DMA"}}, {"ticker": "TSM", "weight_pct": 9.42, "whal |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"n_quotes": 375, "fmp_key_set": true, "first": {"t": "NVDA", "price": 210.96, "pa50": 209.2432}} |  |  |  |  |  |  |  |  |
| 4 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | [{"t": "NVDA", "up_pct": 12.1, "down_pct": 1.0, "ratio": 12.1, "stop_basis": "50DMA"}, {"t": "TSM", "up_pct": 10.3, "down_pct": 2.6, "ratio": 4.0, "stop_basis": "50DMA"}, {"t": "AVGO", "up_pct": 23.8, "down_pct": 10.4, "ratio": 2.3, "stop_basis": "200DMA"}] | 60 |  |  | 40 |  |  |  |
|  | 40 |  | 23 |  | 40 |  |  |  | 49 | [{"t": "NVDA", "pe": null, "fpe": 16.7, "g": 20.5, "peg": 0.81}, {"t": "TSM", "pe": null, "fpe": 0.9, "g": 9.7, "peg": 0.09}, {"t": "AVGO", "pe": null, "fpe": 20.6, "g": 21.7, "peg": 0.95}, {"t": "AMD", "pe": null, "fpe": 42.0, "g": 31.1, "peg": 1.35}] | 47 |  |  |  |  |  |  | 40 |  |  |  | 9 |  |
|  |  |  |  | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | 0 | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |

## Log
## 1. Page live (this-push marker)

## 2. Rail hidden-by-default (site-wide)

## verdict

- `02:17:00` PASS
