## 0. Engine v3.5: invoke + adaptive delta

**Status:** success  
**Duration:** 202.0s  
**Finished:** 2026-07-11T02:52:39+00:00  

## Data

| accdist_soldiers | bb_rows | enriched_soldiers | extreme_rows | fin_sample | fin_soldiers | letters | live_after_s | macd_rows | n_fails | n_warns | need_fresh | nvda | nvda_fin | pe_soldiers | peg_sample | peg_soldiers | rank_days_rows | rank_delta_rows | rank_note | rr_debug | rr_sample | rr_soldiers | rsi_rows | sample_rd | sma20_rows | soldier_sample | squeezes | tsm | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 40 | 40 | ADAPTIVE: rank delta measured over the 3 sessions accrued so far; converges to 20d at 21 sessions (4/21) |  |  |  |  | [{"etf": "SMH", "rd": 12, "d": 3}, {"etf": "CIBR", "rd": 0, "d": 3}, {"etf": "XLK", "rd": 12, "d": 3}, {"etf": "XBI", "rd": -3, "d": 3}] |  |  |  |  |  |
|  |  | 54 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | [{"ticker": "NVDA", "weight_pct": 19.79, "whale_musd": -4428, "pe": 32.2, "fwd_pe": 16.7, "fwd_pe_basis": "estimates", "eps_cagr_pct": 20.5, "peg_fwd": 0.81, "rr": {"up_pct": 12.1, "down_pct": 1.0, "ratio": 12.1, "stop_basis": "50DMA"}, "fi |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"n_quotes": 375, "fmp_key_set": true, "first": {"t": "NVDA", "price": 210.96, "pa50": 209.2432}} |  |  |  |  |  |  |  |  |  |
| 4 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | [{"t": "NVDA", "up_pct": 12.1, "down_pct": 1.0, "ratio": 12.1, "stop_basis": "50DMA"}, {"t": "TSM", "up_pct": 10.3, "down_pct": 2.6, "ratio": 4.0, "stop_basis": "50DMA"}, {"t": "AVGO", "up_pct": 23.8, "down_pct": 10.4, "ratio": 2.3, "stop_basis": "200DMA"}] | 60 |  |  | 40 |  |  |  |  |
|  | 40 |  | 23 |  |  |  |  | 40 |  |  |  |  |  | 47 | [{"t": "NVDA", "pe": 32.2, "fpe": 16.7, "g": 20.5, "peg": 0.81}, {"t": "TSM", "pe": 32.5, "fpe": 29.6, "g": 9.7, "peg": 3.05}, {"t": "AVGO", "pe": 64.6, "fpe": 20.6, "g": 21.7, "peg": 0.95}, {"t": "AMD", "pe": 181.1, "fpe": 42.0, "g": 31.1, "peg": 1.35}] | 45 |  |  |  |  |  |  | 40 |  |  |  | 9 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | {"pe": 32.2, "fwd_pe": 16.7, "peg_fwd": 0.81} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"pe": 32.5, "fwd_pe": 29.6, "fwd_pe_basis": "normalized", "eps_cagr_pct": 9.7, "peg_fwd": 3.05} |  |
|  |  |  |  | [{"t": "NVDA", "grade": "A+", "score": 85, "f_score": 7, "z": 51.231105001843154, "rev_g": 65.5, "sh_g": -0.8}, {"t": "TSM", "grade": "A+", "score": 95, "f_score": 8, "z": 18.541766366406478, "rev_g": 33.0, "sh_g": 0.0}, {"t": "AVGO", "grade": "A", "score": 81, "f_score": 6, "z": 13.415980371648137, "rev_g": 23.9, "sh_g": 1.9}, {"t": "AMD | 60 | ["A", "A+", "A-", "B", "B+", "B-", "C", "C+", "C-", "D"] |  |  |  |  |  |  | {"grade": "A+", "score": 85, "f_score": 7, "z": 51.231105001843154, "z_zone": "SAFE", "rev_g": 65.5, "ni_g": 64.7, "sh_g": -0.8, "as_of": "2026-07-11"} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 0 | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PASS |

## Log
## 0b. FIN grades (v4.0)

## 1. Page live (this-push marker)

## 2. Rail hidden-by-default (site-wide)

## verdict

- `02:52:39` PASS
