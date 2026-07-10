## 0. Phase-detector (publishes phases_all)

**Status:** success  
**Duration:** 174.3s  
**Finished:** 2026-07-10T15:32:30+00:00  

## Data

| apac_sample | appetite | breadth_rows | breadth_sample | cycle | ew_pairs | ew_sample | ir_v | ir_version | join_hits | ma_events | ma_sample | ma_state_n | n_fails | n_warns | page_breadth | page_rrg | phases_all_n | quadrants | radar_v | rrg_n | trails_ok | transitions | verdict | wyckoff_available | wyckoff_sample |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 683 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 487 |  |  |  |  |  |  | 1.3.1 |  |  |  |  |  |  |
|  |  | 30 | [{"etf": "IBB", "pct_above_50d": 100, "pct_above_200d": 88, "n_covered": 8, "n_holdings": 12, "read": "HEALTHY"}, {"etf": "KRE", "pct_above_50d": 100, "pct_above_200d": 100, "n_covered": 12, "n_holdings": 12, "read": "HEALTHY"}, {"etf": "KBE", "pct_above_50d": 100, "pct_above_200d": 92, "n_covered": 12, "n_holdings": 12, "read": "HEALTHY"}, {"etf": "ITA", "pct_above_50d": 57, "pct_above_200d": 86, "n_covered": 7, "n_holdings": 12, "read": "MIXED"}] |  |  |  |  | 3.3 |  |  |  |  |  |  |  |  |  | {"WEAKENING": 3, "LEADING": 16, "IMPROVING": 12, "LAGGING": 9} |  | 40 | 40 | 1 |  |  |  |
|  |  |  |  |  | 11 | {"XLK": {"ew": "RSPT", "ew_cw_20d_pct": 0.58, "above_63d_base": true, "read": "BROAD"}, "XLF": {"ew": "RSPF", "ew_cw_20d_pct": 0.6, "above_63d_base": true, "read": "BROAD"}, "XLV": {"ew": "RSPH", "ew_cw_20d_pct": 1.11, "above_63d_base": true, "read": "BROAD"}, "XLY": {"ew": "RSPD", "ew_cw_20d_pct": -0.05, "above_63d_base": true, "read": "BROAD"}} | 3.3 |  |  | 4 | [{"etf": "XLF", "date": "2026-07-10", "event": "GOLDEN_CROSS", "bullish": true}, {"etf": "XLV", "date": "2026-07-10", "event": "GOLDEN_CROSS", "bullish": true}, {"etf": "XAR", "date": "2026-07-10", "event": "CROSS_BELOW_50D", "bullish": false}, {"etf": "XAR", "date": "2026-07-10", "event": "CROSS_BELOW_100D", "bullish": false}] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| [[{"src": "Taiwan semis \u2192 SMH", "r": -0.3, "lead_days": 10, "note": "contrarian"}, {"src": "Korea memory \u2192 SMH", "r": 0.288, "lead_days": 5, "note": "follow-through"}]] | {"xly_xlp": 1.3979, "read": "RISK_ON", "vs_126d_ma_pct": 0.64, "factor_regime_z": null} |  |  | {"phase_raw": "LATE CYCLE", "phase_bucket": "LATE", "expected_leaders": ["XLE", "XLB", "XLP", "XLV", "XLU", "XES", "OIH", "XOP", "XME"], "actual_top8": ["SMH", "CIBR", "XLK", "XBI", "IYT", "XLV", "IBB", "KRE"], "aligned": ["XLV"], "anomalies": ["SMH", "CIBR", "XLK", "XBI", "IYT", "IBB", "KRE"]} |  |  |  |  | {"wyckoff": 2, "whales": 27, "dark_pool": 0, "capital_flow": 0, "options": 0, "rev_hits": 0} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | [{"etf": "SMH", "phase": "DISTRIBUTION", "begin": "2026-06-05"}, {"etf": "COPX", "phase": "NEUTRAL_RANGE", "begin": "2026-01-26"}] |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True | True |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | 0 | 5 |  |  |  |  |  |  |  |  | PASS |  |  |

## Log
## 1. Radar ma_state (already live from 3054)

## 2. IR 3.1 (RRG + transitions + breadth)

## 2b. Arc B: EW/CW + MA events

## 2c. Arc C: six fleet joins

## 3. Page (warn-level)

## verdict

- `15:32:30` PASS -- Arc A live
