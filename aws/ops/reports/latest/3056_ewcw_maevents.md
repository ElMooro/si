## 1. Radar ma_state (already live from 3054)

**Status:** success  
**Duration:** 149.6s  
**Finished:** 2026-07-10T14:50:39+00:00  

## Data

| breadth_rows | breadth_sample | ew_pairs | ew_sample | ir_v | ir_version | ma_events | ma_sample | ma_state_n | n_fails | n_warns | page_breadth | page_rrg | quadrants | radar_v | rrg_n | trails_ok | transitions | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | 487 |  |  |  |  |  | 1.3.1 |  |  |  |  |
| 36 | [{"etf": "SMH", "pct_above_50d": 67, "pct_above_200d": 100, "n_covered": 12, "n_holdings": 12, "read": "MIXED"}, {"etf": "CIBR", "pct_above_50d": 70, "pct_above_200d": 90, "n_covered": 10, "n_holdings": 12, "read": "HEALTHY"}, {"etf": "XLK", "pct_above_50d": 58, "pct_above_200d": 83, "n_covered": 12, "n_holdings": 12, "read": "MIXED"}, {"etf": "XBI", "pct_above_50d": 92, "pct_above_200d": 100, "n_covered": 12, "n_holdings": 12, "read": "HEALTHY"}] |  |  |  | 3.2 |  |  |  |  |  |  |  | {"WEAKENING": 4, "LEADING": 15, "IMPROVING": 12, "LAGGING": 9} |  | 40 | 40 | 1 |  |
|  |  | 11 | {"XLK": {"ew": "RSPT", "ew_cw_20d_pct": 0.56, "above_63d_base": true, "read": "BROAD"}, "XLF": {"ew": "RSPF", "ew_cw_20d_pct": 0.97, "above_63d_base": true, "read": "BROAD"}, "XLV": {"ew": "RSPH", "ew_cw_20d_pct": 1.15, "above_63d_base": true, "read": "BROAD"}, "XLY": {"ew": "RSPD", "ew_cw_20d_pct": -0.02, "above_63d_base": true, "read": "BROAD"}} | 3.2 |  | 4 | [{"etf": "XLF", "date": "2026-07-10", "event": "GOLDEN_CROSS", "bullish": true}, {"etf": "XLV", "date": "2026-07-10", "event": "GOLDEN_CROSS", "bullish": true}, {"etf": "XAR", "date": "2026-07-10", "event": "CROSS_BELOW_50D", "bullish": false}, {"etf": "XAR", "date": "2026-07-10", "event": "CROSS_BELOW_100D", "bullish": false}] |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | True | True |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 0 | 0 |  |  |  |  |  |  |  | PASS |

## Log
## 2. IR 3.1 (RRG + transitions + breadth)

## 2b. Arc B: EW/CW + MA events

## 3. Page (warn-level)

## verdict

- `14:50:39` PASS -- Arc A live
