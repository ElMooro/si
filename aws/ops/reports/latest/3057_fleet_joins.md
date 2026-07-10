## 1. Radar ma_state (already live from 3054)

**Status:** failure  
**Duration:** 269.5s  
**Finished:** 2026-07-10T15:12:30+00:00  

## Error

```
SystemExit: 1
```

## Data

| apac_sample | appetite | breadth_rows | breadth_sample | cycle | ew_pairs | ew_sample | ir_v | ir_version | join_hits | ma_events | ma_sample | ma_state_n | n_fails | n_warns | page_breadth | page_rrg | quadrants | radar_v | rrg_n | trails_ok | transitions | verdict | wyckoff_sample |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  | 487 |  |  |  |  |  | 1.3.1 |  |  |  |  |  |
|  |  | 36 | [{"etf": "SMH", "pct_above_50d": 67, "pct_above_200d": 100, "n_covered": 12, "n_holdings": 12, "read": "MIXED"}, {"etf": "CIBR", "pct_above_50d": 70, "pct_above_200d": 90, "n_covered": 10, "n_holdings": 12, "read": "HEALTHY"}, {"etf": "XLK", "pct_above_50d": 58, "pct_above_200d": 83, "n_covered": 12, "n_holdings": 12, "read": "MIXED"}, {"etf": "XBI", "pct_above_50d": 100, "pct_above_200d": 100, "n_covered": 12, "n_holdings": 12, "read": "HEALTHY"}] |  |  |  |  | 3.3 |  |  |  |  |  |  |  |  | {"WEAKENING": 4, "LEADING": 15, "IMPROVING": 12, "LAGGING": 9} |  | 40 | 40 | 0 |  |  |
|  |  |  |  |  | 11 | {"XLK": {"ew": "RSPT", "ew_cw_20d_pct": 0.28, "above_63d_base": true, "read": "BROAD"}, "XLF": {"ew": "RSPF", "ew_cw_20d_pct": 0.49, "above_63d_base": true, "read": "BROAD"}, "XLV": {"ew": "RSPH", "ew_cw_20d_pct": 1.03, "above_63d_base": true, "read": "BROAD"}, "XLY": {"ew": "RSPD", "ew_cw_20d_pct": -0.26, "above_63d_base": true, "read": "BROAD"}} | 3.3 |  |  | 7 | [{"etf": "XLF", "date": "2026-07-10", "event": "GOLDEN_CROSS", "bullish": true}, {"etf": "XLV", "date": "2026-07-10", "event": "GOLDEN_CROSS", "bullish": true}, {"etf": "XLY", "date": "2026-07-10", "event": "CROSS_ABOVE_50D", "bullish": true}, {"etf": "XLY", "date": "2026-07-10", "event": "CROSS_ABOVE_200D", "bullish": true}, {"etf": "COPX", "date": "2026-07-10", "event": "CROSS_ABOVE_200D", "bullish": true}] |  |  |  |  |  |  |  |  |  |  |  |  |
| [] | {"xly_xlp": 1.399, "read": "RISK_ON", "vs_126d_ma_pct": 0.72, "factor_regime_z": null} |  |  | {"phase_raw": "LATE CYCLE", "phase_bucket": "LATE", "expected_leaders": ["XLE", "XLB", "XLP", "XLV", "XLU", "XES", "OIH", "XOP", "XME"], "actual_top8": ["SMH", "CIBR", "XLK", "XBI", "IYT", "XLV", "IBB", "KRE"], "aligned": ["XLV"], "anomalies": ["SMH", "CIBR", "XLK", "XBI", "IYT", "IBB", "KRE"]} |  |  |  |  | {"wyckoff": 2, "whales": 27, "dark_pool": 0, "capital_flow": 0, "options": 0, "rev_hits": 0} |  |  |  |  |  |  |  |  |  |  |  |  |  | [{"etf": "SMH", "phase": "DISTRIBUTION", "begin": "2026-06-05"}, {"etf": "COPX", "phase": "NEUTRAL_RANGE", "begin": "2026-01-26"}] |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True | True |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | 1 | 6 |  |  |  |  |  |  |  | FAIL |  |

## Log
## 2. IR 3.1 (RRG + transitions + breadth)

## 2b. Arc B: EW/CW + MA events

## 2c. Arc C: six fleet joins

## 3. Page (warn-level)

## verdict

- `15:12:30` FAIL: wyckoff join=2 (<10)
