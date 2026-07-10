## 1. Radar 1.3.1 (ma_state provider)

**Status:** failure  
**Duration:** 156.6s  
**Finished:** 2026-07-10T14:38:20+00:00  

## Error

```
SystemExit: 1
```

## Data

| breadth_rows | breadth_sample | ir_version | ma_state_n | n_fails | n_warns | page_breadth | page_rrg | quadrants | radar_v | rrg_n | trails_ok | transitions | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | 487 |  |  |  |  |  | 1.3.1 |  |  |  |  |
| 22 | [{"etf": "SMH", "pct_above_50d": 67, "pct_above_200d": 100, "n_covered": 12, "n_holdings": 12, "read": "MIXED"}, {"etf": "CIBR", "pct_above_50d": 67, "pct_above_200d": 89, "n_covered": 9, "n_holdings": 12, "read": "MIXED"}, {"etf": "XLK", "pct_above_50d": 58, "pct_above_200d": 83, "n_covered": 12, "n_holdings": 12, "read": "MIXED"}, {"etf": "IYT", "pct_above_50d": 88, "pct_above_200d": 75, "n_covered": 8, "n_holdings": 12, "read": "HEALTHY"}] | 3.1 |  |  |  |  |  | {"WEAKENING": 3, "LEADING": 16, "IMPROVING": 12, "LAGGING": 9} |  | 40 | 40 | 0 |  |
|  |  |  |  |  |  | True | True |  |  |  |  |  |  |
|  |  |  |  | 1 | 1 |  |  |  |  |  |  |  | FAIL |

## Log
## 2. IR 3.1 (RRG + transitions + breadth)

## 3. Page (warn-level)

## verdict

- `14:38:20` FAIL: breadth on 22 rows (<28)
