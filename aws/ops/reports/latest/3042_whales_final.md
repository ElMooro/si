## 1. Ensure engine

**Status:** failure  
**Duration:** 27.3s  
**Finished:** 2026-07-10T03:50:36+00:00  

## Error

```
SystemExit: 1
```

## Data

| berkshire | citadel_positions | failed | fn_exists | fresh_n | max_single_flow | n_fails | n_warns | page_live | quarter | schedule | status | stocks_moved | top_inflow | top_outflow | verdict | whales_ok |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | True |  |  |  |  |  |  |  | Successful |  |  |  |  |  |
| {"quarter": "2026Q1", "n_positions": 29, "total_value_usd": 263095703570, "n_moves": 29} |  | [] |  | 20 |  |  |  |  | 2026Q1 |  |  | 5565 | [{"GOOGL": 9068108995}, {"MU": 3787394705}, {"AAPL": 2922553686}, {"DAL": 2735985503}, {"USO": 1899556060}] | [{"CVX": -8838577741}, {"NFLX": -5000497983}, {"TSLA": -4720595884}, {"NVDA": -4428060333}, {"LRCX": -4247554467}] |  | 33 |
|  | 5970 |  |  |  | 9068108995 |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | exists |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | 1 | 0 |  |  |  |  |  |  |  | FAIL |  |

## Log
## 2. Run the diff (Event + poll)

## 3. Weekly schedule

## 4. Live page (warn-level)

## verdict

- `03:50:36` FAIL: Citadel positions=5970 (pagination suspect)
