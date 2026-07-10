## 1. AAPL depth matrix

**Status:** success  
**Duration:** 0.9s  
**Finished:** 2026-07-10T18:29:55+00:00  

## Data

| aapl_10y_1wk | aapl_20y_1wk | aapl_max_1d | aapl_max_1mo | aapl_max_1wk | deepest | n_fails | n_warns | verdict |
|---|---|---|---|---|---|---|---|---|
|  |  |  |  | {"rng": "max", "iv": "1wk", "n": 168, "first": "1984-12-01", "err": ""} |  |  |  |  |
|  |  |  | {"rng": "max", "iv": "1mo", "n": 168, "first": "1984-12-01", "err": ""} |  |  |  |  |  |
|  |  | {"rng": "max", "iv": "1d", "n": 168, "first": "1984-12-01", "err": ""} |  |  |  |  |  |  |
| {"rng": "10y", "iv": "1wk", "n": 523, "first": "2016-07-11", "err": ""} |  |  |  |  |  |  |  |  |
|  | {"rng": "20y", "iv": "1wk", "n": 1045, "first": "2006-07-10", "err": ""} |  |  |  |  |  |  |  |
|  |  |  |  |  | {"rng": "max", "iv": "1wk", "n": 168, "first": "1984-12-01", "err": ""} |  |  |  |
|  |  |  |  |  |  | 0 | 0 | PASS |

## Log
## verdict

- `18:29:55` PASS -- deep history reachable; wire page to the deepest combo
