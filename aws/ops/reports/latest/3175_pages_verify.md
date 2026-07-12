# ops 3175 — do his pages actually show his data?

**Status:** success  
**Duration:** 122.7s  
**Finished:** 2026-07-12T23:12:08+00:00  

## Error

```
SystemExit: 0
```

## Data

| markers | n_fails | n_warns | symbol-map_json | thesis-engine_json | tv-watchlists_json | verdict | watchlists_http |
|---|---|---|---|---|---|---|---|
| True |  |  |  |  |  |  | 200 |
|  |  |  |  |  | 207 |  |  |
|  |  |  | 2895 |  |  |  |  |
|  |  |  |  | 31 |  |  |  |
|  | 0 | 0 |  |  |  | PASS |  |

## Log
## 1. watchlists.html (the TradingView mirror)

- `23:10:05` ✅ page served and wired to the watchlists feed
## 2. theses.html (board fix)

- `23:12:06` ✅ board-shadowing bug gone from the served page; watchlist link present
## 3. The feeds the pages read (via the proxy path)

- `23:12:06` ✅ data/tv-watchlists.json: 207 lists
- `23:12:07` ✅ data/symbol-map.json: 2895 map
- `23:12:08` ✅ data/thesis-engine.json: 31 theses
## 4. What he should SEE

- `23:12:08` theses.html should list 31 rows — e.g.:
- `23:12:08`   · Consumers                                    act  57.1% t= -1.62 since 1993-42
- `23:12:08`   · Financial Conditions                         act   8.3% t= -1.23 since 1993-42
- `23:12:08`   · Red list                                     act   9.4% t=  1.17 since 1993-34
- `23:12:08`   · 71699273                                     act  30.0% t= -1.09 since 1993-42
- `23:12:08`   · Chicago Financial conditions                 act   8.3% t= -0.92 since 1993-34
- `23:12:08`   · Economy                                      act  35.3% t= -0.92 since 1993-34
- `23:12:08`   · Draining Liquidity                           act   0.0% t=  0.91 since 2014-16
- `23:12:08`   · Financial Crisis Signs                       act  11.8% t=  -0.9 since 1993-34
- `23:12:08` watchlists.html should list 207 lists — e.g.:
- `23:12:08`   · 71699273                                     500 indicators
- `23:12:08`   · 82604570                                     500 indicators
- `23:12:08`   · Black Swan Event                             500 indicators
- `23:12:08`   · Bottom Indicators                            500 indicators
- `23:12:08`   · FTSE                                         500 indicators
- `23:12:08`   · Red list                                     500 indicators
