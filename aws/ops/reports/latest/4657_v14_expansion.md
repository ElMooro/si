# ops 4657 — v1.4.0 expansion

**Status:** success  
**Duration:** 154.8s  
**Finished:** 2026-08-14T01:55:46+00:00  

## Data

| caps | f13 | f13_n | fc3 | fcfy | fn_error | golden | imom | inv_rev | ladder | nbb | ps | sp500 | warned | whale_rule |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | 6522 |  |  |  |  |  |  |  |  |  |  |  | clone-alpha skill_score>=55, else top-3 positive-skill funds |
|  |  |  |  |  | None |  |  |  |  |  |  |  |  |  |
|  | 465 |  | 295 | 300 |  | 176 |  | 300 | 257 | 300 | 300 |  | 155 |  |
| {"micro": 300} |  |  |  |  |  |  | 290 |  |  |  |  | 300 |  |  |

## Log
## evidence: 13F 't' sample + options discovery

- `01:53:12` 13F t[AAPL] = {"b": 3568974207, "s": 9007628327, "n": -5438654120, "wb": 794441234, "ws": 0, "wn": 794441234, "nf": 14, "na": 4, "tv": 78650336796, "fb": ["Two Sigma", "Duration Capital", "Renaissance Technologies"], "fs": ["Citadel Advisors", "Berkshire Hathaway"]}
- `01:54:21` options stores: ['data/crypto-options-history.json', 'data/crypto-options-surface-history.json', 'data/crypto-options-surface.json', 'data/crypto-options.json', 'data/engines/wl-fear-greed-in-mkt-put-call-ratio.json', 'data/engines/wl-index-option-settlement.json', 'data/options-analytics-iv-history.json', 'data/options-analytics.json']
## deploy + settle

- `01:54:22` ✅   [deploy] v1.5.0 live
## run + expansion truth

- `01:54:46` WARN GPN   ['EPS_CONTRACTION -147.6%']
- `01:54:46` WARN ZBH   ['DOUBLE_TOP']
- `01:54:46` WARN COF   ['MAJOR_DILUTION +22.8%/yr']
- `01:54:46` WARN ABT   ['EPS_CONTRACTION -61.0%']
- `01:54:46` WARN DAL   ['DOUBLE_TOP']
- `01:54:46` league row keys: ['boom_score', 'comp', 'coverage_w', 'industry', 'mcap_b', 'n', 'n_component_families', 'score_delta_20d', 'sector', 'top_names']
- `01:54:46` ✅   [ind-mom] 290 rows carry industry momentum
- `01:54:46` ✅   [caps] cap buckets live: {'micro': 300}
- `01:54:46` ✅   [ladder] 257/300-lane rows carry SMA ladder; 176 golden
- `01:54:46` ✅   [binds] census binds live (nbb/ps/fcfy/fc3/inv >= 200 each)
- `01:54:46` ✅   [warnings] 155 rows carry red-flag warnings
- `01:54:46` ✅   [f13-join] 13F flows joined on 465 rows
## edge (page v5)

- `01:55:46` ✅   [page] v5 structural tokens at edge
## verdict

- `01:55:46` ✅ V1.4 EXPANSION LIVE — ladder+GC, warnings, census binds, cat6 badges, 13F join, engine dropdown · stock-buying.html
