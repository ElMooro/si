## 1. Warroom v12 regen

**Status:** failure  
**Duration:** 63.1s  
**Finished:** 2026-07-10T02:55:43+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3038_grading_close.py", line 93, in main
    n_engines=len(engines_hit),
              ^^^^^^^^^^^^^^^^
TypeError: object of type 'int' has no len()

```

## Data

| earned | feed_ages_h | stale | top_picks |
|---|---|---|---|
| 39.5 | {"macro_grid": 2.4, "funding": 15.2, "leading_markets": 4.4, "dollar": 1.7, "vol": 2.9, "ciss": 19.7, "factor_regime": 5.4, "cftc": 13.9, "global_stress": 1.3, "plumbing": 4.4, "eurodollar": 14.9, "alerts": 12.2} | [] | [{"ticker": "SPY", "direction": "LONG", "score": 0.5, "note": "barometer risk-on band"}] |

## Log
## 2. Harvester ingest (proper verification)

