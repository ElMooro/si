# ops 3868 — liquidity overlay 0/25: stale feed, neutral regime, or defect

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-07-25T16:55:48+00:00  

## Data

| dollar_shortage | feed_age_h | gate_max_age_h | heading | n_missing | n_stale_excluded | ranker_age_h | regime | score |
|---|---|---|---|---|---|---|---|---|
|  |  |  |  | 0 | 0 | 0.16 |  |  |
| CALM | 5.7 | 48 | STABLE / MIXED |  |  |  | NEUTRAL | 50.5 |

## Log
## 1. ask the ranker what it excluded

- `16:55:48`   stale_feeds_excluded: []
- `16:55:48`   missing_feeds: []
- `16:55:48` ✅   liquidity feed is NOT in the ranker's exclusion lists
## 2. the feed itself

- `16:55:48`   composite keys: ['components', 'composite_z', 'liquidity_score', 'n_components', 'read', 'regime']
- `16:55:48`   top-level keys: ['analogs', 'availability', 'backtest', 'brain_predictors', 'china', 'china_engine', 'composite', 'composite_clock', 'composite_projection', 'composite_snapshots', 'credit', 'cycle_clock', 'data_health', 'dealer_survey', 'dollar', 'dollar_shortage']
## 3. verdict

- `16:55:48` ✅   CAUSE (b): feed fresh (5.7h), regime=NEUTRAL, heading=STABLE / MIXED, dollar_shortage=CALM — every multiplier collapses to 1.0, so 0/25 is CORRECT. No change warranted; manufacturing a tilt here would be the same false-signal error as forcing RORO at score 2.5.
