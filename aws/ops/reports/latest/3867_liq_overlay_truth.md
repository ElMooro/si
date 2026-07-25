# ops 3867 — liquidity overlay 0/25: stale feed, neutral regime, or defect

**Status:** failure  
**Duration:** 0.4s  
**Finished:** 2026-07-25T16:52:05+00:00  

## Error

```
SystemExit: 1
```

## Data

| dollar_shortage | feed_age_h | gate_max_age_h | heading | n_missing | n_stale_excluded | ranker_age_h | regime | score |
|---|---|---|---|---|---|---|---|---|
|  |  |  |  | 0 | 0 | 0.1 |  |  |
| CALM | 5.6 | 48 | STABLE / MIXED |  |  |  | None | None |

## Log
## 1. ask the ranker what it excluded

- `16:52:05`   stale_feeds_excluded: []
- `16:52:05`   missing_feeds: []
- `16:52:05` ✅   liquidity feed is NOT in the ranker's exclusion lists
## 2. the feed itself

- `16:52:05`   top-level keys: ['analogs', 'availability', 'backtest', 'brain_predictors', 'china', 'china_engine', 'composite', 'composite_clock', 'composite_projection', 'composite_snapshots', 'credit', 'cycle_clock', 'data_health', 'dealer_survey', 'dollar', 'dollar_shortage']
## 3. verdict

- `16:52:05` ✗   CAUSE (a'): feed is fresh (5.6h) but the score field the overlay reads is None — a key-contract drift in the producer. Grep the producer before changing the consumer.
