# ops 3862 — PROBE: master-ranker sector resolution, live (no code written)

**Status:** failure  
**Duration:** 2.8s  
**Finished:** 2026-07-25T16:32:37+00:00  

## Error

```
SystemExit: 1
```

## Data

| age_hours | generated_at | map_size | n_rows | ranked | resolved | unresolved |
|---|---|---|---|---|---|---|
| 17.7 | None |  | 63 |  |  |  |
|  |  | 521 |  | 0 | 0 | 0 |

## Log
## 1. live master-ranker output

- `16:32:35`   list-bearing keys: ['feed_freshness', 'missing_feeds', 'stale_feeds_excluded', 'top_macro', 'top_tickers']
## 2. overlay coverage — how many rows actually got a sector

- `16:32:35` ✗   rotation_mult            non-neutral on 0/63
- `16:32:35` ✗   risk_regime_mult         non-neutral on 0/63
- `16:32:35` ✗   liquidity_regime_mult    non-neutral on 0/63
- `16:32:35` ✗   nowcast_regime_mult      non-neutral on 0/63
- `16:32:35`   rotation_note present on 0/63
## 3. rebuild the harvest from the same donors — per-donor attribution

- `16:32:35` ✅   screener/data.json                    503 pairs · covers  0/0 ranked · 22h old
- `16:32:36` ✗   data/capital-flow-radar.json            0 pairs · covers  0/0 ranked · 18h old
- `16:32:36` ✅   data/deep-value.json                   30 pairs · covers  0/0 ranked · 6h old
- `16:32:36` ✗   data/accumulation-radar.json            0 pairs · covers  0/0 ranked · 19h old
- `16:32:36` ✅   data/asymmetric-scorer.json            43 pairs · covers  0/0 ranked · 6h old
## 4. TRUE overlap — the number that decides the next move

- `16:32:36`   unresolved tickers: []
- `16:32:36` ✅   0/0 ranked tickers resolvable to a sector
## 5. did the deployed engine log its map size on the last run

- `16:32:37` ✅   [sector-map] harvested 513 ticker->sector pairs
- `16:32:37` ✅   [sector-map] harvested 513 ticker->sector pairs
## 6. verdict

- `16:32:37` ✗ OPEN — resolvable 0/0, rotation non-neutral 0/63
