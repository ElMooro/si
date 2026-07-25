# ops 3863 — PROBE: master-ranker sector resolution, live (no code written)

**Status:** failure  
**Duration:** 2.5s  
**Finished:** 2026-07-25T16:36:34+00:00  

## Error

```
SystemExit: 1
```

## Data

| age_hours | generated_at | map_size | n_rows | ranked | resolved | unresolved |
|---|---|---|---|---|---|---|
| 17.8 | None |  | 25 |  |  |  |
|  |  | 521 |  | 25 | 17 | 8 |

## Log
## 1. live master-ranker output

- `16:36:32`   list-bearing keys: ['feed_freshness', 'missing_feeds', 'stale_feeds_excluded', 'top_macro', 'top_tickers']
- `16:36:32`   row keys: ['capital_flow_mult', 'capture_gap', 'capture_tier', 'catchup_basis', 'catchup_pct', 'census', 'contributions', 'cycle_phase', 'cycle_warning', 'details', 'global_capture_gap', 'liquidity_regime_mult', 'mcap_share_pct', 'n_systems', 'nowcast_regime_mult', 'rationale', 'red_flags', 'risk_regime_mult', 'rotation_mult', 'rotation_note', 'score', 'systems', 'ticker', 'undervaluation_score']
## 2. overlay coverage — how many rows actually got a sector

- `16:36:32` ✅   rotation_mult            non-neutral on 16/25
- `16:36:32` ✗   risk_regime_mult         non-neutral on 0/25
- `16:36:32` ✅   liquidity_regime_mult    non-neutral on 16/25
- `16:36:32` ✅   nowcast_regime_mult      non-neutral on 16/25
- `16:36:32`   rotation_note present on 16/25
## 3. rebuild the harvest from the same donors — per-donor attribution

- `16:36:32` ✅   screener/data.json                    503 pairs · covers 15/25 ranked · 22h old
- `16:36:33` ✗   data/capital-flow-radar.json            0 pairs · covers  0/25 ranked · 18h old
- `16:36:33` ✅   data/deep-value.json                   30 pairs · covers  2/25 ranked · 6h old
- `16:36:33` ✗   data/accumulation-radar.json            0 pairs · covers  0/25 ranked · 19h old
- `16:36:33` ✅   data/asymmetric-scorer.json            43 pairs · covers  0/25 ranked · 6h old
## 4. TRUE overlap — the number that decides the next move

- `16:36:33`   unresolved tickers: ['BE', 'UMC', 'DFTX', 'OVV', 'HCC', 'ALHC', 'IPGP', 'CENX']
- `16:36:33` ✗   17/25 ranked tickers resolvable to a sector
## 5. did the deployed engine log its map size on the last run

- `16:36:33` ✅   [sector-map] harvested 513 ticker->sector pairs
- `16:36:34` ✅   [sector-map] harvested 513 ticker->sector pairs
## 6. verdict

- `16:36:34` ✗ OPEN — resolvable 17/25, rotation non-neutral 16/25
