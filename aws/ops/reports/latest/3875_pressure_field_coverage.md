# ops 3875 — PROBE: field-coverage audit, new stock fields vs served flows.html

**Status:** failure  
**Duration:** 1.4s  
**Finished:** 2026-07-25T18:40:47+00:00  

## Error

```
SystemExit: 1
```

## Data

| gaps | n_gaps | n_stock_fields | n_waived |
|---|---|---|---|
| ['flow_pct_mcap_21d', 'market_cap', 'perf_w_pct', 'perf_ytd_pct'] | 4 | 17 | 3 |

## Log
## 1. pull the served page's JS (word-boundary check, not naive substring)

- `18:40:46` ✅   buildUnifiedRows() found, 1100 chars
## 2. live per_stock_exposure — every field vs buildUnifiedRows' consumption

- `18:40:47` ✅   country                           1421/2247 populated · consumed in buildUnifiedRows
- `18:40:47`   cumulative_weight_pct             2247/2247 populated · WAIVED — internal aggregation detail (how much of the stock's float the holding-ETF set covers); not a flow signal itself
- `18:40:47` ✗   flow_pct_mcap_21d                 1244/2247 populated · NOT CONSUMED — open bug
- `18:40:47` ✅   flow_zscore_cross_sectional       1244/2247 populated · consumed in buildUnifiedRows
- `18:40:47`   holding_etfs                      2247/2247 populated · WAIVED — the detailed per-ETF breakdown already renders in the EXISTING Cross-ETF Constituent Pressure section above (per-stock lookup), not duplicated here
- `18:40:47` ✗   market_cap                        1244/2247 populated · NOT CONSUMED — open bug
- `18:40:47`   n_etfs_holding                    2247/2247 populated · WAIVED — internal — used to build holding_etfs, not a standalone metric worth its own column
- `18:40:47` ✅   name                              2247/2247 populated · consumed in buildUnifiedRows
- `18:40:47` ✅   perf_m_pct                        1420/2247 populated · consumed in buildUnifiedRows
- `18:40:47` ✗   perf_w_pct                        1421/2247 populated · NOT CONSUMED — open bug
- `18:40:47` ✗   perf_ytd_pct                      1421/2247 populated · NOT CONSUMED — open bug
- `18:40:47` ✅   quadrant                          2247/2247 populated · consumed in buildUnifiedRows
- `18:40:47` ✅   sector                            1421/2247 populated · consumed in buildUnifiedRows
- `18:40:47` ✅   stock                             2247/2247 populated · consumed in buildUnifiedRows
- `18:40:47` ✅   total_aggregate_flow_21d_usd      2247/2247 populated · consumed in buildUnifiedRows
- `18:40:47` ✅   total_aggregate_flow_5d_usd       2247/2247 populated · consumed in buildUnifiedRows
- `18:40:47` ✅   total_aggregate_flow_daily_usd    2247/2247 populated · consumed in buildUnifiedRows
## 3. same check for the top-level meta the engine added

- `18:40:47`   n_stocks_with_sector = 1421 — informational meta, not required to render (the master table's own filter/count UI surfaces the same information live: sector coverage is visible by using the sector filter, quadrant counts are visible in the heatmap)
- `18:40:47`   n_stocks_with_price_return = 1420 — informational meta, not required to render (the master table's own filter/count UI surfaces the same information live: sector coverage is visible by using the sector filter, quadrant counts are visible in the heatmap)
- `18:40:47`   n_stocks_with_flow_zscore = 1244 — informational meta, not required to render (the master table's own filter/count UI surfaces the same information live: sector coverage is visible by using the sector filter, quadrant counts are visible in the heatmap)
- `18:40:47`   quadrant_counts = {'STEALTH_ACCUMULATION': 30, 'DISTRIBUTION_RALLY': 30, 'TREND_CONFIRMED': 14, 'CAPITULATION': 59, 'NEUTRAL': 2114} — informational meta, not required to render (the master table's own filter/count UI surfaces the same information live: sector coverage is visible by using the sector filter, quadrant counts are visible in the heatmap)
## 4. verdict

- `18:40:47` ✗ OPEN BUGS 4: ['flow_pct_mcap_21d', 'market_cap', 'perf_w_pct', 'perf_ytd_pct']
