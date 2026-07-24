# ops 3787 — probe growth/cap fields (no code written)

**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-07-24T00:13:42+00:00  

## Data

| rows | version |
|---|---|
| 2811 | 4.1.1 |

## Log
## Row field inventory (union across all rows)

- `00:13:41`   backlog_accelerating             populated 2811/2811
- `00:13:41`   backlog_covered                  populated 2811/2811
- `00:13:41`   backlog_deferred_accel           populated 2811/2811
- `00:13:41`   cap_bucket                       populated 2811/2811
- `00:13:41`   capture_gap                      populated 2811/2811
- `00:13:41`   catchup_basis                    populated 2761/2811
- `00:13:41`   catchup_capped                   populated 2761/2811
- `00:13:41`   catchup_pct                      populated 2761/2811
- `00:13:41`   catchup_pct_evs                  populated 2759/2811
- `00:13:41`   catchup_pct_pe                   populated 2119/2811
- `00:13:41`   centrality                       populated 2811/2811
- `00:13:41`   criticality                      populated 2811/2811
- `00:13:41`   criticality_basis                populated 2811/2811
- `00:13:41`   criticality_pctile               populated 2811/2811
- `00:13:41`   dependency_pct                   populated 154/2811
- `00:13:41`   discount_to_fair_pct             populated 390/2811
- `00:13:41`   ev_sales                         populated 2811/2811
- `00:13:41`   gap_divergence                   populated 2811/2811
- `00:13:41`   global_capture_gap               populated 2811/2811
- `00:13:41`   global_criticality_pctile        populated 2811/2811
- `00:13:41`   global_mcap_pctile               populated 2811/2811
- `00:13:41`   gm_level                         populated 2811/2811
- `00:13:41`   gm_stability                     populated 2811/2811
- `00:13:41`   industry                         populated 2811/2811
- `00:13:41`   industry_mcap_total              populated 2811/2811
- `00:13:41`   industry_median_ev_sales         populated 2770/2811
- `00:13:41`   industry_median_pe               populated 2680/2811
- `00:13:41`   industry_peers                   populated 2811/2811
- `00:13:41`   is_chokepoint                    populated 2811/2811
- `00:13:41`   legs                             populated 2811/2811
- `00:13:41`   legs_available                   populated 2811/2811
- `00:13:41`   legs_why                         populated 2811/2811
- `00:13:41`   market_cap                       populated 2811/2811
- `00:13:41`   mcap_share_pct                   populated 2811/2811
- `00:13:41`   mcap_share_pctile                populated 2811/2811
- `00:13:41`   name                             populated 2811/2811
- `00:13:41`   pe                               populated 2212/2811
- `00:13:41`   revenue_coverage_pct             populated 2811/2811
- `00:13:41`   revenue_currency                 populated 1366/2811
- `00:13:41`   revenue_share_basis              populated 2811/2811
- `00:13:41`   revenue_share_pct                populated 1063/2811
- `00:13:41`   revenue_share_suppressed         populated 1748/2811
- `00:13:41`   revenue_ttm                      populated 1915/2811
- `00:13:41`   revenue_usd_coverage_pct         populated 2805/2811
- `00:13:41`   roic                             populated 2796/2811
- `00:13:41`   rpo_yoy                          populated 68/2811
- `00:13:41`   sector                           populated 2811/2811
- `00:13:41`   ticker                           populated 2811/2811
- `00:13:41`   tier                             populated 2811/2811
- `00:13:41`   undervaluation_score             populated 2811/2811
## Growth-related?

- `00:13:41`   growth-ish keys: ['rpo_yoy', 'backlog_deferred_accel', 'backlog_accelerating']
- `00:13:41` ✅ PROBE.growth_exists :: growth fields present
## Cap bucket values

- `00:13:41`   small        1094
- `00:13:41`   micro        729
- `00:13:41`   mid          652
- `00:13:41`   large        259
- `00:13:41`   mega         68
- `00:13:41`   nano         9
## Is there an S&P500 membership flag anywhere?

- `00:13:41`   candidates: NONE
## Feeds that could supply SP500 membership

- `00:13:41`   data/fundamental-census-matrix.json        OK top-keys=['generated_at', 'n_tickers', 'n_metrics', 'tickers', 'sectors', 'industries', 'quality', 'turn', 'flagged', 'metrics', 'cols']
- `00:13:41`   data/spx-ma.json                           OK top-keys=['engine', 'version', 'generated_at', 'index', 'breadth', 'siblings', 'methodology', 'disclaimer', 'elapsed_s']
- `00:13:42`   data/universe.json                         OK top-keys=['schema_version', 'method', 'generated_at', 'duration_s', 'stats', 'cap_buckets', 'stocks']
- `00:13:42` ✅ PASS_ALL — probe complete
