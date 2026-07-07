- `01:36:32` ⚠ DEAD referenced feed cot/extremes/current.json (http=None) <- /cot-extremes.html,/desk-v2.html,/desk.html
- `01:36:32` ⚠ DEAD referenced feed crypto-intel.json (http=None) <- /desk-v2.html
**Status:** failure  
**Duration:** 232.2s  
**Finished:** 2026-07-07T01:38:53+00:00  

## Error

```
SystemExit: 1
```

## Data

| engines_aging_8d | engines_dead | engines_fresh_26h | engines_no_outs | engines_stale | engines_total | feeds_live | feeds_referenced | feeds_unparseable | homepage | nav_pages | pages_down | pages_ok | summary |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  | 366 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | /asymmetric.html,/catalysts.html,/charts.html,/options-flow.html,/positioning.html | 361/366 |  |
|  |  |  |  |  |  |  | 419 |  |  |  |  |  |  |
|  |  |  |  |  |  | 396/419 |  | 0 |  |  |  |  |  |
| 14 | 6 | 555 | 82 | 4 | 661 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | operator-console+amber |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | pages=361/366 ref-feeds=396/419 engines fresh=555 aging=14 stale=4 dead=6 no-outs=82 t=232s |

## Log
- `01:36:32` ⚠ DEAD referenced feed divergence/current.json (http=None) <- /desk-v2.html,/desk.html,/news.html
- `01:36:32` ⚠ DEAD referenced feed edgar_insiders.json (http=None) <- /analytics.html
- `01:36:32` ⚠ DEAD referenced feed edge-data.json (http=None) <- /edge.html
- `01:36:32` ⚠ DEAD referenced feed equity_research.json (http=None) <- /analytics.html
- `01:36:32` ⚠ DEAD referenced feed flow-data.json (http=None) <- /flow.html,/vol.html
- `01:36:32` ⚠ DEAD referenced feed ici-flows.json (http=None) <- /ici-flows.html
- `01:36:32` ⚠ DEAD referenced feed intelligence-report.json (http=None) <- /desk-v2.html
- `01:36:32` ⚠ DEAD referenced feed investor-debate/_index.json (http=None) <- /desk-v2.html,/desk.html
- `01:36:32` ⚠ DEAD referenced feed justhodl-dashboard-live/data/macro-nowcast.json (http=None) <- /chart-macro.html
- `01:36:32` ⚠ DEAD referenced feed liquidity-data.json (http=None) <- /liquidity.html
- `01:36:32` ⚠ DEAD referenced feed opportunities/asymmetric-equity.json (http=None) <- /desk-v2.html,/desk.html
- `01:36:32` ⚠ DEAD referenced feed portfolio/pnl-daily.json (http=None) <- /desk-v2.html,/desk.html
- `01:36:32` ⚠ DEAD referenced feed portfolio/signal-portfolio-state.json (http=None) <- /ticker.html
- `01:36:32` ⚠ DEAD referenced feed portfolio/sizer-v2.json (http=None) <- /position-sizer.html
- `01:36:32` ⚠ DEAD referenced feed predictions.json (http=None) <- /ml-predictions.html,/trading-signals.html
- `01:36:32` ⚠ DEAD referenced feed regime/current.json (http=None) <- /bonds.html,/desk-v2.html,/desk.html
- `01:36:32` ⚠ DEAD referenced feed repo-data.json (http=None) <- /repo.html
- `01:36:32` ⚠ DEAD referenced feed reports/scorecard.json (http=None) <- /reports.html
- `01:36:32` ⚠ DEAD referenced feed research_critique.json (http=None) <- /analytics.html
- `01:36:32` ⚠ DEAD referenced feed risk/recommendations.json (http=None) <- /desk-v2.html,/desk.html
- `01:36:32` ⚠ DEAD referenced feed valuations-data.json (http=None) <- /valuations-macro.html
- `01:38:53` ⚠ dead engines: justhodl-engine-robustness,justhodl-eurostat-history,justhodl-ici-flows,justhodl-kill-switch,justhodl-transcript-indexer,justhodl-transcript-query
- `01:38:53` stale engines (>8d): justhodl-feedback,justhodl-history-api,justhodl-trade-journal,justhodl-watchlist
- `01:38:53` ✗ pages down: ['/asymmetric.html', '/catalysts.html', '/charts.html', '/options-flow.html', '/positioning.html']
- `01:38:53` ✗ 23 referenced feeds dead
