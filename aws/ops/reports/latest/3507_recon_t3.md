# ops 3507 — Tier-3 recon

**Status:** success  
**Duration:** 1.1s  
**Finished:** 2026-07-19T05:59:52+00:00  

## Log
- `05:59:51` FAIL  R1_master_ranker_schema — {'top_keys': ['alerts', 'as_of', 'calibration_weights', 'duration_s', 'feed_freshness', 'feed_health', 'method', 'missing_feeds', 'n_macro_signals', 'n_tickers_total', 'nowcast_regime', 'regime_context', 'risk_regime', 'schema_version', 'stale_feeds_excluded', 'top_macro', 'top_tickers', 'wl_research'], 'n_rows': 0, 'row_keys': [], 'nvda_row': None, 'sample': '[]'}
- `05:59:52` PASS  R2_freshness — {'sector_medians_lastmod': '2026-07-19 03:38:08+00:00', 'forensic_lastmod': '2026-07-18 17:26:48+00:00'}
- `05:59:52` PASS  R3_schedules — [{'name': 'fundamental-graphs-warmer-sched', 'expr': 'cron(25 9 * * ? *)', 'input': '{"warm_auto": true}'}, {'name': 'fundamentals-engine-sched', 'expr': 'cron(0 13 ? * MON-FRI *)', 'input': ''}]
# RESULT: FAILS: ['R1_master_ranker_schema']

