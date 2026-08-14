# ops 4656 — v1.4 evidence probe

**Status:** success  
**Duration:** 68.5s  
**Finished:** 2026-08-14T01:16:59+00:00  

## Log
- `01:15:50` [ps] ['book_value_ps', 'cash_ps', 'cfo_ps_ttm', 'fcf_ps_ttm', 'ncav_ps', 'ps_fwd', 'ps_ttm', 'revenue_ps_ttm'] · book_value_ps=7.2893 ; cash_ps=4.2304 ; cfo_ps_ttm=9.9472 ; fcf_ps_ttm=9.7274
- `01:15:50` [fcf] ['ev_fcf_ttm', 'fcf', 'fcf_cagr_3y_pct', 'fcf_cagr_5y_pct', 'fcf_conversion_pct', 'fcf_ev_yield_pct', 'fcf_margin_pct', 'fcf_per_employee'] · ev_fcf_ttm=29.326 ; fcf=38713000000.0 ; fcf_cagr_3y_pct=12.42 ; fcf_cagr_5y_pct=8.649
- `01:15:50` [inventory] ['days_inventory', 'inventory', 'inventory_to_revenue_pct', 'inventory_turnover', 'inventory_turnover_ttm'] · days_inventory=16.89 ; inventory=11092000000.0 ; inventory_to_revenue_pct=2.376 ; inventory_turnover=21.61
- `01:15:50` [revenue-chg] [] · 
- `01:15:50` [eps-chg] [] · 
- `01:15:50` [shares] ['share_count_yoy_pct'] · share_count_yoy_pct=-1.324
## store discovery

- `01:16:58` stores: ['data/13f-clone-alpha.json', 'data/13f-cusip-map-v2.json', 'data/13f-cusip-map.json', 'data/13f-flows-by-ticker.json', 'data/13f-positions.json', 'data/13f-price-anchors.json', 'data/13f-price-divergence.json', 'data/_archive/institutional-convergence.json', 'data/a2a/threads/engine-audit-capital-flow.json', 'data/ai-commentary/13f.json', 'data/ai-commentary/history/13f/2026-06-02.json', 'data/ai-commentary/history/13f/2026-06-30.json', 'data/ai-commentary/history/13f/2026-07-01.json', 'data/ai-commentary/history/13f/2026-07-02.json', 'data/ai-commentary/history/13f/2026-07-03.json', 'data/ai-commentary/history/13f/2026-07-06.json']
- `01:16:58` data/13f-flows-by-ticker.json top-keys=['as_of', 'whale_rule', 'whale_funds', 'n_tickers', 't']
- `01:16:58` data/dark-pool.json top-keys=['engine', 'version', 'ok', 'generated_at', 'thesis', 'latest_week', 'fetch_status', 'data_age_days']
- `01:16:59` data/etf-true-flows.json top-keys=['engine', 'version', 'engine_class', 'generated_at', 'duration_s', 'n_etfs', 'maturity', 'evidence_tier']
- `01:16:59` ✅ probe complete
