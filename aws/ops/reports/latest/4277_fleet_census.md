# ops 4277 -- fleet census: every engine, every artifact

**Status:** success  
**Duration:** 13.2s  
**Finished:** 2026-08-02T18:01:04+00:00  

## Data

| age_h | fields | kb | key | mode | n | path | writers |
|---|---|---|---|---|---|---|---|
| 0.0 | ticker,n_engines,convergence_score,domain_coverage,n_domains |  | convergence-radar.json | list | 29 | $.pump_candidates |  |
| 0.0 | id,symbol,text,created,source |  | tradingview-notes.json | list | 3495 | $.notes |  |
| 0.2 | symbol,name,circulating_usd,prev_day,prev_week |  | stablecoin-flow.json | list | 15 | $.top_stablecoins_by_mcap |  |
| 0.3 | Representative,BioGuideID,TransactionDate,ReportDate,Ticker |  | house-ptr-trades.json | list | 209 | $.trades |  |
| 0.3 | ticker,n_trades,trade_source,source_status,n_buys |  | political-stocks.json | list | 12 | $.congress.top_buys |  |
| 0.4 | symbol,name,current_price,cycle_fit_phases,in_current_cycle |  | sector-rotation.json | list | 11 | $.sectors |  |
| 0.6 | factors,components_missing,m_score,m_flag,m_high_risk |  | forensic-screen.json | list | 25 | $.fortress_financials |  |
| 0.6 | ticker,current_price,perf_5d_pct,perf_20d_pct,perf_60d_pct |  | momentum-leaders.json | list | 30 | $.leaders |  |
| 0.6 | ticker,expected_return_pct,n_scenarios,by_scenario |  | stress-scenarios.json | list | 24 | $.asset_impact.all |  |
| 0.7 | ticker,score,engines,neg_gamma,coiled |  | options-confluence.json | list | 16 | $.multi_engine_confluence |  |
| 0.7 | symbol,name,conviction,posture,quadrant |  | sector-flow-state.json | list | 11 | $.sectors |  |
| 0.8 | why,ticker,name,conviction,khalid_panels |  | best-setups.json | list | 50 | $.top_setups |  |
| 0.8 | ticker,err |  | news-velocity.json | dict | 15 | $.by_ticker |  |
| 0.8 | ticker,score,n_recent_patents,n_baseline_patents,velocity_ratio |  | patent-velocity.json | list | 15 | $.highlights.highest_scale |  |
| 0.8 | ticker,pump_likelihood,pump_category,squeeze_profile,options_structure |  | pump-mechanics.json | list | 12 | $.candidates |  |
| 0.8 | ticker,n_engines,convergence_score,domain_coverage,n_domains |  | pump-positioning.json | list | 12 | $.candidates |  |
| 1.0 | ticker,name,sector,cap_bucket,overlap_score |  | deep-value-overlap.json | list | 19 | $.prime_setups |  |
| 1.0 | ticker,score,velocity,current_level,prior_level |  | ticker-trends.json | list | 20 | $.top_20 |  |
| 1.3 | ticker,future_intel_score,n_independent_signals,subscores,forward_orders |  | future-intelligence.json | list | 25 | $.top_25 |  |
| 1.5 | float |  | capital-flow-history.json | dict | 95 | $.entries[].flagged_scores |  |
| 1.5 | ticker,name,sector,flow_score,invested_usd |  | capital-flow.json | list | 20 | $.dollar_flow_in |  |
| 2.0 | ticker,client,score,recent_amount_usd,baseline_amount_usd |  | lobbying-intel.json | list | 200 | $.all_tickers |  |
| 2.0 | ticker,ret,n,win_rate |  | signal-backtest.json | list | 20 | $.by_verdict_stocks.STRONG OPPORTU |  |
| 2.2 | ticker,own_30d_pct,own_60d_pct,leader_30d_pct,lag_pct |  | rotation-chains.json | list | 19 | $.top_next_up |  |
| 2.3 | filer,ticker,tx_date,amount,type |  | congress-alpha.json | list | 15 | $.plans |  |
| 2.3 | float |  | etf-shares-history.json | dict | 119 | $.days[].shares |  |
| 2.3 | ticker,category,shares_outstanding,price,aum_est_b |  | etf-true-flows.json | list | 25 | $.inflows |  |
| 2.4 | ticker,stock_price,call_volume,put_volume,call_premium |  | options-flow.json | list | 8 | $.data.put_call.options_flow |  |
| 0.2 |  | 109 | data/canary-warroom.json |  |  |  | ['justhodl-canary-warroom'] |
| 0.4 |  | 36 | data/sector-rotation.json |  |  |  | ['justhodl-deal-scanner', 'justhodl-sector-rotation'] |
| 1.5 |  | 25 | data/capital-flow-history.json |  |  |  | ['justhodl-capital-flow'] |
| 1.5 |  | 257 | data/capital-flow.json |  |  |  | ['justhodl-capital-flow'] |
| 2.2 |  | 10 | data/rotation-chains.json |  |  |  | ['justhodl-rotation-chain'] |
| 4.3 |  | 0 | data/import-canary-history.json |  |  |  | ['justhodl-import-canary'] |
| 4.3 |  | 29 | data/import-canary.json |  |  |  | ['justhodl-import-canary', 'justhodl-portwatch'] |
| 4.3 |  | 7 | data/rotation-radar.json |  |  |  | ['justhodl-rotation-radar'] |
| 4.5 |  | 28 | data/theme-rotation.json |  |  |  | ['justhodl-theme-rotation'] |
| 5.3 |  | 26 | data/global-recession.json |  |  |  | ['justhodl-global-recession'] |
| 5.5 |  | 27 | data/boom-stage.json |  |  |  | ['justhodl-alert-sentinel'] |
| 5.5 |  | 29 | data/canary-grid.json |  |  |  | ['justhodl-canary-grid', 'justhodl-crisis-canaries'] |
| 5.7 |  | 1 | data/macro-leads.json |  |  |  | ['justhodl-macro-leads'] |
| 6.2 |  | 3 | data/freight-pulse.json |  |  |  | [] |
| 7.2 |  | 51 | data/industry-boom-history.json |  |  |  | ['justhodl-industry-boom'] |
| 7.2 |  | 56 | data/industry-boom.json |  |  |  | ['justhodl-deal-scanner', 'justhodl-industry-boom'] |
| 18.4 |  | 12 | data/ka-metrics.json |  |  |  | ['justhodl-ka-metrics'] |
| 18.4 |  | 12 | data/khalid-metrics.json |  |  |  | ['justhodl-ka-metrics', 'justhodl-khalid-metrics'] |
| 19.3 |  | 1 | data/gold-equity-rotation.json |  |  |  | ['justhodl-gold-equity-rotation'] |
| 19.5 |  | 0 | data/capital-flow-radar-state.json |  |  |  | ['justhodl-capital-flow-radar'] |
| 19.5 |  | 67 | data/capital-flow-radar.json |  |  |  | ['justhodl-capital-flow-radar'] |
| 19.8 |  | 8 | data/rotation-dashboard-history.json |  |  |  | ['justhodl-rotation-dashboard'] |

## Log
- `18:00:51` ✅ repo walk: 771 engines, 916 py files, 680 distinct data/ write targets
- `18:00:58` ✅ live S3: 874 top-level data/*.json (649 fresh<26h); 634 have identified writers; 46 write-targets not yet materialized
## per-ticker shape probe (every candidate, capped fetch)

- `18:01:04` ✅ ticker-keyed artifacts found: 32 (probed 120)
## macro candidates (fresh, non-ticker, for new legs)

- `18:01:04` ✅ census PUBLISHED: data/quantum-desk-sources.json (32 per-ticker sources, 29 macro candidates)
## RESULT

- `18:01:04` ✅ OPS 4277 PASS -- the fleet is mapped; v2 wiring next
