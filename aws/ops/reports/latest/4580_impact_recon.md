## 1. Feed schemas (top-level + first-row fields)

**Status:** success  
**Duration:** 7.2s  
**Finished:** 2026-08-10T19:40:54+00:00  

## Log
- `19:40:48`   census {bottom_quality,cadence,careful,coverage,elapsed_s,generated_at,metric_boards,ok,sectors,summary,top_quality,turnarounds,version}
- `19:40:48`   readthrough {beneficiaries,by_beneficiary,caveats,chase_guard,consensus_coverage,data_source,degraded,elapsed_s,engine,events,generated_at,industry_boom_context,n_beneficiaries,n_events,n_unpriced,ok,params,quadrant_counts,status,thesis,tier_caps,tiers,top_picks,twice_unpriced}
- `19:40:48`     .events[0] fields: ['age_h', 'anchor_day', 'dollar_vol', 'headline', 'industry', 'market_cap', 'move_extended_pct', 'move_pct', 'move_regular_pct', 'n_beneficiaries', 'n_unpriced', 'order_value_str', 'order_value_usd', 'prev_close', 'price', 'propagation', 'published', 'sec_8k', 'sec_8k_confirm', 'sector', 'source', 'spy_move_since_pct', 'ticker', 'type']
- `19:40:48`   congress {elapsed_s,generated_at,house,ok,senate,source,version}
- `19:40:48`   activist {academic_basis,all_setups,as_of,current_readings,engine,forward_expectations,methodology,recommended_trade,run_duration_seconds,schedule,signal_strength,sources,state,summary,top_setups,trigger_conditions,version,why_now_explainer}
- `19:40:48`   deal-scanner {base_rates,by_cap,by_event,by_sector,coverage,deals,disclaimer,elapsed_s,engine,generated_at,history,methodology,sources,summary,version,window}
- `19:40:48`   etf-true-flows {anomalies,by_etf,category_rotation,duration_s,engine,engine_class,evidence_tier,gaps,generated_at,ground_truth,inflows,maturity,method,n_etfs,n_price_fallback_degraded,nav_source_counts,outflows,probes,version}
- `19:40:48`   etf-shares-hist {days}
- `19:40:49`   flow-lookthrough {actual_accumulation,actual_distribution,caveats,elapsed_s,engine,evidence_tier,flows_asof,generated_at,index_events,inflow_leaders,methodology,n_etfs_used,n_etfs_with_delta,n_names,outflow_leaders,thematic_rotation_leaders,thesis,tier_note,top_picks,version}
- `19:40:49`   dark-pool {block_composition_mode,block_share_map,board,caveats,composition_note,daily_fusion,dark_map,dark_share_map,data_age_days,data_source,distribution,distribution_into_strength,dix,elapsed_s,engine,expected_lag_days,fetch_status,generated_at,high_conviction,lag_note,latest_week,monthly_ats,n_scored,ok}
- `19:40:49`   finra-short {config,data_date,elapsed_seconds,generated_at,generated_at_unix,market_composite,sectors,squeeze_candidates,tickers,top_svr,top_zscore,version}
- `19:40:51`   finra-short-hist {last_data_date,last_updated,tickers}
- `19:40:51`   share-flows {boards,disclaimer,engine_class,fresh_fetched,generated_at,method,n_tickers,tickers,version,warns}
- `19:40:52`   stealth {as_of,convergence,current_readings,data_sufficiency,engine,forward_expectations,historical_episodes,methodology,previous_state,recommended_trade,run_duration_seconds,schedule,signal_strength,sources,state,state_description,summary,top_insider_only,top_options_flow_only,top_short_covering_only,top_smart_money_only,trigger_conditions,version,why_now_explainer}
- `19:40:52`   accum-composite {component_coverage,dark_composition_mode,duration_s,engine,engine_class,feeds_missing,gaps,generated_at,method,n_names,names,radar_disagreement,state,validation,version,weights}
- `19:40:52`     .names[0] fields: ['components', 'n_components', 'radar_rank', 'rank', 'score', 'ticker', 'weight_covered']
- `19:40:52`   radar {accumulating,bottoms,buffer_days,confirmed_bottoms,confirmed_tops,days_added,distributing,dma200_breaks,duration_s,engine,etf_phases,evidence_note,evidence_tier,generated_at,join_coverage,leaders_fading,legend,ma_state,market_context,market_leaders,n_scored,reversals,role,signals_logged}
- `19:40:52`   grid-queue {attribution,coverage,gaps,generated_at,hotspots,industrial_load,iso_queues,lbnl_priors,method,national,planned_capacity,queue,version}
- `19:40:52`   port-cargo {accelerating_ports,countries,data_age_days,date_field_type,decelerating_ports,duration_s,engine,engine_class,evidence_tier,expected_lag_days,fetch_status,gaps,generated_at,global_pulse,lag_months,latest_data_date,method,n_ports_with_data,n_rows_window,stale,tonnage_fields,version}
- `19:40:52`   portwatch {attribution,chile_trace,chokepoints,daily_layer,daily_rows,date_span,disruptions,errors,exporters,exporters_slowing,generated_at,industry_exposure_summary,metric_field,n_disrupted,ok,pids_seen,ports,ports_disrupted,ports_metric,ports_ref_matched,ports_ref_matched_total,ports_ref_sample,ports_ref_total,ref_misses}
- `19:40:52`     .ports[0] fields: ['baseline_1y', 'country', 'id', 'industry_exposure', 'last_date', 'latest_7d_avg', 'n_days', 'name', 'prev_30d_avg', 'status', 'vs_baseline_pct', 'yoy_pct', 'z']
- `19:40:52`   freight-pulse {composite,composite_role,engine_class,errors,generated_at,inflections,lag_months,method,n_live,ok,role_note,series,verdict,version}
- `19:40:52`     .series['cass_expend'] fields: ['date', 'inflection', 'level', 'm6_ann_pct', 'name', 'ok', 'spark', 'yoy_pct', 'z_5y']
- `19:40:52`   insider-enr {academic_basis,as_of,engine,methodology,schedule,signal_strength,source,source_as_of,sources,state,summary,top_setups,version}
- `19:40:53`   13f-clusters {as_of_quarter,clusters,duration_s,generated_at,method,schema_version,stats}
- `19:40:53`     .clusters[0] fields: ['components', 'flag', 'fund_actions', 'fundamentals', 'legend_buyers', 'n_buyers', 'n_funds_holding', 'n_new', 'n_sellers', 'name', 'pct_from_52w_high', 'quant_buyers', 'rationale', 'score', 'signal_types', 'ticker', 'total_value']
## 2. S3 prefix inventory

- `19:40:53`   data/history/ n>=12 dirs=[] keys=['data/history/acm-term-premium.json', 'data/history/alt-px-cm.json', 'data/history/apac-flows.json', 'data/history/behavior-mirror-history.json', 'data/history/blackout.json', 'data/history/bond-crisis-analogs.json']
- `19:40:53`   data/warm/ n>=12 dirs=['data/warm/banxico/', 'data/warm/bis/', 'data/warm/boe/', 'data/warm/cftc/', 'data/warm/edgar-filings/', 'data/warm/eiopa/', 'data/warm/eurostat/', 'data/warm/fred-canary/'] keys=['data/warm/bis-gleif-summary.json', 'data/warm/canary-macro-summary.json', 'data/warm/eurostat-oecd-summary.json', 'data/warm/fred-catalog-summary.json']
- `19:40:53`   data/impact/ n>=0 dirs=[] keys=[]
- `19:40:53`   data/etf-shares-snapshots/ n>=1 dirs=[] keys=['data/etf-shares-snapshots/latest.json']
- `19:40:53`   data/features/ n>=0 dirs=[] keys=[]
- `19:40:53`   data/providers/ n>=12 dirs=[] keys=['data/providers/atlantafed.json', 'data/providers/banxico.json', 'data/providers/bcb.json', 'data/providers/bea.json', 'data/providers/bis.json', 'data/providers/bls.json']
- `19:40:53`   data/archive/ n>=12 dirs=['data/archive/ai-website-synthesis/', 'data/archive/auction-crisis-ai/', 'data/archive/auction-crisis/', 'data/archive/catalysts/', 'data/archive/convergence-radar/', 'data/archive/correlation-breaks/', 'data/archive/crisis-plumbing/', 'data/archive/feature-bus/', 'data/archive/momentum-leaders/', 'data/archive/pair-trades/'] keys=[]
## 3. SSM inventory (names only) + graded signal census

- `19:40:54`   121 params total; key-ish names: ['/justhodl/a2a/token/perplexity', '/justhodl/ai-chat/auth-token', '/justhodl/anthropic/api-key', '/justhodl/anthropic/api_key', '/justhodl/api-admin/token', '/justhodl/banxico-token', '/justhodl/bea-api-key', '/justhodl/bls-api-key', '/justhodl/census-api-key', '/justhodl/cryptoquant/token', '/justhodl/cryptoquant_api', '/justhodl/eia-api-key', '/justhodl/eodhd_api', '/justhodl/feedback/auth-token', '/justhodl/finviz/auth-token', '/justhodl/fred-api-key', '/justhodl/fred/api-key', '/justhodl/history-api/url', '/justhodl/massive-api-key', '/justhodl/openfigi/api-key', '/justhodl/perplexity/api-key', '/justhodl/polygon/api-key', '/justhodl/portfolio-admin/token', '/justhodl/push/admin-token', '/justhodl/push/vapid-private-key', '/justhodl/push/vapid-public-key', '/justhodl/push/vapid-subject', '/justhodl/te_api', '/justhodl/telegram/bot_token', '/justhodl/tvnotes/ingest-token', '/justhodl/zai-api-key']
- `19:40:54`   calibration graded signal_types (n_scored): []
## 4. DynamoDB signals table

- `19:40:54`   tables: ['justhodl-alert-actions', 'justhodl-api-keys', 'justhodl-api-rate', 'justhodl-backtest', 'justhodl-feedback', 'justhodl-history', 'justhodl-llm-cost', 'justhodl-outcomes', 'justhodl-portfolio', 'justhodl-push-subscriptions', 'justhodl-signal-registry', 'justhodl-signals', 'justhodl-subscribers', 'justhodl-trades', 'openbb-trading-signals']
- `19:40:54`   justhodl-alert-actions keys=[('alert_id', 'HASH'), ('action_ts', 'RANGE')] n_items~0
- `19:40:54`   justhodl-api-keys keys=[('key_hash', 'HASH')] n_items~14
- `19:40:54`   justhodl-api-rate keys=[('pk', 'HASH')] n_items~0
- `19:40:54` ✅ recon complete — build proceeds against these shapes
