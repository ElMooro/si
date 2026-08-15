# ops 4716 — justhodl-invest field probe (read-only)

**Status:** success  
**Duration:** 1.0s  
**Finished:** 2026-08-15T20:55:21+00:00  

## Data

| legs_mismatch | legs_ok |
|---|---|
| 15 | 1 |

## Log
## 1. Leading-indicator legs (causal_graph.LEADING_INDICATORS)

- `20:55:20` ── copper_demand_pulse ──
- `20:55:20` ✗   copper_price_yoy: fleet:data/canary-grid.json:signals.copper.yoy_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/canary-grid.json: ['band', 'disclaimer', 'early_warning_level', 'elapsed_s', 'freshness', 'generated_at', 'headline', 'method', 'methodology', 'n_available', 'n_total', 'schema_version', 'signals', 'sub_grids', 'top_deteriorating']
- `20:55:21` ✗   data/divergence-engine-v2.json: NoSuchKey — engine has never run, or key name is wrong
- `20:55:21` ✗   peru_copper_production: fleet:data/peru-copper.json:production_yoy_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/peru-copper.json: ['copper_production', 'engine', 'generated_at', 'headline', 'note', 'read', 'source', 'unit']
- `20:55:21` ── korea_semiconductor_exports ──
- `20:55:21` ✗   korea_export_value_yoy: fleet:data/canary-grid.json:signals.korea_exports.yoy_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/canary-grid.json: ['band', 'disclaimer', 'early_warning_level', 'elapsed_s', 'freshness', 'generated_at', 'headline', 'method', 'methodology', 'n_available', 'n_total', 'schema_version', 'signals', 'sub_grids', 'top_deteriorating']
- `20:55:21` ✅   korea_export_value_yoy_flash: fleet:data/asia-leads.json:korea_exports.yoy_pct = 47.96
- `20:55:21` ✗   korea_port_volume: fleet:data/portwatch.json:korea.volume_yoy_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/portwatch.json: ['attribution', 'chile_trace', 'chokepoints', 'daily_layer', 'daily_rows', 'date_span', 'disruptions', 'errors', 'exporters', 'exporters_slowing', 'generated_at', 'industry_exposure_summary', 'metric_field', 'n_disrupted', 'ok', 'pids_seen', 'ports', 'ports_disrupted', 'ports_metric', 'ports_ref_matched']
- `20:55:21` ── taiwan_export_orders ──
- `20:55:21` ✗   taiwan_export_orders_yoy: fleet:data/asia-leads.json:taiwan_orders.yoy_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/asia-leads.json: ['disclaimer', 'elapsed_s', 'engine', 'generated_at', 'korea_exports', 'korea_flash', 'korea_flash_tape', 'methodology', 'siblings', 'sources', 'taiwan_exports', 'taiwan_orders', 'version']
- `20:55:21` ✗   taiwan_moea_detail: fleet:data/taiwan-moea.json:orders_yoy_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/taiwan-moea.json: ['_errors', 'engine', 'export_orders', 'generated_at', 'headline', 'note', 'semiconductor', 'source']
- `20:55:21` ── china_credit_impulse ──
- `20:55:21` ✗   china_tsf_yoy: fleet:data/asia-leads.json:china_tsf.yoy_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/asia-leads.json: ['disclaimer', 'elapsed_s', 'engine', 'generated_at', 'korea_exports', 'korea_flash', 'korea_flash_tape', 'methodology', 'siblings', 'sources', 'taiwan_exports', 'taiwan_orders', 'version']
- `20:55:21` ✗   china_liquidity_impulse: fleet:data/china-liquidity.json:credit_impulse_z -> path did not resolve to a number (got NoneType). Top-level keys of data/china-liquidity.json: ['credit_impulse', 'currency', 'dr_copper', 'elapsed_s', 'fred_failed', 'generated_at', 'interbank_rate', 'method', 'money', 'regime', 'regime_read', 'schema_version', 'series_resolved', 'tsf']
- `20:55:21` ── global_port_freight_pulse ──
- `20:55:21` ✗   port_throughput_pulse: fleet:data/port-cargo.json:global_pulse_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/port-cargo.json: ['accelerating_ports', 'chokepoints_context', 'complete_through', 'countries', 'coverage', 'data_age_days', 'date_field_type', 'decelerating_ports', 'duration_s', 'engine', 'engine_class', 'evidence_tier', 'expected_lag_days', 'fetch_status', 'gaps', 'generated_at', 'global_pulse', 'impact_map', 'lag_months', 'latest_data_date']
- `20:55:21` ✗   freight_composite_z: fleet:data/freight-pulse.json:composite_z -> path did not resolve to a number (got NoneType). Top-level keys of data/freight-pulse.json: ['composite', 'composite_role', 'engine_class', 'errors', 'fast_leg', 'generated_at', 'impact_map', 'inflections', 'lag_months', 'lead_vs_port', 'method', 'n_live', 'ok', 'rate_vs_volume', 'role_note', 'series', 'verdict', 'version']
- `20:55:21` ── grid_buildout_pulse ──
- `20:55:21` ✗   grid_executed_mw: fleet:data/grid-queue.json:executed_mw_yoy_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/grid-queue.json: ['attribution', 'coverage', 'gaps', 'generated_at', 'hotspots', 'impact_map', 'industrial_load', 'iso_queues', 'large_load_queue', 'lbnl_priors', 'method', 'national', 'planned_capacity', 'queue', 'queue_velocity', 'version']
- `20:55:21` ✗   pjm_queue_detail: fleet:data/pjm-grid.json:executed_mw_yoy_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/pjm-grid.json: ['ai_demand_read', 'as_of', 'canaries', 'engine', 'forecast', 'fuel_mix', 'lmp', 'load', 'schema_version', 'source', 'thesis']
- `20:55:21` ── lumber_housing_pulse ──
- `20:55:21` ✗   lumber_price_yoy: fleet:data/canary-grid.json:signals.lumber.yoy_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/canary-grid.json: ['band', 'disclaimer', 'early_warning_level', 'elapsed_s', 'freshness', 'generated_at', 'headline', 'method', 'methodology', 'n_available', 'n_total', 'schema_version', 'signals', 'sub_grids', 'top_deteriorating']
- `20:55:21` ✗   construction_housing_pmi: fleet:data/construction-housing.json:production_yoy_pct -> path did not resolve to a number (got NoneType). Top-level keys of data/construction-housing.json: ['cycle_score', 'elapsed_s', 'generated_at', 'method', 'n_resolved', 'n_series', 'note', 'read', 'regime', 'regime_color', 'schema_version', 'series', 'signals']
## 2. Whole-document reads (Tier 2 / Tier 3)

- `20:55:21` ✅   forward-returns (Tier 2 SPX/sector ER) (data/forward-returns.json): present. Top-level shape: ['assets', 'benchmark_portfolios', 'disclaimer', 'elapsed_s', 'engine', 'generated_at', 'headlines', 'horizon_years', 'macro_inputs', 'methodology', 'rankings', 'real_gdp_growth_assumption_pct', 'version']
- `20:55:21` ✅   industry-boom (Tier 3 universe seed) (data/industry-boom.json): present. Top-level shape: ['coverage', 'disclaimer', 'elapsed_s', 'engine', 'generated_at', 'league', 'methodology', 'n_industries', 'n_universe', 'siblings', 'trouble', 'version']
- `20:55:21` ✗   data/backlog-miner.json: NoSuchKey — engine has never run, or key name is wrong
- `20:55:21` ✅   backlog XBRL (Tier 3 backlog fallback) (data/backlog.json): present. Top-level shape: ['accelerating', 'by_ticker', 'cap_distribution', 'cheap_vs_backlog', 'duration_s', 'engine', 'generated_at', 'ledger_size', 'method', 'n_covered', 'slice_this_run', 'sources', 'version']
- `20:55:21` ✅   catalyst (Tier 3 catalyst strength) (data/catalyst.json): present. Top-level shape: ['as_of', 'by_ticker', 'class_census', 'doctrine', 'engine', 'macro', 'n_tickers', 'taxonomy']
- `20:55:21` ✅   stock-buying (Tier 3 PEG/buyback/QoQ/cycle) (data/stock-buying.json): present. Top-level shape: ['as_of', 'backlog_join_n', 'backlog_kinds', 'catalyst_join_n', 'census_fields_sample', 'census_mode', 'census_source', 'cmode', 'crows_len', 'doctrine', 'engine', 'f13_join_n', 'fmp_key', 'gates_summary', 'khalid_five_missing', 'lanes', 'matrix_probe', 'n_scored', 'n_universe', 'schema_version']
- `20:55:21` ✅   impact-graph exposure graph (data/impact/exposure-graph.json): present. Top-level shape: ['adv_sessions_used', 'field_coverage', 'float_status', 'generated_at', 'industries', 'industry_backfill', 'n_tickers', 'sector_etf_proxy', 'tickers', 'version']
- `20:55:21` ✅   impact-graph betas (data/impact/betas.json): present. Top-level shape: ['betas', 'generated_at', 'min_n_obs', 'n_history_days', 'note', 'pairs_by_factor', 'status', 'version']
## 3. industry_boom_label cross-walk sanity check

- `20:55:21`   132 live industry-boom labels found
- `20:55:21` ✅   semis_memory: 'Semiconductors' matches a live league row
- `20:55:21` ✅   semis_foundry_logic: 'Semiconductors' matches a live league row
- `20:55:21` ✅   electronics_hardware: 'Computer Hardware' matches a live league row
- `20:55:21` ✗   construction_housing: 'Homebuilding' NOT found in live industry-boom league — closest live labels: ['Advertising Agencies', 'Aerospace & Defense', 'Agricultural - Machinery', 'Agricultural Farm Products', 'Agricultural Inputs', 'Airlines, Airports & Air Services', 'Apparel - Footwear & Accessories', 'Apparel - Manufacturers', 'Apparel - Retail', 'Asset Management - Cryptocurrency', 'Auto - Dealerships', 'Auto - Manufacturers', 'Auto - Parts', 'Banks - Diversified', 'Banks - Regional']
- `20:55:21` ✗   grid_electrical_infra: 'Industrial Machinery' NOT found in live industry-boom league — closest live labels: ['Advertising Agencies', 'Aerospace & Defense', 'Agricultural - Machinery', 'Agricultural Farm Products', 'Agricultural Inputs', 'Airlines, Airports & Air Services', 'Apparel - Footwear & Accessories', 'Apparel - Manufacturers', 'Apparel - Retail', 'Asset Management - Cryptocurrency', 'Auto - Dealerships', 'Auto - Manufacturers', 'Auto - Parts', 'Banks - Diversified', 'Banks - Regional']
## Verdict

- `20:55:21` ⚠ 15 leg(s) mismatched — engine will still deploy safely (mismatched legs degrade to INSUFFICIENT_DATA, never a fake value) but Tier 1 will confirm nothing for those indicators until causal_graph.py's Leg.source strings are corrected against the top-level keys printed above.
