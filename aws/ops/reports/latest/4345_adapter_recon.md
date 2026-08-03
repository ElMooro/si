# ops 4345 -- what the artifacts actually say

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-08-03T23:53:38+00:00  

## Log
- `23:53:38` ai-rerating-radar.json tops=['engine', 'version', 'generated_at', 'thesis', 'methodology', 'coverage', 'summary', 'all_ranked', 'sources']
- `23:53:38`    rows@'all_ranked' r0keys=['symbol', 'name', 'layer', 'sector', 'industry', 'peer_group', 'peer_group_kind', 'in_ai_cohort', 'cap_bucket', 'market_cap', 'is_small_mid', 'growth_pct', 'growth_basis', 'trailing_growth_pct', 'margin_pct', 'ev_sales']
- `23:53:38`    sample={"symbol": "BA", "name": "The Boeing Company", "layer": "None", "sector": "Industrials", "industry": "Aerospace & Defense", "peer_group": "Aerospace & Defense", "peer_group_kind": "industry", "in_ai_cohort": "False"}
- `23:53:38` magic-formula.json tops=['version', 'generated_at', 'universe', 'regime', 'n_universe_eligible', 'median_earnings_yield_pct', 'median_roic_pct', 'top_10_avg_earnings_yield_pct', 'top_10_avg_roic_pct']
- `23:53:38` opportunities.json tops=['schema_version', 'method', 'generated_at', 'risk_gate', 'elapsed_s', 'n_covered', 'factor_weights', 'verdict_counts', 'changes']
- `23:53:38` insider-clusters.json tops=['schema_version', 'method', 'generated_at', 'lookback_days', 'duration_s', 'stats', 'thresholds', 'clusters', 'all_ticker_buys']
- `23:53:38`    rows@'clusters' r0keys=['ticker', 'company', 'cik', 'n_insiders', 'n_transactions', 'total_shares', 'total_value', 'avg_price', 'first_buy', 'last_buy', 'highest_role', 'has_ceo', 'has_cfo', 'has_chairman', 'has_director', 'insiders']
- `23:53:38`    sample={"ticker": "ACI", "company": "Albertsons Companies, Inc.", "cik": "0001646972", "n_insiders": "2", "n_transactions": "3", "total_shares": "209909", "total_value": "2412222.3", "avg_price": "11.49"}
- `23:53:38` congress-direct.json tops=['ok', 'version', 'generated_at', 'elapsed_s', 'source', 'senate', 'house']
- `23:53:38` squeeze-fuel.json tops=['engine', 'version', 'ok', 'generated_at', 'thesis', 'si_settlement_date', 'ftd_file', 'n_finra_universe', 'n_scored']
- `23:53:38` short-interest.json tops=['version', 'generated_at', 'watchlist_size', 'n_tickers_with_data', 'n_tickers_finra', 'n_tickers_polygon', 'n_tickers_short_interest', 'n_tickers_priced', 'by_ticker']
- `23:53:38` ✅ recon complete
