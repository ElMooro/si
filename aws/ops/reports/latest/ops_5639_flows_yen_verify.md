- `19:47:51` ✅ justhodl-etf-fund-flows: source bytes, AWS code and new measurements verified
- `19:47:53` ✅ justhodl-capital-flow-radar: source bytes, AWS code and new measurements verified
**Status:** success  
**Duration:** 11.9s  
**Finished:** 2026-09-17T19:47:58+00:00  

## Data

| baseline_weeks | cftc_current | cftc_reports | code_sha256 | commit | complete_complexes | fresh_funds | function | funding_proxy | generated_at | large_source_bytes | quality | sample | total_complexes | total_funds |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | 299 |  |  |  | 94085 |  | {'ticker': 'SPY', 'methodology_version': 'etf-dated-flows.v2', 'unit': 'USD', 'measure': 'provider_fund_flow', 'source': 'ETF Global fund_flow via Massive/Polygon', 'date_basis': 'effective_date', 'observation_date': '2026-09-16', 'quality': {'status': 'fresh', 'age_days': 1, 'max_age_days': 5, 'invalid_rows': 0}, 'daily_flow_usd': -4998877950.0, 'windows': {'5': {'status': 'complete_observed_window', 'dates': ['2026-09-16', '2026-09-15', '2026-09-14', '2026-09-11', '2026-09-10'], 'n': 5, 'calendar_span_days': 6, 'sum_usd': -17426631306.0}, '21': {'status': 'complete_observed_window', 'dates': ['2026-09-16', '2026-09-15', '2026-09-14', '2026-09-11', '2026-09-10', '2026-09-09', '2026-09-08', '2026-09-04', '2026-09-03', '2026-09-02', '2026-09-01', '2026-08-31', '2026-08-28', '2026-08-27', '2026-08-26', '2026-08-25', '2026-08-24', '2026-08-21', '2026-08-20', '2026-08-19', '2026-08-18'], 'n': 21, 'calendar_span_days': 29, 'sum_usd': -22683927863.3}}, 'aum_usd': 778093107663.8375, 'persistence_observations': 4, 'price_return_5obs_pct': None, 'flow_zscore_60observations': -1.29, 'execution_eligible': False, 'note': 'Provider-reported fund flows, not exchange trading volume or identified institutional purchases. Windows count observed reporting dates.'} |  | 300 |
|  |  |  | 270911qliLn+4LXx0Dli0xsmRYvMeNGZdA4cP1kO2NM= | b68d0cb4cc972bee62a59378b9132edb022d4a04 |  |  | justhodl-etf-fund-flows |  | 2026-09-17T19:47:50.438821+00:00 |  | {'status': 'partial', 'fresh_series': 299, 'total_series': 300} |  |  |  |
|  |  |  |  |  | 44 |  |  |  |  |  |  |  | 46 |  |
|  |  |  | 4peH/uJ2bIncX4h2oaeYAW0pIC3tfsWoF3aB7uV28Q0= | 12907314717f4dc7bee82c1a058a15d865bb5ada |  |  | justhodl-capital-flow-radar |  | 2026-09-17T19:47:52.878813+00:00 |  | {'status': 'partial', 'complete_complexes': 44, 'total_complexes': 46, 'duplicate_tickers_excluded': []} |  |  |  |
| 260 | {'report_date': '2026-09-08', 'leveraged_funds_long': 81760.0, 'leveraged_funds_short': 130858.0, 'net_contracts': -49098.0, 'open_interest': 499635.0, 'net_pct_open_interest': -9.826773544687622} | 600 |  |  |  |  |  | {'status': 'fresh', 'spread_pp': 2.1696, 'us_rate_pct': 3.62806, 'jp_rate_pct': 1.45845, 'observation_month': '2026-07', 'us_observations': 31, 'basis': 'monthly averages, differing instrument definitions; indicative comparison only'} |  |  |  |  |  |  |
|  |  |  | cSOmEipb51a378C9ka1Jd+ilbix7SyYKepLZbKYHvEg= | 60b4695be9ac6f35e8291b4151226442b03a139e |  |  | justhodl-yen-carry |  | 2026-09-17T19:47:58.551817+00:00 |  | {'status': 'fresh', 'fresh_series': 7, 'required_series': 7} |  |  |  |

## Log
- `19:47:58` ✅ justhodl-yen-carry: source bytes, AWS code and new measurements verified
