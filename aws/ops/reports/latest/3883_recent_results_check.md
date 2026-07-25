# ops 3883 — read recent_results_30d + data_sources directly, by the RIGHT key

**Status:** success  
**Duration:** 0.1s  
**Finished:** 2026-07-25T19:35:28+00:00  

## Data

| age_h | len_pead_signals | len_recent_results_30d | n_pead_field | n_recent_field | semi_hits_in_pead | semi_hits_in_recent_30d |
|---|---|---|---|---|---|---|
| 8.1 |  | 46 |  | 46 |  |  |
|  |  |  |  |  |  | 4 |
|  | 10 |  | 10 |  |  |  |
|  |  |  |  |  | 1 |  |

## Log
## 1. the right field this time

- `19:35:28`   sample record keys: ['eps_actual', 'eps_estimate', 'eps_surprise_pct', 'filing_date', 'importance', 'pead_label', 'pead_score', 'period_end', 'returns', 'revenue_actual', 'revenue_surprise_pct', 'surprise_source', 'ticker']
- `19:35:28`   full sample record: {"ticker": "VZ", "filing_date": "2026-07-24", "period_end": "Q2 2026", "eps_actual": 1.3, "eps_estimate": 1.27, "eps_surprise_pct": 2.36, "revenue_actual": 34253000000.0, "revenue_surprise_pct": -3.04, "importance": 5, "returns": {"1d": null, "5d": null, "20d": null}, "pead_label": "MIXED", "pead_score": 50, "surprise_source": "benzinga"}
## 2. data_sources — what's ACTUALLY live vs dead right now

- `19:35:28`   data_sources: {"surprises_and_calendar": "Benzinga Earnings (via Massive) \u2014 actual/estimate EPS+revenue, importance, AMC/BMO", "upcoming_watchlist": "Nasdaq earnings calendar API (free)", "returns": "Polygon aggregates API"}
## 3. semi tickers in recent_results_30d specifically

- `19:35:28`     {"ticker": "INTC", "filing_date": "2026-07-23", "period_end": "Q2 2026", "eps_actual": 0.42, "eps_estimate": 0.19, "eps_surprise_pct": 121.05, "revenue_actual": 16128000000.0, "revenue_surprise_pct": 12.0, "importance": 5, "returns": {"1d": -7.89, "5d": null, "20d": null}, "pead_label": "BEAT_BUT_FELL", "pead_score": 43, "surprise_source": "benzinga"}
- `19:35:28`     {"ticker": "TSM", "filing_date": "2026-07-16", "period_end": "Q2 2026", "eps_actual": 4.31, "eps_estimate": 3.77, "eps_surprise_pct": 14.32, "revenue_actual": 40200000000.0, "revenue_surprise_pct": 1.11, "importance": 5, "returns": {"1d": -2.77, "5d": 1.43, "20d": null}, "pead_label": "BEAT_BUT_FELL", "pead_score": 43, "surprise_source": "benzinga"}
- `19:35:28`     {"ticker": "ASML", "filing_date": "2026-07-15", "period_end": "Q2 2026", "eps_actual": 8.824, "eps_estimate": 7.98, "eps_surprise_pct": 10.58, "revenue_actual": 10841000000.0, "revenue_surprise_pct": 5.46, "importance": 5, "returns": {"1d": -1.67, "5d": -0.74, "20d": null}, "pead_label": "BEAT_BUT_FELL", "pead_score": 43, "surprise_source": "benzinga"}
- `19:35:28`     {"ticker": "MU", "filing_date": "2026-06-24", "period_end": "Q3 2026", "eps_actual": 25.11, "eps_estimate": 20.2, "eps_surprise_pct": 24.31, "revenue_actual": 41456000000.0, "revenue_surprise_pct": 18.41, "importance": 5, "returns": {"1d": 15.74, "5d": -1.55, "20d": -5.56}, "pead_label": "STRONG_POSITIVE_DRIFT", "pead_score": 88, "surprise_source": "benzinga"}
## 4. PEAD signals — same check, might carry surprise/drift even if recent_results doesn't

- `19:35:28`     {"ticker": "MU", "filing_date": "2026-06-24", "period_end": "Q3 2026", "eps_actual": 25.11, "eps_estimate": 20.2, "eps_surprise_pct": 24.31, "revenue_actual": 41456000000.0, "revenue_surprise_pct": 18.41, "importance": 5, "returns": {"1d": 15.74, "5d": -1.55, "20d": -5.56}, "pead_label": "STRONG_POSITIVE_DRIFT", "pead_score": 88, "surprise_source": "benzinga"}
- `19:35:28` ✅ PROBE COMPLETE
