# ops 3882 — is earnings-tracker's Benzinga path dead, and what's live now

**Status:** success  
**Duration:** 2.0s  
**Finished:** 2026-07-25T19:31:16+00:00  

## Data

| failures | max_days_to | min_days_to | n_events |
|---|---|---|---|
|  | 55 | 2 | 579 |
| [] |  |  |  |

## Log
## 1. live earnings-tracker.json — content, freshness, semi coverage

- `19:31:14` ✅   data/earnings-tracker.json: 8.0h old
- `19:31:14`   top-level keys: ['aggregate_stats', 'data_sources', 'duration_s', 'forward_calendar', 'generated_at', 'n_forward_calendar', 'n_pead', 'n_recent', 'n_upcoming', 'pead_signals', 'recent_results_30d', 'upcoming_14d', 'version', 'watchlist_size']
- `19:31:14`   list-bearing keys: ['upcoming_14d', 'forward_calendar', 'recent_results_30d', 'pead_signals']
- `19:31:14`   n records: 81
- `19:31:14`   sample record keys: ['earnings_date', 'eps_consensus', 'fiscal_quarter_ending', 'last_year_eps', 'market_cap', 'n_estimates', 'name', 'ticker', 'time']
- `19:31:14`   semi-ticker records found: 3
- `19:31:14`     {"ticker": "QCOM", "name": "QUALCOMM Incorporated", "earnings_date": "2026-07-29", "time": "AMC", "eps_consensus": 1.54, "n_estimates": "8", "fiscal_quarter_ending": "Jun/2026", "last_year_eps": "$2.29", "market_cap": "$180,349,940,000"}
- `19:31:14`     {"ticker": "AMD", "name": "Advanced Micro Devices, Inc.", "earnings_date": "2026-08-04", "time": "AMC", "eps_consensus": 1.35, "n_estimates": "13", "fiscal_quarter_ending": "Jun/2026", "last_year_eps": "$0.27", "market_cap": "$880,018,858,862"}
- `19:31:14`     {"ticker": "AMAT", "name": "Applied Materials, Inc.", "earnings_date": "2026-08-13", "time": "AMC", "eps_consensus": 3.36, "n_estimates": "9", "fiscal_quarter_ending": "Jul/2026", "last_year_eps": "$2.48", "market_cap": "$446,840,367,204"}
## 2. is benzinga actually reachable right now, or dead (per memory: 403)

- `19:31:15`   no benzinga/403/massive lines in the last 3 log streams (200 events each)
## 3. does earnings-tracker's source still call the dead Benzinga path unconditionally

- `19:31:15`   (source-level check, no live call — just confirms whether a fallback exists)
## 4. catalyst-calendar.json — confirm it truly has zero retained history (re-verify ops 3881's finding independently)

- `19:31:16`   confirmed: 0 past events retained
## 5. verdict

- `19:31:16` ✅ PROBE COMPLETE
