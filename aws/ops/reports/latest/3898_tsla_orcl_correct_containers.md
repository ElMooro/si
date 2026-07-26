# ops 3898 — same feeds, real container names this time

**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-07-26T00:42:05+00:00  

## Data

| capital_return_age_h | earnings_whisper_age_h | eps_revision_age_h | headline | hiring_velocity_age_h | n_appointments | n_cannibals | n_departures | n_events_total | n_qualifying | n_tickers_with_signals | n_total | sec_filings_age_h | signal_definitions | summary | talent_migration_age_h | tsla_hits |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 16.7 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0 |
|  |  | 13.7 |  |  |  |  |  |  | 63 |  |  |  |  | {"top_25_overall": [{"symbol": "PLTR", "company": "Palantir Technologies Inc.", "score": 87.0, "flag": "HIGH_VELOCITY_TIER_B", "fy2_lift_pct": 42.8, "fwd_rev_growth_pct": 45.5, "upgrade_pct": 0.13, "n_estimates": 19, "sector": ""}, {"symbol": "SNDK", "company": "Sandisk Corporation", "score": 85.7, "flag": "HIGH_VELOCITY_TIER_B", "fy2_lift_pct": 210.1, "fwd_rev_growth_pct": 144.6, "upgrade_pct": 0 |  |  |
|  |  |  |  |  |  |  |  | 367 |  | 264 |  | 3.6 | [{"id": "going_concern", "label": "Going concern warning", "polarity": "bearish", "severity": "critical", "weight": -40, "desc": "Auditor or company expressed substantial doubt about ability to continue operations", "forms": ["8-K", "10-Q", "10-K"]}, {"id": "material_weakness", "label": "Material weakness in controls", "polarity": "bearish", "severity": "high", "weight": -25, "desc": "Internal con |  |  |  |
| 10.9 |  |  | 88 capital-return cannibals - companies shrinking their share count with free cash flow, lifting EPS for every remaining shareholder. |  |  | 60 |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | 156.2 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | 25 |  | 25 |  |  |  | 99 |  |  |  | 11.6 |  |

## Log
## 1. earnings-whisper — all_setups + top_setups, search for TSLA

- `00:42:04`   n_upcoming=83 tier_counts={'S': 0, 'A': 0, 'B': 1, 'C': 82} — TSLA genuinely absent from this feed's setups
## 2. eps-revision-velocity — all_qualifying, search for TSLA and ORCL

- `00:42:04`   TSLA: 1 hits
- `00:42:04`     [18]: {"symbol": "TSLA", "company": "Tesla, Inc.", "score": 70.4, "flag": "HIGH_VELOCITY_TIER_B", "status": "ok", "fundamentals": {"price": 313.03, "market_cap": 1236326809475, "sector": "", "industry": "", "pct_from_52w_high": -37.2}, "estimates": {"fy1_year": "2026", "fy1_eps_avg": 1.8127, "fy1_rev_avg": 105092055501, "fy1_n_estimates": 25, "fy2_year": "2027", "fy2_eps_avg": 2.34693, "fy2_lift_pct": 29.5, "fwd_rev_growth_pct": 14.4, "dispersion": 0.745}, "ratings_breadth": {"n_recent_90d": 25, "n_up
- `00:42:04`   ORCL: 1 hits
- `00:42:04`     [8]: {"symbol": "ORCL", "company": "Oracle Corporation", "score": 85.0, "flag": "HIGH_VELOCITY_TIER_B", "status": "ok", "fundamentals": {"price": 114.99, "market_cap": 331225245300, "sector": "", "industry": "", "pct_from_52w_high": -66.7}, "estimates": {"fy1_year": "2027", "fy1_eps_avg": 8.04981, "fy1_rev_avg": 89614281041, "fy1_n_estimates": 26, "fy2_year": "2028", "fy2_eps_avg": 10.90724, "fy2_lift_pct": 35.5, "fwd_rev_growth_pct": 45.5, "dispersion": 0.041}, "ratings_breadth": {"n_recent_90d": 25
## 3. sec-filings-intel — all_tickers + events_by_signal + highlights, search ORCL

- `00:42:05`   ORCL hits: 0
## 4. capital-return — cannibals list, is ORCL in it (aggressive buybacks)

- `00:42:05`   ORCL in cannibals list: 0 hits
## 5. hiring-velocity top_50 + talent-migration departures/appointments — ORCL

- `00:42:05`   ORCL in hiring top_50: 0 hits: not present
- `00:42:05`   ORCL departures: 0, appointments: 0, recent_moves: 0
- `00:42:05` ✅ PROBE COMPLETE
