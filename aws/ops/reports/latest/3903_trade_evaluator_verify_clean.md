# ops 3903 — clean re-verification (3902's fix worked, 3902's gate parsing didn't)

**Status:** success  
**Duration:** 0.2s  
**Finished:** 2026-07-26T03:36:27+00:00  

## Data

| n_strategies_tracked | overall_win_rate_30d_pct | total_evaluated_30d |
|---|---|---|
| 5 | 53.2 | 1557 |

## Log
- `03:36:27`   DEBATE_BUY           evaluated_30d=6      win_rate=33.3% avg_return=9.47%
- `03:36:27`   REGIME_PICK          evaluated_30d=260    win_rate=61.9% avg_return=2.83%
- `03:36:27`   OPTIONS_TIER_A       evaluated_30d=1262   win_rate=52.1% avg_return=0.78%
- `03:36:27`   TIER_A_ALPHA         evaluated_30d=29     win_rate=24.1% avg_return=-5.11%
- `03:36:27`   DEBATE_STRONG_BUY    evaluated_30d=0      win_rate=None% avg_return=None%
- `03:36:27` ✅   total_evaluated_30d is a real, large positive number
- `03:36:27` ✅   at least 3 strategies have real evaluated data
- `03:36:27` ✅   overall win rate is a real, plausible number (not null, not 0 or 100)
- `03:36:27` ✅ PASS_ALL — 1557 real evaluated calls, 53.2% overall win rate, first time ever populated
