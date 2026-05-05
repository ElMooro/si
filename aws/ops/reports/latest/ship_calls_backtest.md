# 1) Create or update Lambda

**Status:** success  
**Duration:** 6.5s  
**Finished:** 2026-05-05T12:53:44+00:00  

## Log
- `12:53:37`   zip size: 4,171b
- `12:53:37`   + creating fresh function
- `12:53:38` ✅   ✓ created: arn:aws:lambda:us-east-1:857687956942:function:justhodl-calls-backtest
# 2) EventBridge schedule

- `12:53:42`   rule: justhodl-calls-backtest-daily  expr=cron(15 14 * * ? *)
- `12:53:43` ✅   ✓ added invoke permission
- `12:53:43` ✅   ✓ target set
# 3) Smoke invoke

- `12:53:44`   status: 200, duration: 1.0s
- `12:53:44`   ok: True
- `12:53:44`   n_calls: 9
- `12:53:44`   n_trading_days: 0
- `12:53:44`   total_return_pct: 0.0
- `12:53:44`   spy_return_pct: 0.0
- `12:53:44`   alpha_vs_spy_pct: 0.0
- `12:53:44`   max_dd_pct: 0
# 4) Verify backtest/calls-results.json

- `12:53:44` ✅   ✓ written  (3,472b mod=2026-05-05 12:53:45+00:00)
- `12:53:44`   method: decisive_call_replay_v1
- `12:53:44`   n_calls: 9
- `12:53:44`   first_call: 2026-05-04
- `12:53:44`   last_date: 2026-05-05
- `12:53:44`   n_changes: 6
- `12:53:44`   total_return_pct: 0.0
- `12:53:44`   spy_return_pct: 0.0
- `12:53:44`   alpha_vs_spy_pct: 0.0
- `12:53:44`   sharpe_proxy: None
- `12:53:44`   max_dd_pct: 0
- `12:53:44` 
- `12:53:44`   Per-call breakdown:
- `12:53:44`     UNKNOWN           2026-05-04 → 2026-05-04  days=0 expo=1.00 SPY=+0.000% strat=+0.000%
- `12:53:44`     UNKNOWN           2026-05-04 → 2026-05-04  days=0 expo=1.00 SPY=+0.000% strat=+0.000%
- `12:53:44`     EXIT_ALL_RISK     2026-05-04 → 2026-05-04  days=0 expo=0.00 SPY=+0.000% strat=+0.000%
- `12:53:44`     EXIT              2026-05-04 → 2026-05-05  days=0 expo=0.15 SPY=+0.000% strat=+0.000%
- `12:53:44`     EXIT_ALL_RISK     2026-05-05 → 2026-05-05  days=0 expo=0.00 SPY=+0.000% strat=+0.000%
- `12:53:44`     EXIT              2026-05-05 → 2026-05-05  days=0 expo=0.15 SPY=+0.000% strat=+0.000%
- `12:53:44`     EXIT_ALL_RISK     2026-05-05 → 2026-05-05  days=0 expo=0.00 SPY=+0.000% strat=+0.000%
- `12:53:44`     EXIT              2026-05-05 → 2026-05-05  days=0 expo=0.15 SPY=+0.000% strat=+0.000%
- `12:53:44`     EXIT              2026-05-05 → 2026-05-05  days=0 expo=0.15 SPY=+0.000% strat=+0.000%
- `12:53:44` 
- `12:53:44`   nav_curve has 1 datapoints
- `12:53:44`     2026-05-04  nav=100000.0  spy_nav=100000.0 active=UNKNOWN
# 5) Verify backtest.html section

- `12:53:44`     ✓ Calls section heading
- `12:53:44`     ✓ loadCallsBacktest
- `12:53:44`     ✓ renderCallsNavChart
- `12:53:44`     ✓ VERB_COLOR map
- `12:53:44`     ✓ calls-results.json fetch
- `12:53:44`     ✓ calls-table tbody
- `12:53:44`     ✓ calls-nav-chart svg
