# 1) justhodl-pnl-tracker config

**Status:** success  
**Duration:** 2.9s  
**Finished:** 2026-05-04T20:26:55+00:00  

## Log
- `20:26:52`   state: Active, mem=256MB, timeout=60s
- `20:26:52`   last modified: 2026-04-26T12:53:01.000+0000
- `20:26:52`   env: ['POLYGON_KEY']
# 2) pnl/ and portfolio/ S3 keys

- `20:26:52`     portfolio/pnl-daily.json                               1,519b  2026-05-03T22:00:05+00:00
- `20:26:52`     portfolio/pnl-history.json                             2,597b  2026-05-03T22:00:05+00:00
- `20:26:52`     portfolio/signal-portfolio-history.json                  226b  2026-05-04T12:21:46+00:00
- `20:26:52`     portfolio/signal-portfolio-state.json                  7,244b  2026-05-04T12:21:46+00:00
- `20:26:52`     portfolio/state.json                                     396b  2026-04-25T12:27:28+00:00
- `20:26:52`     portfolio/watchlist.json                                 343b  2026-04-25T12:32:55+00:00
# 3) pnl-tracker write paths from source

- `20:26:53`   source: lambda_function.py (10,303 chars)
- `20:26:53`   put_object keys: []
- `20:26:53`   snapshot top keys: ['as_of', 'generated_at', 'inception', 'days_since_inception', 'starting_value_usd', 'current_phase', 'current_regime', 'current_action_required', 'buy_and_hold', 'allocation', 'current_value_usd', 'return_pct', 'breakdown']
# 4) Invoke pnl-tracker to get latest output

- `20:26:54`   status: 200, duration: 1.3s
- `20:26:54`   resp: {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": "{\"as_of\": \"2026-05-04\", \"buy_and_hold_return_pct\": 0.08, \"khalid_return_pct\": -0.2, \"delta_pct\": -0.28, \"phase\": \"PRE-CRISIS\", \"regime\": \"NEUTRAL\"}"}
# 5) signal-portfolio Lambda + outputs

- `20:26:54`   state: Active, mod: 2026-05-04T12:21:35.617+0000
- `20:26:54`   portfolio/signal-portfolio-state.json: 7,244b modified 2026-05-04 12:21:46+00:00
- `20:26:55`   top keys: ['version', 'generated_at', 'as_of_date', 'first_seen', 'last_run_date', 'initial_nav', 'current_nav', 'current_nav_pct_chg', 'unrealized_pnl_dollars', 'open_positions', 'recently_closed', 'all_closed_positions', 'stats', 'duration_s']
- `20:26:55`     version                             = 1.0
- `20:26:55`     generated_at                        = 2026-05-04T12:21:45.877520+00:00
- `20:26:55`     as_of_date                          = 2026-05-04
- `20:26:55`     first_seen                          = 2026-05-04
- `20:26:55`     last_run_date                       = 2026-05-04
- `20:26:55`     initial_nav                         = 100000.0
- `20:26:55`     current_nav                         = 100000.0
- `20:26:55`     current_nav_pct_chg                 = 0.0
- `20:26:55`     unrealized_pnl_dollars              = 0.0
- `20:26:55`     open_positions                      = list (n=10)
- `20:26:55`     recently_closed                     = list (n=0)
- `20:26:55`     all_closed_positions                = list (n=0)
- `20:26:55`     stats                               = dict (keys: ['n_closed', 'n_open', 'win_rate', 'avg_win_pct', 'avg_loss_pct', 'profit_factor', 'total_realized_pnl', 'total_realized_pnl_pct', 'sharpe_proxy', 'max_drawdown_pct'])
- `20:26:55`     duration_s                          = 0.78
