# === portfolio/pnl-daily.json ===

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-05-04T20:29:03+00:00  

## Log
- `20:29:03`   size: 1,519b
- `20:29:03`   top keys: ['as_of', 'generated_at', 'inception', 'days_since_inception', 'starting_value_usd', 'current_phase', 'current_regime', 'current_action_required', 'buy_and_hold', 'khalid_strategy', 'delta_pct', 'system_alpha', 'prices', 'v', 'DISCLAIMER', 'ka_strategy']
- `20:29:03`     as_of                               = 2026-05-04
- `20:29:03`     generated_at                        = 2026-05-04T20:26:54.141234+00:00
- `20:29:03`     inception                           = 2026-04-25
- `20:29:03`     days_since_inception                = 9
- `20:29:03`     starting_value_usd                  = 100000
- `20:29:03`     current_phase                       = PRE-CRISIS
- `20:29:03`     current_regime                      = NEUTRAL
- `20:29:03`     current_action_required             = REDUCE ALL RISK. Raise cash to 40%+. Exit leveraged and speculative positions.
- `20:29:03`     buy_and_hold                        = dict (keys: ['allocation', 'current_value_usd', 'return_pct', 'breakdown'])
- `20:29:03`       .current_value_usd              = 100077.76
- `20:29:03`       .return_pct                     = 0.08
- `20:29:03`     khalid_strategy                     = dict (keys: ['allocation', 'current_value_usd', 'return_pct', 'breakdown', '_note'])
- `20:29:03`       .current_value_usd              = 99795.81
- `20:29:03`       .return_pct                     = -0.2
- `20:29:03`       ._note                          = v1 approximation: current regime applied to current prices; 
- `20:29:03`     delta_pct                           = -0.28
- `20:29:03`     system_alpha                        = -0.28
- `20:29:03`     prices                              = dict (keys: ['current', 'baseline'])
- `20:29:03`     v                                   = 1.0
- `20:29:03`     DISCLAIMER                          = HYPOTHETICAL — for tracking only. Not investment advice. Past hypothetical performance does not pred
- `20:29:03`     ka_strategy                         = dict (keys: ['allocation', 'current_value_usd', 'return_pct', 'breakdown', '_note'])
- `20:29:03`       .current_value_usd              = 99795.81
- `20:29:03`       .return_pct                     = -0.2
- `20:29:03`       ._note                          = v1 approximation: current regime applied to current prices; 
- `20:29:03` 
# === portfolio/pnl-history.json ===

- `20:29:03`   size: 2,880b
- `20:29:03`   top keys: ['v', 'snapshots', 'last_updated']
- `20:29:03`     v                                   = 1.0
- `20:29:03`     snapshots                           = list (n=10)
- `20:29:03`       [0] keys: ['as_of', 'buy_and_hold_value_usd', 'khalid_strategy_value_usd', 'buy_and_hold_return_pct', 'khalid_return_pct', 'delta_pct', 'regime', 'phase', 'ka_strategy_value_usd', 'ka_return_pct']
- `20:29:03`       [0] sample: {'as_of': '2026-04-25', 'buy_and_hold_value_usd': '100000.0', 'khalid_strategy_value_usd': '100000.0', 'buy_and_hold_return_pct': '0.0', 'khalid_return_pct': '0.0', 'delta_pct': '0.0', 'regime': 'BEAR', 'phase': 'PRE-CRISIS', 'ka_strategy_value_usd': '100000.0', 'ka_return_pct': '0.0'}
- `20:29:03`     last_updated                        = 2026-05-04T20:26:54.141234+00:00
- `20:29:03` 
# === portfolio/signal-portfolio-state.json ===

- `20:29:03`   size: 7,244b
- `20:29:03`   top keys: ['version', 'generated_at', 'as_of_date', 'first_seen', 'last_run_date', 'initial_nav', 'current_nav', 'current_nav_pct_chg', 'unrealized_pnl_dollars', 'open_positions', 'recently_closed', 'all_closed_positions', 'stats', 'duration_s']
- `20:29:03`     version                             = 1.0
- `20:29:03`     generated_at                        = 2026-05-04T12:21:45.877520+00:00
- `20:29:03`     as_of_date                          = 2026-05-04
- `20:29:03`     first_seen                          = 2026-05-04
- `20:29:03`     last_run_date                       = 2026-05-04
- `20:29:03`     initial_nav                         = 100000.0
- `20:29:03`     current_nav                         = 100000.0
- `20:29:03`     current_nav_pct_chg                 = 0.0
- `20:29:03`     unrealized_pnl_dollars              = 0.0
- `20:29:03`     open_positions                      = list (n=10)
- `20:29:03`       [0] keys: ['signal_id', 'source', 'ticker', 'direction', 'signal_type', 'rationale', 'score', 'entry_date', 'entry_price', 'stop_price', 'target_price', 'qty', 'notional_at_entry', 'max_hold_days', 'status', 'current_price', 'current_pnl_pct', 'current_pnl_dollars', 'high_water_mark_price', 'low_water_mark_price', 'exit_date', 'exit_price', 'exit_reason', 'realized_pnl_pct', 'realized_pnl_dollars', 'days_held']
- `20:29:03`       [0] sample: {'signal_id': 'fb39fc94f3ce1e43', 'source': 'earnings_pead', 'ticker': 'QCOM', 'direction': 'LONG', 'signal_type': 'post_earnings_drift', 'rationale': 'PEAD: beat +0.0%, 1d +0.0%', 'score': '80', 'entry_date': '2026-05-04', 'entry_price': '177.01', 'stop_price': '169.9296', 'target_price': '190.2857', 'qty': '141', 'notional_at_entry': '24958.41', 'max_hold_days': '21', 'status': 'OPEN', 'current_price': '177.01', 'current_pnl_pct': '0.0', 'current_pnl_dollars': '0.0', 'high_water_mark_price': '177.01', 'low_water_mark_price': '177.01', 'exit_date': 'None', 'exit_price': 'None', 'exit_reason': 'None', 'realized_pnl_pct': 'None', 'realized_pnl_dollars': 'None', 'days_held': '0'}
- `20:29:03`     recently_closed                     = list (n=0)
- `20:29:03`     all_closed_positions                = list (n=0)
- `20:29:03`     stats                               = dict (keys: ['n_closed', 'n_open', 'win_rate', 'avg_win_pct', 'avg_loss_pct', 'profit_factor', 'total_realized_pnl', 'total_realized_pnl_pct', 'sharpe_proxy', 'max_drawdown_pct'])
- `20:29:03`       .n_closed                       = 0
- `20:29:03`       .n_open                         = 10
- `20:29:03`       .total_realized_pnl             = 0.0
- `20:29:03`       .total_realized_pnl_pct         = 0.0
- `20:29:03`     duration_s                          = 0.78
- `20:29:03` 
# === portfolio/signal-portfolio-history.json ===

- `20:29:03`   size: 226b
- `20:29:03`   top keys: ['daily_snapshots']
- `20:29:03`     daily_snapshots                     = list (n=1)
- `20:29:03`       [0] keys: ['date', 'n_open', 'n_closed_today', 'n_closed_total', 'current_nav', 'current_nav_pct_chg', 'unrealized_pnl', 'win_rate', 'total_realized_pnl_pct']
- `20:29:03`       [0] sample: {'date': '2026-05-04', 'n_open': '10', 'n_closed_today': '0', 'n_closed_total': '0', 'current_nav': '100000.0', 'current_nav_pct_chg': '0.0', 'unrealized_pnl': '0.0', 'win_rate': 'None', 'total_realized_pnl_pct': '0.0'}
- `20:29:03` 
# === portfolio/state.json ===

- `20:29:03`   size: 396b
- `20:29:03`   top keys: ['v', 'as_of', 'starting_value_usd', 'allocations', 'baseline_prices', 'managed_by', '_note']
- `20:29:03`     v                                   = 1.0
- `20:29:03`     as_of                               = 2026-04-25
- `20:29:03`     starting_value_usd                  = 100000
- `20:29:03`     allocations                         = dict (keys: ['SPY', 'TLT', 'GLD', 'CASH'])
- `20:29:03`       .SPY                            = 0.6
- `20:29:03`       .TLT                            = 0.2
- `20:29:03`       .GLD                            = 0.1
- `20:29:03`       .CASH                           = 0.1
- `20:29:03`     baseline_prices                     = dict (keys: ['SPY', 'TLT', 'GLD'])
- `20:29:03`       .SPY                            = 713.94
- `20:29:03`       .TLT                            = 86.71
- `20:29:03`       .GLD                            = 433.25
- `20:29:03`     managed_by                          = loop2_pnl_tracker_v1
- `20:29:03`     _note                               = Hypothetical $100k starting portfolio. Edit allocations to match your real allocation if desired. ba
- `20:29:03` 
# === portfolio/watchlist.json ===

- `20:29:03`   size: 343b
- `20:29:03`   top keys: ['v', 'tickers', '_note', 'created_at']
- `20:29:03`     v                                   = 1.0
- `20:29:03`     tickers                             = list (n=10)
- `20:29:03`     _note                               = Edit this list to control which tickers get nightly multi-agent debate. Max 10. Used by justhodl-wat
- `20:29:03`     created_at                          = 2026-04-25T12:32:54.794744+00:00
- `20:29:03` 
