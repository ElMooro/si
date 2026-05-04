# Create justhodl-signal-portfolio + smoke test

**Status:** success  
**Duration:** 10.8s  
**Finished:** 2026-05-04T12:21:46+00:00  

## Log
- `12:21:35`   zip size: 6,738b
- `12:21:35` ✅   ✓ created
## EventBridge schedule (daily 22:30 UTC, after market close)

- `12:21:36` ✅   ✓ wired
## Smoke test (mtm + harvest + open new positions)

- `12:21:45`   status: 200 duration: 1.6s
- `12:21:45`   resp: {"statusCode": 200, "body": "{\"n_open\": 10, \"n_new_today\": 10, \"n_closed_today\": 0, \"n_closed_total\": 0, \"current_nav\": 100000.0, \"current_nav_pct_chg\": 0.0, \"win_rate\": null, \"duration_s\": 0.78}"}
## S3 verify — state

- `12:21:46`   initial_nav: $100,000.00
- `12:21:46`   current_nav: $100,000.00 (+0.00%)
- `12:21:46`   unrealized_pnl: $0.00
- `12:21:46`   n_open: 10
- `12:21:46`   n_closed_today: 0
- `12:21:46`   n_closed_total: 0
- `12:21:46`   win_rate: None%
- `12:21:46`   profit_factor: None
- `12:21:46`   expectancy_pct: None
- `12:21:46`   max_dd_pct: None
## Open positions sample

- `12:21:46`     earnings_pead      QCOM   LONG  entry=$ 177.01 now=$ 177.01 pnl=+0.00% qty=141
- `12:21:46`     earnings_pead      TMUS   LONG  entry=$ 196.06 now=$ 196.06 pnl=+0.00% qty=127
- `12:21:46`     earnings_pead      NOW    LONG  entry=$  91.16 now=$  91.16 pnl=+0.00% qty=274
- `12:21:46`     earnings_pead      ELV    LONG  entry=$ 372.68 now=$ 372.68 pnl=+0.00% qty=67
- `12:21:46`     short_squeeze      LIN    LONG  entry=$ 507.92 now=$ 507.92 pnl=+0.00% qty=49
- `12:21:46`     short_squeeze      SHOP   LONG  entry=$ 127.67 now=$ 127.67 pnl=+0.00% qty=195
- `12:21:46`     short_squeeze      TSLA   LONG  entry=$ 390.82 now=$ 390.82 pnl=+0.00% qty=63
- `12:21:46`     short_squeeze      MELI   LONG  entry=$1850.05 now=$1850.05 pnl=+0.00% qty=13
- `12:21:46`     short_squeeze      ABBV   LONG  entry=$ 206.60 now=$ 206.60 pnl=+0.00% qty=121
- `12:21:46`     short_squeeze      LLY    LONG  entry=$ 963.33 now=$ 963.33 pnl=+0.00% qty=25
## By-source stats

