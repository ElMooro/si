# 0) Wait for any pending update

**Status:** success  
**Duration:** 5.8s  
**Finished:** 2026-05-04T23:27:21+00:00  

## Log
- `23:27:15`   ready, mod=2026-05-04T23:27:13.000+0000
# 1) Force redeploy with SPY benchmark integration

- `23:27:15`   zip size: 6,290b
- `23:27:18` ✅   ✓ deployed, mod=2026-05-04T23:27:16.000+0000
# 2) Verify SPY code in deployed source

- `23:27:18`   ✓ fetch_spy_window
- `23:27:18`   ✓ POLYGON_KEY
- `23:27:18`   ✓ alpha_vs_spy_pct field
- `23:27:18`   ✓ spy_nav in nav curve
# 3) Re-invoke + check SPY data flowed through

- `23:27:20`   status: 200, duration: 2.1s
- `23:27:20`   Strategy return: +69.02%
- `23:27:20`   SPY return:      9.2225%
- `23:27:20`   Alpha vs SPY:    59.7955%
- `23:27:20`   Final NAV:       $169018.0
- `23:27:20`   Max DD:          0.5304%
- `23:27:20`   Sharpe:          9.8867
# 4) Inspect backtest/results.json — full summary + spy_nav in nav curve

- `23:27:21`   Window: 2026-03-26 → 2026-05-03
- `23:27:21`   Strategy: $169018.0  (+69.02%)
- `23:27:21`   SPY     : $109222.49  (+9.22%)
- `23:27:21`   Alpha   : +59.80%
- `23:27:21` 
- `23:27:21`   Sample nav_curve entries (with SPY):
- `23:27:21`     2026-03-26: strat=$   101442  spy=$97770.54
- `23:27:21`     2026-03-27: strat=$   106971  spy=$96103.36
- `23:27:21`     2026-03-28: strat=$   113514  spy=$96103.36
- `23:27:21`     2026-05-01: strat=$   169230  spy=$109222.49
- `23:27:21`     2026-05-02: strat=$   169133  spy=$109222.49
- `23:27:21`     2026-05-03: strat=$   169018  spy=$109222.49
# 5) backtest.html updates

- `23:27:21`   ✓ 200, 22,262b
- `23:27:21`     ✗ 5 KPI columns
- `23:27:21`     ✗ Alpha vs SPY KPI
- `23:27:21`     ✗ SPY Buy & Hold
- `23:27:21`     ✗ hasSpy chart logic
- `23:27:21`     ✗ legend rendering
