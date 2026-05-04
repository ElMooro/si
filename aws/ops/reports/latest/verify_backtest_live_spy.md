# 1) Wait 30s for cache propagation, then fetch with cache-bust

**Status:** success  
**Duration:** 30.5s  
**Finished:** 2026-05-04T23:33:00+00:00  

## Log
- `23:32:59`   cache-busted fetch: 200, 23,667b
- `23:32:59`     ✓ 5 KPI grid
- `23:32:59`     ✓ Alpha vs SPY KPI
- `23:32:59`     ✗ SPY Buy & Hold KPI
- `23:32:59`     ✓ hasSpy chart logic
- `23:32:59`     ✓ Strategy NAV legend
# 2) Check if backtest.html exists in S3

- `23:32:59`   ✗ Not in S3 bucket: An error occurred (404) when calling the HeadObject operation: Not Found
- `23:32:59`   → backtest.html is served from GitHub Pages, not S3
# 3) Confirm backtest data has SPY benchmark in S3

- `23:33:00`   ✗ cannot access local variable 'head' where it is not associated with a value
