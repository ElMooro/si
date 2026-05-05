# #3 — Calls-backtest deployment state

**Status:** success  
**Duration:** 0.6s  
**Finished:** 2026-05-05T12:33:06+00:00  

## Log
- `12:33:05`   ✗ Lambda DOES NOT EXIST — needs creation
- `12:33:05` 
- `12:33:05`   EventBridge schedule:
- `12:33:06`     ✗ No EventBridge rule targets this Lambda
- `12:33:06` 
- `12:33:06`   S3 output:
- `12:33:06`     ✗ An error occurred (404) when calling the HeadObject operation: Not Found
- `12:33:06` 
- `12:33:06`   backtest.html calls-backtest section:
- `12:33:06`     ✗ An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `12:33:06` 
- `12:33:06`   Local source (aws/lambdas/justhodl-calls-backtest/source/lambda_function.py):
- `12:33:06`     ✗ [Errno 2] No such file or directory: 'aws/lambdas/justhodl-calls-backtest/source/lambda_function.py'
# #4 — Realistic backtest improvements (slippage / gross cap / leverage cost)

- `12:33:06`     ✗ slippage constant
- `12:33:06`     ✗ gross exposure cap
- `12:33:06`     ✗ leverage cost
- `12:33:06`     ✗ concentration cap
- `12:33:06`     ✓ realistic flag
- `12:33:06`     ✗ v1.2 marker
- `12:33:06` 
- `12:33:06`   Current backtest/summary.json:
- `12:33:06`     method: None
- `12:33:06`     total_return_pct: None
- `12:33:06`     sharpe_ratio: None
- `12:33:06`     n_horizon_weighted: None
- `12:33:06`     has slippage_bps: False
- `12:33:06`     has gross_exposure_cap_pct: False
- `12:33:06`     has v1_2_realistic: False
