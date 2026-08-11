# ops 4596 — fleet re-gate (sync, crashes visible)

**Status:** failure  
**Duration:** 98.0s  
**Finished:** 2026-08-11T00:08:58+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Settle the hardening deploys

## 2. Sync invoke + gate, all nine

- `00:07:30` ✅   [13f-price-divergence] refreshed, state=INSUFFICIENT_DATA, ds={'feeder_loaded': True, 'n_feeder_tickers': 0}
- `00:07:32` ✅   [catalyst-skew-premove] refreshed, state=INSUFFICIENT_DATA, ds={'n_events_in_window': 413, 'n_with_options_data': 0}
- `00:07:38` ✅   [earnings-iv-crush] refreshed, state=QUIET, ds={'http_ok': 121, 'http_err': 0}
- `00:07:38` ✗   [failed-pattern-reversal] CONTRACT MISS — handler error: {"statusCode": 500, "body": "{\"error\": \"'INSUFFICIENT_DATA'\", \"trace\": \"Traceback (most recent call last):\\n  File \\\"/var/task/lambda_function.py\\\", line 431, in lambda_handler\\n    \\\"forward_expectations\\\": priors[state],\\n                  
- `00:07:39` ✅   [forced-selling-bounce] refreshed, state=QUIET, ds={'feeds_ok': 20, 'feeds_miss': 4}
- `00:07:41` ✅   [lockup-expiration] refreshed, state=QUIET, ds={'http_ok': 1, 'http_err': 0}
- `00:08:43` ✅   [ma-target-predictor] refreshed, state=QUIET, ds={'http_ok': 250, 'http_err': 0, 'n_results': 50}
- `00:08:57` ✅   [post-earnings-mean-rev] refreshed, state=QUIET, ds={'http_ok': 120, 'http_err': 0}
- `00:08:58` ✅   [vvix-vov-regime] refreshed, state=NEUTRAL, ds={}
## verdict

- `00:08:58` ✗ fleet re-gate: 1 red
