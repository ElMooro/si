# === justhodl-asymmetric-scorer ===

**Status:** success  
**Duration:** 4.9s  
**Finished:** 2026-05-04T20:08:09+00:00  

## Log
- `20:08:04`   state: Active  mem=256MB  timeout=60s
- `20:08:04`   last modified: 2026-04-27T21:56:15.000+0000
- `20:08:04`   handler: lambda_function.lambda_handler
- `20:08:04`   env: []
- `20:08:05`   source file: lambda_function.py  (26,962 chars)
- `20:08:05`   S3 keys referenced in code:
- `20:08:05` 
- `20:08:05`   S3 keys matching pattern:
- `20:08:05` 
- `20:08:05`   Smoke invoke justhodl-asymmetric-scorer:
- `20:08:07`     status: 200, duration: 1.9s
- `20:08:07`     response: {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": "{\"n_setups\": 94, \"n_value_traps\": 21, \"n_new_this_week\": 0, \"n_dropped_this_week\": 0, \"top_5_symbols\": [\"INCY\", \"CF\", \"MU\", \"NEM\", \"NVDA\"], \"sector_breakdown\": {\"Healthcare\": 12, \"Basic Materials\": 6, \"Technology\": 31, \"Energy\": 2, \"Consumer Cyclical\": 5, \"Communication Services\": 6, \"Real Estate\": 2, \"Industrials\": 16, \"Financial Services\": 11, \"Consumer Defensive\": 2, \"Utili
- `20:08:07` 
# === justhodl-risk-sizer ===

- `20:08:07`   state: Active  mem=256MB  timeout=60s
- `20:08:07`   last modified: 2026-04-25T18:45:30.000+0000
- `20:08:07`   handler: lambda_function.lambda_handler
- `20:08:07`   env: []
- `20:08:07`   source file: lambda_function.py  (20,204 chars)
- `20:08:07`   S3 keys referenced in code:
- `20:08:07` 
- `20:08:07`   S3 keys matching pattern:
- `20:08:07` 
- `20:08:07`   Smoke invoke justhodl-risk-sizer:
- `20:08:09`     status: 200, duration: 1.8s
- `20:08:09`     response: {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": "{\"regime\": \"NEUTRAL\", \"max_gross_exposure_pct\": 75.0, \"current_drawdown_pct\": -0.2, \"drawdown_multiplier\": 1.0, \"n_ideas\": 30, \"n_clusters\": 15, \"total_size_pct\": 75.01, \"n_warnings\": 1, \"top_5_sized\": [{\"symbol\": \"MU\", \"size_pct\": 3.82}, {\"symbol\": \"NEM\", \"size_pct\": 3.73}, {\"symbol\": \"NVDA\", \"size_pct\": 3.58}, {\"symbol\": \"EXE\", \"size_pct\": 3.56}, {\"symbol\": \"FSLR\", \"si
- `20:08:09` 
