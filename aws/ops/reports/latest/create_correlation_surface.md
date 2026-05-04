# Create justhodl-correlation-surface + smoke test

**Status:** success  
**Duration:** 10.4s  
**Finished:** 2026-05-04T12:42:40+00:00  

## Log
- `12:42:29`   zip size: 4,329b
- `12:42:30` ✅   ✓ created
## EventBridge schedule (daily 15 UTC)

- `12:42:30` ✅   ✓ wired
## Smoke test

- `12:42:39`   status: 200 duration: 1.2s
- `12:42:39`   resp: {"statusCode": 200, "body": "{\"macro_regime\": \"MACRO_ALL_ON\", \"avg_30d_abs_correlation\": 0.572, \"n_regime_breaks\": 23, \"n_decouplings\": 8, \"duration_s\": 0.36}"}
## S3 verify

- `12:42:40`   as_of: None
- `12:42:40`   macro_regime: MACRO_ALL_ON
- `12:42:40`   avg_30d_abs_corr: 0.572
- `12:42:40`   n_regime_breaks: 23
- `12:42:40`   n_decouplings: 8
## 📊 Headline pairs

- `12:42:40` ✗   ✗ unsupported format string passed to NoneType.__format__
