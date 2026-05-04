# Create justhodl-skew-engine + hourly schedule

**Status:** success  
**Duration:** 4.4s  
**Finished:** 2026-05-04T17:13:59+00:00  

## Log
- `17:13:54`   zip size: 4,155b
- `17:13:55` ✅   ✓ created
# EventBridge schedule (hourly)

- `17:13:58` ✅   ✓ wired
# Smoke test

- `17:13:59`   status: 200  duration: 1.0s
- `17:13:59`   resp: {"statusCode": 200, "body": "{\"duration_s\": 0.21, \"underlyings\": [\"SPY\", \"QQQ\", \"IWM\"], \"summary\": {\"spy_skew_regime\": null, \"spy_term_structure\": null, \"spy_front_atm_iv\": null, \"spy_front_risk_reversal\": null}}"}
# S3 verify

- `17:13:59`   duration_s: 0.21
- `17:13:59`   summary: {'spy_skew_regime': None, 'spy_term_structure': None, 'spy_front_atm_iv': None, 'spy_front_risk_reversal': None}
- `17:13:59`   SPY: ERROR no_chain
- `17:13:59`   QQQ: ERROR no_chain
- `17:13:59`   IWM: ERROR no_chain
