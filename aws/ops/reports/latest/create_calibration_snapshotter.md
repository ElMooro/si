# 1) Create justhodl-calibration-snapshotter Lambda

**Status:** success  
**Duration:** 6.7s  
**Finished:** 2026-05-04T20:44:03+00:00  

## Log
- `20:43:56`   zip size: 2,934b
- `20:43:56` ✅   ✓ created
- `20:44:01`   state: Active, last update: Successful
# 2) EventBridge schedule — Sundays 12:00 UTC

- `20:44:01` ✅   ✓ wired (justhodl-calibration-snapshotter-weekly → Sundays 12:00 UTC)
# 3) Bootstrap — invoke now to seed first snapshot

- `20:44:02`   status: 200, duration: 1.2s
- `20:44:02`   resp: {"errorMessage": "float() argument must be a string or a real number, not 'dict'", "errorType": "TypeError", "requestId": "f9a621e3-c8d7-4dd7-8379-b9dea2a578af", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 131, in lambda_handler\n    num += float(w) * float(a)\n"]}
# 4) Verify outputs

- `20:44:02`   ✗ calibration/history-index.json: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist.
- `20:44:03` 
- `20:44:03`   ✓ calibration/latest.json
- `20:44:03`     iso_week: None (None → None)
- `20:44:03`     n_weights: None
- `20:44:03`     n_calibrated_n30: None
- `20:44:03`     highest_weight: None
- `20:44:03`     median_weight: None
- `20:44:03`     weighted_mean_accuracy: None
- `20:44:03` 
- `20:44:03`     Top 8 weights (preview):
- `20:44:03`       edge_composite                    w=1.340  acc=      —  n=0
- `20:44:03`       plumbing_stress                   w=1.340  acc=      —  n=0
- `20:44:03`       crypto_fear_greed                 w=1.110  acc=      —  n=0
- `20:44:03`       khalid_index                      w=1.000  acc=      —  n=0
- `20:44:03`       ka_index                          w=1.000  acc=      —  n=0
- `20:44:03`       screener_top_pick                 w=0.850  acc=      —  n=0
- `20:44:03`       valuation_composite               w=0.800  acc=      —  n=0
- `20:44:03`       cftc_gold                         w=0.800  acc=      —  n=0
