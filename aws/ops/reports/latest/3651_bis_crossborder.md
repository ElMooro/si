# ops 3651 — BIS cross-border claims engine + plumbing wire

**Status:** success  
**Duration:** 73.4s  
**Finished:** 2026-07-21T16:04:32+00:00  

## Error

```
SystemExit: 0
```

## Log
- `16:03:19`   zip: 84952 bytes
## 1. Lambda

- `16:03:19`   Lambda exists — updating
- `16:03:25` ✅   ✓ updated justhodl-bis-crossborder
## 3. Smoke test

- `16:03:25`   invoking justhodl-bis-crossborder…
- `16:03:29` G1_bis False
- `16:03:29`   zip: 94783 bytes
## 1. Lambda

- `16:03:29`   Lambda exists — updating
- `16:03:34` ✅   ✓ updated justhodl-eurodollar-plumbing
## 3. Smoke test

- `16:03:34`   invoking justhodl-eurodollar-plumbing…
- `16:03:46` ✗   ✗ FunctionError: Unhandled
- `16:03:46`   body: {"errorMessage": "name 'bis_cb' is not defined", "errorType": "NameError", "requestId": "d5fbfae3-1e27-45e6-b114-6a3d44a0f8ea", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 533, in lambda_handler\n    health, verdict, reds, yellows = composite(layers)\n", "  File \"/var/task/lambda_function.py\", line 459, in composite\n    weight = {\"bis_crossborder\": bis_cb,\n"]}
- `16:03:55` G2_plumbing False
- `16:03:56`   zip: 107400 bytes
## 1. Lambda

- `16:03:56`   Lambda exists — updating
- `16:03:59` ✅   ✓ updated justhodl-morning-intelligence
## 3. Smoke test

- `16:03:59`   invoking justhodl-morning-intelligence…
- `16:04:31` ✅   ✓ smoke test passed
- `16:04:31`     success                  True
- `16:04:31`     khalid_adj               46.0
- `16:04:31`     regime                   NEUTRAL
- `16:04:31`     btc                      66797
- `16:04:31`     outcomes                 17453
- `16:04:31`     improved                 False
- `16:04:31`     weights_active           277
- `16:04:31`     ka_adj                   46.0
- `16:04:32` G3_mi True
- `16:04:32` VERDICT: GAPS: G1_bis,G2_plumbing
