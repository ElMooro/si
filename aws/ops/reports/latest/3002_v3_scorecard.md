## 1. Gates + factor-regime bootstrap

**Status:** failure  
**Duration:** 122.7s  
**Finished:** 2026-07-08T02:33:57+00:00  

## Error

```
SystemExit: 1
```

## Data

| appetite | bs_setups | credit_mentions | factor_page | fr_body | fr_env | fr_err | fr_exists | fr_secs | ir_body | ir_env | ir_err | ir_gate | ir_page_v2 | ir_secs | leading | min_mult | pairs_ok | thrusts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  | 4 |  | True |  |  |  |  |  |  |
|  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | 3 |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  | {"statusCode": 200, "body": "{\"ok\": true, \"risk_appetite\": 45.9, \"thrusts\": 1, \"leading\": 6}"} |  | None |  | 3.8 |  |  |  |  |  |  |  |  |  |  |
| 45.9 |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ["SMALL_SIZE", "EQUAL_WEIGHT_BREADTH", "QUALITY", "MINVOL_DEFENSE", "SPECULATIVE_APPETITE", "BIOTECH_RISK_APPETITE"] |  | 11 | ["USMV/SPY (MINVOL_DEFENSE)"] |
|  |  |  |  |  |  |  |  |  | {"errorMessage": "'ratio_above_50dma'", "errorType": "KeyError", "requestId": "1b16b7ce-7d8d-4428-aaaf-3600851f13bc", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 492, in lambda_handl |  | Unhandled |  |  | 8.9 |  |  |  |  |
|  | 50 | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.88 |  |  |
|  |  |  | True |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |

## Log
- `02:33:18`   rule already correct: factor-regime-daily (cron(28 21 * * ? *))
- `02:33:18` ✅   ✓ target → justhodl-factor-regime
- `02:33:18` ✅   ✓ added invoke permission
## 2. Invoke factor-regime

## 3. Invoke industry-rotation v2

## 4b. Best-setups credit-penalty path

## 4. Pages live

- `02:33:57` FAILS=1 WARNS=0
