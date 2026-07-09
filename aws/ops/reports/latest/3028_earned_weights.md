## 1. Ensure engine exists + settled

**Status:** failure  
**Duration:** 33.3s  
**Finished:** 2026-07-09T23:44:44+00:00  

## Error

```
SystemExit: 1
```

## Data

| action | fn_exists | invoke | n_fails | n_warns | verdict |
|---|---|---|---|---|---|
| boto3 create_function fallback | False |  |  |  |  |
|  |  | {"errorMessage": "'str' object has no attribute 'get'", "errorType": "AttributeError", "requestId": "d91b925d-b4a1-4755-b4f6-37e4c11a1a43", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 310, in lambda_handler\n    proxies = build_proxies()\n", "  File \"/var/task/lambda_function.py\" |  |  |  |
|  |  |  | 1 | 0 | FAIL |

## Log
## 2. Event study run

