# ops 3141 — Telegram send-path forensics

**Status:** success  
**Duration:** 8.0s  
**Finished:** 2026-07-12T03:48:51+00:00  

## Error

```
SystemExit: 0
```

## Data

| n_fails | n_warns | verdict |
|---|---|---|
| 0 | 0 | PASS |

## Log
## 1. Runner-side replay (identical payload)

- `03:48:43` runner replay HTTP 403: {"ok":false,"error_code":403,"description":"Forbidden: bot can't initiate conversation with a user"}
## 2. Lambda test invoke + CloudWatch tail

- `03:48:44` invoke payload: {"statusCode": 200, "body": "{\"telegram\": false}"}
- `03:48:51` CW: no 'telegram' lines in window
## 3. Verdict

- `03:48:51` payload itself rejected — CW irrelevant; fix per the HTTP body above
