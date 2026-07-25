# ops 3842 — port confirmation into global-business-cycle

**Status:** failure  
**Duration:** 39.5s  
**Finished:** 2026-07-25T02:22:13+00:00  

## Error

```
SystemExit: 1
```

## Log
## G0. Baseline contract BEFORE the change

- `02:21:33` ✅   34 countries · 23 fields/row
- `02:21:33` ✅   portwatch: 88 ports carry yoy_pct
## 1. Deploy

- `02:21:34`   zip: 98306 bytes
## 1. Lambda

- `02:21:34`   Lambda exists — updating
- `02:21:41` ✅   ✓ updated justhodl-global-business-cycle
## 2. ZIP-SETTLE by marker

- `02:21:52` ✅   settled with 'load_port_physical' after 10s
## 3. Invoke

- `02:22:13` ✗   invoke error: b'{"errorMessage": "cannot access local variable \'_phys_summary\' where it is not associated with a value", "errorType": "UnboundLocalError", "requestId": "48eede6c-5dac-419e-86d7-83abb922fbba", "stackTrace": ["  File \\"/var/task/lambda_function.py\\", line 1120, in lambda_handler\\n    \\"physical_confirmation\\": _phys_summary,\\n"]}'
