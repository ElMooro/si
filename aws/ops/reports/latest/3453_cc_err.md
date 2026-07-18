- `04:56:14` FunctionError=Unhandled payload={"errorMessage": "'list' object has no attribute 'get'", "errorType": "AttributeError", "requestId": "360ff0c4-f0b4-4c5d-8276-d81f9bffe045", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 119, in lambda_handler\n    L, comp = score_lenses(docs)\n", "  File \"/var/task/lambda_function.py\", line 75, in score_lenses\n    corp = ((sf.get(\"classes\") or {}).get(\"corporate\")\n"]}
**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-07-18T04:56:14+00:00  

## Error

```
SystemExit: 0
```

## Log

