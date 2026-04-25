# Re-run legacy retirement with ttl-keyword fix

**Status:** failure  
**Duration:** 33.2s  
**Finished:** 2026-04-25T20:45:41+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Re-scan correct=None outcomes

- `20:45:08`   Found 4410 correct=None outcomes
## 2. Tag with is_legacy=true, legacy_reason, #ttl=now+30d

- `20:45:08`   Target TTL: 1779741908 (2026-05-25T20:45:08+00:00)
- `20:45:41` 
  Tagged: 0
- `20:45:41`   Failed: 4410
- `20:45:41`   Sample errors:
- `20:45:41`     fdfa64fe-acef-44f9-809b-be177b: An error occurred (AccessDeniedException) when calling the UpdateItem operation: User: arn:aws:iam::857687956942:user/gi
- `20:45:41`     c2447ef6-3f05-43ee-9ebc-3dc49a: An error occurred (AccessDeniedException) when calling the UpdateItem operation: User: arn:aws:iam::857687956942:user/gi
- `20:45:41`     035715a5-ba9e-44ad-8ebd-aa9ab0: An error occurred (AccessDeniedException) when calling the UpdateItem operation: User: arn:aws:iam::857687956942:user/gi
- `20:45:41` ✗   ❌ All updates failed
