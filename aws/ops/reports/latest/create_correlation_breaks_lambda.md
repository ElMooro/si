# Phase 9.5 — bootstrap justhodl-correlation-breaks

**Status:** success  
**Duration:** 0.8s  
**Finished:** 2026-04-26T21:50:05+00:00  

## Log
## 1. Locate source

- `21:50:04`   SOURCE_DIR: /home/runner/work/si/si/aws/lambdas/justhodl-correlation-breaks/source
- `21:50:04`   Found 2 Python files: ['ka_aliases.py', 'lambda_function.py']
## 2. Build deployment zip

- `21:50:04`   zip size: 9040 bytes
## 3. Create or update Lambda

- `21:50:04`   Lambda doesn't exist — creating fresh
- `21:50:05`   ✅ created Lambda: arn:aws:lambda:us-east-1:857687956942:function:justhodl-correlation-breaks
- `21:50:05`      CodeSha256: z6Jbv7sTNfPvYgfU+CmansYYfwKg+zRdFdC1gMyHD/I=
## 4. Create EventBridge schedule

- `21:50:05`   ✅ rule justhodl-correlation-breaks-refresh rate(1 day) ENABLED
## 5. Grant EB → Lambda invoke permission

- `21:50:05`   ✅ permission added: AllowExecutionFromEventBridge9_5
## 6. Wire EB target → Lambda

- `21:50:05`   ✅ target wired: rule=justhodl-correlation-breaks-refresh → arn:aws:lambda:us-east-1:857687956942:function:justhodl-correlation-breaks
## 7. Manual invoke to seed first output

- `21:50:05`   invoking Lambda to produce data/correlation-breaks.json (first run)...
- `21:50:05` ⚠   ✗ invoke error: An error occurred (ResourceConflictException) when calling the Invoke operation: The operation cannot be performed at this time. The function is currently in the following state: Pending
## FINAL

- `21:50:05`   Phase 9.5 producer Lambda is live.
- `21:50:05`   Output: s3://justhodl-dashboard-live/data/correlation-breaks.json
- `21:50:05`   Schedule: daily (rate(1 day))
- `21:50:05`   Next: build correlation.html frontend (separate commit)
- `21:50:05` Done
