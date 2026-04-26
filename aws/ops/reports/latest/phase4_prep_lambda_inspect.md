# Phase 4 prep — inspect justhodl-khalid-metrics

**Status:** success  
**Duration:** 12.0s  
**Finished:** 2026-04-26T13:29:02+00:00  

## Log
## A. justhodl-khalid-metrics configuration

- `13:28:51`   ARN: arn:aws:lambda:us-east-1:857687956942:function:justhodl-khalid-metrics
- `13:28:51`   runtime: python3.12  memory: 512MB  timeout: 240s
- `13:28:51`   state: Active  last_modified: 2026-04-25T10:27:46
- `13:28:51`   role: lambda-execution-role
- `13:28:51`   code size: 7514B
- `13:28:51`   handler: lambda_function.lambda_handler
- `13:28:51`   env vars (4): ['FRED_API_KEY', 'POLYGON_API_KEY', 'S3_BUCKET', 'ANTHROPIC_API_KEY']
- `13:28:51`   Function URL: https://2ijajv2pntkgj5yw5c3ukh5oq40xsyaf.lambda-url.us-east-1.on.aws/ auth=NONE
- `13:28:51`   reserved concurrency: unset
## B. EventBridge rules targeting justhodl-khalid-metrics

- `13:29:01`   1 rules:
- `13:29:01`     name=justhodl-khalid-metrics-refresh                     state=ENABLED     sched=cron(0 11 * * ? *)
- `13:29:01`       target_id=khalid-metrics  input=
## C. justhodl-khalid-metrics source — S3 keys written/read

- `13:29:01`   zip files: ['lambda_function.py']
- `13:29:01`   S3 writes (3): ['data/khalid-analysis.json', 'data/khalid-config.json', 'data/khalid-metrics.json']
- `13:29:01`   S3 reads  (6): ['data/khalid-analysis.json', 'data/khalid-config.json', 'data/khalid-metrics.json']
- `13:29:01`   source line count: 369
## D. Frontend pages referencing old Lambda URL

- `13:29:01`   searching repo for token: 2ijajv2pntkgj5yw5c3ukh5oq40xsyaf.lambda-url.us-east-1.on.aws
## E. justhodl-khalid-metrics invocations last 7 days

- `13:29:02`   total invocations 7d: 7
- `13:29:02`   total errors 7d: 0
## F. Does justhodl-ka-metrics already exist?

- `13:29:02`   ✅ justhodl-ka-metrics does not yet exist — safe to create
- `13:29:02` Done
