# ops 6534 - audit before the runaway guardrails (read-only)

**Status:** success  
**Duration:** 55.0s  
**Finished:** 2026-10-17T01:30:57+00:00  

## Log
## 1. Account Lambda profile, normal vs storm

- `01:30:04` account Invocations Sum per 5 min | NORMAL {"n": 7776, "p50": 69.0, "p99": 486.0, "p999": 1677.0, "max": 5907.0, "sustained15": 5303.0, "sustained30": 1081.0} | STORM {"n": 1277, "p50": 24867.0, "p99": 35302.0, "p999": 36617.0, "max": 36663.0, "sustained15": 35302.0, "sustained30": 34454.0}
- `01:30:07` account ConcurrentExecutions Maximum per 5 min | NORMAL {"n": 7776, "p50": 8.0, "p99": 47.0, "p999": 101.0, "max": 654.0, "sustained15": 34.0, "sustained30": 25.0} | STORM {"n": 1277, "p50": 764.0, "p99": 767.0, "p999": 776.0, "max": 777.0, "sustained15": 770.0, "sustained30": 766.0}
- `01:30:08` account ConcurrentExecutions Average per 5 min | NORMAL {"n": 7776, "p50": 4.0, "p99": 12.0, "p999": 21.6, "max": 329.3, "sustained15": 17.7, "sustained30": 6.8} | STORM {"n": 1277, "p50": 755.5, "p99": 760.0, "p999": 761.1, "max": 761.3, "sustained15": 759.7, "sustained30": 759.3}
- `01:30:10` account Throttles Sum per 5 min | NORMAL {"n": 7776, "p50": 0.0, "p99": 10.0, "p999": 12.0, "max": 80.0, "sustained15": 6.0, "sustained30": 0.0} | STORM {"n": 1277, "p50": 402.0, "p99": 620.0, "p999": 7362.0, "max": 7393.0, "sustained15": 6909.0, "sustained30": 563.0}
- `01:30:11` normal top 5-min invocation bursts: [('10-05 00:10', 5907), ('10-05 00:20', 5637), ('10-05 00:15', 5303), ('10-05 19:05', 2874), ('10-05 16:20', 2789), ('10-05 16:25', 2406), ('10-05 00:25', 2376), ('10-10 16:30', 1677)]
- `01:30:11` normal top 5-min concurrency peaks: [('10-10 16:30', 654), ('10-08 16:50', 321), ('09-15 13:30', 254), ('09-15 13:35', 116), ('10-06 14:45', 114), ('09-22 07:00', 105), ('09-20 06:55', 104), ('09-17 07:00', 101)]
- `01:30:14` account Duration Sum ms per 5 min | NORMAL {"n": 7776, "p50": 476111.3, "p99": 2421561.6, "p999": 3985307.1, "max": 53254316.5, "sustained15": 4789458.8, "sustained30": 1458374.2} | STORM {"n": 1277, "p50": 226400682.1, "p99": 228192509.9, "p999": 228797887.5, "max": 228822841.7, "sustained15": 227613882.5, "sustained30": 227438704.9}
## 2. Coordinator + route targets, Sep 1 - Oct 11

- `01:30:15` justhodl-event-coordinator     hourly max  336 p99  251 | daily max  1446 | total  19009 | hours active 870
- `01:30:15` justhodl-calibrator            hourly max    3 p99    3 | daily max     6 | total     35 | hours active 29
- `01:30:15` justhodl-alpha-calibrator      hourly max    3 p99    3 | daily max     4 | total     37 | hours active 35
- `01:30:16` justhodl-alpha-compass         hourly max   29 p99   13 | daily max    57 | total   1386 | hours active 433
- `01:30:16` justhodl-signal-board          hourly max    3 p99    3 | daily max     7 | total    181 | hours active 173
- `01:30:16` justhodl-miss-calibrator       hourly max   24 p99   24 | daily max    24 | total     81 | hours active 5
- `01:30:17` justhodl-engine-signal-map     hourly max   42 p99   42 | daily max    43 | total    104 | hours active 49
- `01:30:17` justhodl-jh-fusion             hourly max    3 p99    2 | daily max    30 | total    867 | hours active 824
## 3. Event bus

- `01:30:17` bus justhodl-system-events: 1 rules
- `01:30:18`   rule justhodl-events-to-coordinator state=ENABLED pattern={"source": [{"prefix": "justhodl."}]}
- `01:30:18`      -> justhodl-event-coordinator retry=None dlq=False input=False
- `01:30:18` bus metrics available: ['IngestionToInvocationCompleteLatency', 'IngestionToInvocationStartLatency', 'IngestionToInvocationSuccessLatency', 'InvocationAttempts', 'Invocations', 'MatchedEvents', 'SuccessfulInvocationAttempts', 'TriggeredRules']
- `01:30:18`   bus IngestionToInvocationCompleteLatency Sum/5min NORMAL {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0} | STORM(Oct 13) {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0}
- `01:30:19`   bus IngestionToInvocationStartLatency Sum/5min NORMAL {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0} | STORM(Oct 13) {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0}
- `01:30:20`   bus IngestionToInvocationSuccessLatency Sum/5min NORMAL {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0} | STORM(Oct 13) {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0}
- `01:30:21`   bus InvocationAttempts Sum/5min NORMAL {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0} | STORM(Oct 13) {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0}
- `01:30:21`   bus Invocations Sum/5min NORMAL {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0} | STORM(Oct 13) {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0}
- `01:30:22`   bus MatchedEvents Sum/5min NORMAL {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0} | STORM(Oct 13) {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0}
- `01:30:23`   bus SuccessfulInvocationAttempts Sum/5min NORMAL {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0} | STORM(Oct 13) {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0}
- `01:30:23`   bus TriggeredRules Sum/5min NORMAL {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0} | STORM(Oct 13) {"n": 0, "p50": 0, "p99": 0, "p999": 0, "max": 0, "sustained15": 0, "sustained30": 0}
## 4. Existing alarms

- `01:30:24` 7 related alarms
- `01:30:24`   enhanced-openbb-handler-Throttles [OK] AWS/Lambda/Throttles Sum GreaterThanThreshold 1.0 x1 actions=[]
- `01:30:24`   indexToOpenSearch-Throttles [OK] AWS/Lambda/Throttles Sum GreaterThanThreshold 1.0 x1 actions=[]
- `01:30:24`   justhodl-guard-lambda-concurrency [OK] AWS/Lambda/ConcurrentExecutions Maximum GreaterThanThreshold 150.0 x5 actions=['justhodl-fleet-alerts']
- `01:30:24`   justhodl-guard-lambda-invocations-1h [OK] AWS/Lambda/Invocations Sum GreaterThanThreshold 20000.0 x1 actions=['justhodl-fleet-alerts']
- `01:30:24`   justhodl-spend-governor-errors [OK] AWS/Lambda/Errors Sum GreaterThanOrEqualToThreshold 1.0 x1 actions=['justhodl-fleet-alerts']
- `01:30:24`   justhodl-spend-governor-heartbeat [OK] JustHodl/SpendGovernor/Heartbeat SampleCount LessThanThreshold 1.0 x3 actions=['justhodl-fleet-alerts']
- `01:30:24`   scrapeMacroData-Throttles [OK] AWS/Lambda/Throttles Sum GreaterThanThreshold 1.0 x1 actions=[]
## 5. Cost Anomaly Detection + Budgets

- `01:30:24` anomaly monitor Default-Services-Monitor type=DIMENSIONAL dim=SERVICE arn=...401975da073d
- `01:30:24` anomaly subscription Default-Services-Subscription freq=DAILY threshold=None expr={"And": [{"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_ABSOLUTE", "Values": ["100.0"], "MatchOptions": ["GREATER_THAN_OR_EQUAL"]}}, {"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_PERCENTAGE", "Values": [ subscribers=[('EMAIL', 'r***@gmail.com', 'CONFIRMED')] monitors=1
- `01:30:24` budget JustHodl-Monthly-Hard-Cap-500 MONTHLY COST 500.0
- `01:30:25`    notify ACTUAL GREATER_THAN 100.0% -> [('EMAIL', 'r***@gmail.com'), ('SNS', 'arn:aws:sns:us-east-1:857687956942:justhodl-spend-governor')]
- `01:30:25`    notify ACTUAL GREATER_THAN 50.0% -> [('EMAIL', 'r***@gmail.com'), ('SNS', 'arn:aws:sns:us-east-1:857687956942:justhodl-spend-governor')]
- `01:30:25`    notify ACTUAL GREATER_THAN 80.0% -> [('EMAIL', 'r***@gmail.com'), ('SNS', 'arn:aws:sns:us-east-1:857687956942:justhodl-spend-governor')]
- `01:30:25`    notify ACTUAL GREATER_THAN 90.0% -> [('EMAIL', 'r***@gmail.com'), ('SNS', 'arn:aws:sns:us-east-1:857687956942:justhodl-spend-governor')]
- `01:30:26`    notify FORECASTED GREATER_THAN 100.0% -> [('EMAIL', 'r***@gmail.com'), ('SNS', 'arn:aws:sns:us-east-1:857687956942:justhodl-spend-governor')]
- `01:30:26` budget My Monthly Cost Budget MONTHLY COST 150.0
- `01:30:26`    notify ACTUAL GREATER_THAN 100.0% -> [('EMAIL', 'r***@gmail.com')]
- `01:30:26`    notify ACTUAL GREATER_THAN 50.0% -> [('EMAIL', 'r***@gmail.com')]
- `01:30:27`    notify ACTUAL GREATER_THAN 80.0% -> [('EMAIL', 'r***@gmail.com')]
- `01:30:27`    notify ACTUAL GREATER_THAN 85.0% -> [('EMAIL', 'r***@gmail.com')]
- `01:30:27`    notify FORECASTED GREATER_THAN 100.0% -> [('EMAIL', 'r***@gmail.com')]
## 6. SNS topics

- `01:30:28` topic justhodl-fleet-alerts: [('email', 'r***@gmail.com', 'PENDING'), ('lambda', 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-guardrail-notify', 'confirmed')]
- `01:30:28` topic justhodl-spend-governor: [('lambda', 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-spend-governor', 'confirmed')]
## 7. Bucket

- `01:30:28` justhodl-dashboard-live top-level prefixes: ['13f-cache/', '13f/', '_audit/', '_health/', '_ops/', 'air/', 'analytics/', 'archive/', 'asia/', 'audit-private/', 'aws/', 'backtest/', 'backups/', 'base-rates/', 'boom/', 'bot/', 'calibration/', 'cb-cache/', 'chokepoint/', 'config/', 'cot/', 'credit/', 'data-census/', 'data/', 'deploy/', 'discovery/', 'divergence/', 'domain-barometers/', 'edgar-insiders/', 'equity-critique/', 'equity-prewarm/', 'equity-research-history/', 'equity-research/', 'errors/', 'estimate-revisions/', 'etf-constituents-v2/', 'etf-constituents/', 'etf-flows/', 'factory/', 'flow-anomalies/', 'foreign-flows/', 'geo/', 'history/', 'index/', 'intel/', 'investor-analysis/', 'investor-debate/', 'js/', 'khalid/', 'lake/', 'learning/', 'llm-cache/', 'macro-attribution/', 'macro/', 'opportunities/', 'ops/', 'pboc/', 'plumbing-composite/', 'portfolio/', 'readthrough/', 'regime/', 'reports/', 'risk/', 'screener/', 'sec-filings-cache/', 'sec/', 'secretary/', 'sentiment/', 'signals/', 'spx-beaters/', 'spx-ma/', 'state/', 'stock-ai/', 'stock-analysis/', 'stock/', 'symbol-resolver/', 'system-events/', 'telegram/', 'tools/', 'transcripts/', 'valuations-archive/']
- `01:30:28` versioning: Enabled
- `01:30:28`   lifecycle archive-to-glacier-deep-after-90d [Enabled] prefix='archive/' noncurrent=None newer_kept=None expiration=None abort_mpu=True
- `01:30:28`   lifecycle expire-old-versions-after-30d [Enabled] prefix=None noncurrent=None newer_kept=None expiration=None abort_mpu=True
- `01:30:28`   lifecycle expire-screener-snapshots-30d [Enabled] prefix='screener/snapshots/' noncurrent=None newer_kept=None expiration={'Days': 30} abort_mpu=False
- `01:30:28`   lifecycle jh-noncurrent-14d [Enabled] prefix=None noncurrent=None newer_kept=None expiration=None abort_mpu=True
- `01:30:28`   lifecycle justhodl-e9-warm-ia [Enabled] prefix='data/warm/' noncurrent=None newer_kept=None expiration=None abort_mpu=False
- `01:30:28`   lifecycle justhodl-e9-raw-glacier-ir [Enabled] prefix='data/raw/' noncurrent=None newer_kept=None expiration=None abort_mpu=False
- `01:30:28`   lifecycle justhodl-e9-attic-ia [Enabled] prefix='data/attic/' noncurrent=None newer_kept=None expiration=None abort_mpu=False
- `01:30:28`   lifecycle ops5027-purge-dead-versions-providers [Enabled] prefix='data/providers/' noncurrent=1 newer_kept=None expiration={'ExpiredObjectDeleteMarker': True} abort_mpu=True
- `01:30:28`   lifecycle ops5164-purge-dead-versions-data [Enabled] prefix='data/' noncurrent=1 newer_kept=None expiration={'ExpiredObjectDeleteMarker': True} abort_mpu=False
- `01:30:28`   lifecycle ops6520-purge-dead-versions-system-events [Enabled] prefix='system-events/' noncurrent=1 newer_kept=None expiration={'ExpiredObjectDeleteMarker': True} abort_mpu=False
- `01:30:28` justhodl-dashboard-live notifications: lambda=[('openbb-websocket-broadcast', ['s3:ObjectCreated:*'], '{"Key": {"FilterRules": [{"Name": "Prefix", "Value": "data/report.json"}]}}'), ('openbb-websocket-broadcast', ['s3:ObjectCreated:*'], '{"Key": {"FilterRules": [{"Name": "Prefix", "Value": "data/macro-nowcast.json"}]}}'), ('openbb-websocket-broadcast', ['s3:ObjectCreated:*'], '{"Key": {"FilterRules": [{"Name": "Prefix", "Value": "data/compound-signals.json"}]}}'), ('openbb-websocket-broadcast', ['s3:ObjectCreated:*'], '{"Key": {"FilterRules": [{"Name": "Prefix", "Value": "data/cross-asset-regime.json"}]}}'), ('openbb-websocket-broadcast', ['s3:ObjectCreated:*'], '{"Key": {"FilterRules": [{"Name": "Prefix", "Value": "data/options-flow.json"}]}}'), ('openbb-websocket-broadcast', ['s3:ObjectCreated:*'], '{"Key": {"FilterRules": [{"Name": "Prefix", "Value": "data/eurodollar-stress.json"}]}}'), ('openbb-websocket-broadcast', ['s3:ObjectCreated:*'], '{"Key": {"FilterRules": [{"Name": "Prefix", "Value": "data/nobrainers.json"}]}}'), ('openbb-websocket-broadcast', ['s3:ObjectCreated:*'], '{"Key": {"FilterRules": [{"Name": "Prefix", "Value": "data/narrative-density.json"}]}}')] sqs=0 sns=0 eventbridge=True
- `01:30:28` justhodl-ops-private-857687956942 notifications: lambda=[] sqs=0 sns=0 eventbridge=False
- `01:30:29` BucketSizeBytes TB by day: [('09-17', 0.68), ('09-18', 0.69), ('09-19', 0.67), ('09-20', 0.68), ('09-21', 0.71), ('09-22', 0.86), ('09-23', 0.71), ('09-24', 0.67), ('09-25', 0.63), ('09-26', 0.64), ('09-27', 0.66), ('09-28', 0.7), ('09-29', 0.84), ('09-30', 0.7), ('10-01', 0.71), ('10-02', 0.73), ('10-03', 0.75), ('10-04', 0.77), ('10-05', 0.82), ('10-06', 0.97), ('10-07', 0.82), ('10-08', 0.84), ('10-09', 0.86), ('10-10', 0.88), ('10-11', 0.89), ('10-12', 0.92), ('10-13', 24.65), ('10-14', 61.15), ('10-15', 105.87), ('10-16', 145.39)]
## 8. Coordinator role

- `01:30:29` lambda-execution-role attached=['justhodl-reconcile-daily-internals', 'justhodl-reconcile-six-brief-schedules', 'AmazonSSMFullAccess', 'AmazonSESFullAccess', 'AWSLambdaBasicExecutionRole', 'AWSLambdaRole', 'AmazonDynamoDBFullAccess', 'AmazonS3FullAccess'] inline=['a2a-wake-backend-agent', 'api-keys-admin-permissions', 'Audit20260909PrivateArtifactIdentity', 'ddb-push-subscriptions', 'ddb-websocket-connections', 'execute-api-manage-connections', 'fleet-monitor-perms', 'HealthMonitorEventBridgeRead', 'HealthMonitorTelegramSSM', 'jh-llm-cost-governance', 'jh-schedule-reconciler', 'justhodl-ai-sagemaker-control', 'justhodl-dlq-send', 'justhodl-fleet-monitor-perm', 'justhodl-owned-inference-invoke', 'justhodl-schedule-selfheal', 'lambda-describe-self-and-peers', 'lambda-publish-events', 'SESPolicy', 'signal-registry-ddb-write', 'smartwake-events', 'ssm-ai-chat-auth-read', 'ssm-justhodl-alerts-read', 'ssm-justhodl-nowcast-state', 'ssm-push-vapid']
- `01:30:29` simulated on table justhodl-event-breaker: [('dynamodb:UpdateItem', 'allowed'), ('dynamodb:GetItem', 'allowed'), ('sns:Publish', 'implicitDeny'), ('dynamodb:CreateTable', 'allowed')]
- `01:30:30` justhodl-spend-governor-role inline=['justhodl-spend-governor']
## 9. Lambda side

- `01:30:30` account concurrency limit=1000 unreserved=999
- `01:30:30` 0 event source mappings: []
- `01:30:30`   justhodl-event-coordinator     reserved=0 eic=default(2 retries, 6h)
- `01:30:31`   justhodl-calibrator            reserved=0 eic=default(2 retries, 6h)
- `01:30:31`   justhodl-alpha-calibrator      reserved=0 eic=default(2 retries, 6h)
- `01:30:31`   justhodl-alpha-compass         reserved=0 eic=default(2 retries, 6h)
- `01:30:31`   justhodl-signal-board          reserved=0 eic=default(2 retries, 6h)
- `01:30:32`   justhodl-miss-calibrator       reserved=0 eic=default(2 retries, 6h)
- `01:30:32`   justhodl-engine-signal-map     reserved=0 eic=default(2 retries, 6h)
- `01:30:32`   justhodl-jh-fusion             reserved=0 eic=default(2 retries, 6h)
- `01:30:57` 926 functions; RecursiveLoop settings: {'Terminate': 924, 'Allow': 2}; Allow on: ['justhodl-backend-agent', 'justhodl-a2a-bus']
- `01:30:57` ✅ audit complete
