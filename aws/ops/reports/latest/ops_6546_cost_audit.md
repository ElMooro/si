# ops 6546 - deep cost audit (read-only)

**Status:** success  
**Duration:** 356.2s  
**Finished:** 2026-10-20T15:46:00+00:00  

## Log
## 1. Billing (Cost Explorer)

- `15:40:07` monthly totals: {'2026-06-01': 241.87, '2026-07-01': 285.41, '2026-08-01': 484.27, '2026-09-01': 633.36, '2026-10-01': 2798.08}
- `15:40:07` last 12 days: [('10-08', 23.53), ('10-09', 24.31), ('10-10', 20.34), ('10-11', 20.69), ('10-12', 336.23), ('10-13', 515.88), ('10-14', 555.54), ('10-15', 587.85), ('10-16', 537.38), ('10-17', 7.94), ('10-18', 13.27), ('10-19', 9.17)]
- `15:40:07` lambda resource-level cost: not enabled
- `15:40:07` elapsed 3s
## 2. Lambda inventory + per-function metrics

- `15:40:18` 926 functions
- `15:40:19` lambda account: {'AccountLimit': {'TotalCodeSize': 322122547200, 'CodeSizeUnzipped': 262144000, 'CodeSizeZipped': 52428800, 'ConcurrentExecutions': 1000, 'UnreservedConcurrentExecutions': 999}, 'AccountUsage': {'TotalCodeSize': 582381836, 'FunctionCount': 926}}
- `15:41:33` daily metric series: 3550 (non-empty)
- `15:41:33` elapsed 90s
## 3. CloudWatch Logs

- `15:41:40` 1245 log groups, stored 19.5 GB, never-expire 173
- `15:41:44` log groups with ingestion since Sep 29: 874
- `15:41:44` elapsed 100s
## 4. Triggers

- `15:41:44` event buses: ['default', 'justhodl-system-events']
- `15:42:35` EventBridge rules: 592 (551 enabled, 589 scheduled)
- `15:42:41` Scheduler schedules: 432 (430 enabled)
- `15:42:42` event source mappings: 0
- `15:43:08` buckets: 49; with event notifications: ['justhodl-dashboard-live']
- `15:43:08` alert subscriptions: [['liquidity-alerts-v2', 'email', 'ra***@gmail.com', 'confirmed'], ['liquidity', 'email', 'ra***@gmail.com', 'confirmed'], ['liquidity-alerts-yahoo-1757821911', 'email', 'kh***@yahoo.com', 'confirmed']]
- `15:43:09` SNS->lambda 2, REST APIs 19, HTTP APIs 24, state machines 3
- `15:43:09` elapsed 185s
## 5. Non-Lambda resources

- `15:43:30` regions with anything: {'us-east-1': {'instances': 2, 'volumes_gb': 16, 'nat': 0, 'eips': 0, 'snap_gb': 0, 'lambdas': 926}}
- `15:44:05` bucket sizes (GB): {'ai-assistant-downloads': {'StandardStorage': 0.3, 'objects': 3}, 'aiapi-historical-data-lake': {'StandardStorage': 0.0, 'objects': 7}, 'autonomous-deploy-857687956942': {'StandardStorage': 0.0, 'objects': 1}, 'daily-liquidity-reports': {'StandardStorage': 0.0, 'objects': 1}, 'financial-intelligence-857687956942': {'StandardStorage': 0.0, 'objects': 3}, 'financial-intelligence-platform-857687956942': {'StandardStorage': 0.0, 'objects': 1}, 'justhodl-ai-857687956942': {'StandardStorage': 31.0, 'objects': 21684}, 'justhodl-backups-857687956942': {'StandardStorage': 0.0, 'objects': 1}, 'justhodl-bloomberg-terminal': {'StandardStorage': 2.2, 'objects': 25596}, 'justhodl-dashboard-live': {'StandardStorage': 952.5, 'StandardIAStorage': 185.3, 'GlacierInstantRetrievalStorage': 1.1, 'objects': 3966082}, 'justhodl-dashboard-live-dr': {'StandardStorage': 0.0, 'StandardIAStorage': 10.9, 'objects': 63893}, 'justhodl-dr-usw2-857687956942': {'StandardStorage': 5.9, 'StandardIAStorage': 1.1, 'objects': 152824}, 'justhodl-historical-data-1758485495': {'StandardStorage': 3.2, 'objects': 5108}, 'justhodl-ops-private-857687956942': {'StandardStorage': 0.0, 'objects': 76}, 'justhodl-s3-access-logs-857687956942': {'StandardStorage': 142.3, 'objects': 11970416}, 'liquidity-agent-data': {'StandardStorage': 0.1, 'objects': 1445}, 'macro-data-lake': {'StandardStorage': 9.2, 'objects': 835}, 'macro-financial-intelligence': {'StandardStorage': 0.2, 'objects': 771}, 'multi-agent-system-1758294711': {'StandardStorage': 0.0, 'objects': 22}, 'my-fred-data-12345': {'StandardStorage': 0.2, 'objects': 105}, 'openbb-dashboard-public': {'StandardStorage': 0.0, 'objects': 4}, 'openbb-data-bucket': {'StandardStorage': 0.0, 'objects': 1}, 'openbb-historical-data': {'StandardStorage': 0.0, 'objects': 1}, 'openbb-lambda-data': {'StandardStorage': 0.0, 'objects': 12}, 'openbb-ml-predictions': {'StandardStorage': 0.0, 'objects': 2}, 'openbb-platform-857687956942': {'StandardStorage': 0.0, 'objects': 5}}
- `15:44:06` budgets: [['JustHodl-Daily-Spend-Guard', '40.0', 'DAILY', '9.169', None], ['JustHodl-Monthly-Hard-Cap-500', '500.0', 'MONTHLY', '2803.622', '2976.884'], ['My Monthly Cost Budget', '150.0', 'MONTHLY', '2803.622', '2976.884']]
- `15:44:10` cost anomaly detection: monitors [['Default-Services-Monitor', 'DIMENSIONAL', 'SERVICE']] | subscriptions [['Default-Services-Subscription', 'DAILY', None, '{"And": [{"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_ABSOLUTE", "Values": ["25.0"], "MatchOptions": ["GREATER_THAN_OR_EQUAL"]}}, {"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_PERCENTAGE", "Values": ["', ['ra***@gmail.com']]] | anomalies since Aug 1: 25
- `15:44:10` budget detail: {"JustHodl-Daily-Spend-Guard": {"notifications": [["ACTUAL", "GREATER_THAN", 100.0, null, [["EMAIL", "ra***@gmail.com"], ["SNS", "justhodl-spend-governor"]]]], "actions": []}, "JustHodl-Monthly-Hard-Cap-500": {"notifications": [["ACTUAL", "GREATER_THAN", 100.0, null, [["EMAIL", "ra***@gmail.com"], ["SNS", "justhodl-spend-governor"]]], ["ACTUAL", "GREATER_THAN", 50.0, null, [["EMAIL", "ra***@gmail.com"], ["SNS", "justhodl-spend-governor"]]], ["ACTUAL", "GREATER_THAN", 80.0, null, [["EMAIL", "ra***@gmail.com"], ["SNS", "justhodl-spend-governor"]]], ["ACTUAL", "GREATER_THAN", 90.0, null, [["EMAIL", "ra***@gmail.com"], ["SNS", "justhodl-spend-governor"]]], ["FORECASTED", "GREATER_THAN", 100.0, null, [["EMAIL", "ra***@gmail.com"], ["SNS", "justhodl-spend-governor"]]]], "actions": []}, "My Monthly Cost Budget": {"notifications": [["ACTUAL", "GREATER_THAN", 100.0, null, [["EMAIL", "ra***@gmail.com"]]], ["ACTUAL", "GREATER_THAN", 50.0, null, [["EMAIL", "ra***@gmail.com"]]], ["ACTUAL", "GREATER_THAN", 80.0, null, [["EMAIL", "ra***@gmail.com"]]], ["ACTUAL", "GREATER_THAN", 85.0, null, [["EMAIL", "ra***@gmail.com"]]], ["FORECASTED", "GREATER_THAN", 100.0, null, [["EMAIL", "ra***@gmail.com"]]]], "actions": []}}
- `15:44:11` governor: stage FREEZE since 2026-10-16T22:53:03.699511+00:00, ledger entries 915
- `15:44:11` elapsed 248s
## 6. S3 requests per engine (server access logs)

- `15:45:16` 2026/10/10: 186698 log objects (x37.3); top by request cost: [('justhodl-provider-catalog', 78525, 2987), ('iam-user:github-actions-justhodl', 16242, 40289), ('justhodl-daily-report-v3', 9446, 57689), ('justhodl-student-rsi', 12172, 15794), ('justhodl-carry-surface', 8886, 12807), ('justhodl-tradingview', 8028, 19192), ('justhodl-a2a-bus', 896, 106865), ('justhodl-etf-constituents', 8102, 7281), ('justhodl-fleet-freshness-monitor', 485, 101302), ('justhodl-research-backtest', 0, 97157), ('justhodl-data-census', 7467, 410), ('justhodl-public-archive-index', 6833, 0)]
- `15:45:59` 2026/10/18: 87411 log objects (x17.5); top by request cost: [('iam-user:github-actions-justhodl', 18391, 30891), ('anonymous', 0, 90592), ('justhodl-guardrail-notify', 34, 0), ('AmazonS3', 0, 0)]
- `15:45:59` elapsed 355s
- `15:46:00` data: 492 KB compressed -> ops_6546_cost_audit_data.md
- `15:46:00` errors recorded: 3 ['ce lambda resources (opt-in): AccessDeniedException: Resource-level data granularity is an opt-in only feature. You can be enable this feature from the PAYER account’s Cost Explorer Settings page.', 'ecr: AccessDeniedException: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: ecr:DescribeRepositories on resource: arn:aws:ecr:us-east-1:857687956942:repository/* because no identity-bas', "budget actions JustHodl-Daily-Spend-Guard: InvalidParameterException: AWS Budgets Actions don't support daily granularity budget for now."]
- `15:46:00` ✅ audit complete
