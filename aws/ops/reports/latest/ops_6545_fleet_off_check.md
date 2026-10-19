# ops 6545 - is every engine off? (read 2026-10-19 22:33 UTC)

**Status:** success  
**Duration:** 40.2s  
**Finished:** 2026-10-19T22:34:25+00:00  

## Log
## Governor (the switch that holds the fleet off)

- `22:33:46` ✅ governor v1.1.1: stage FREEZE since 2026-10-16T22:53:03.699511+00:00, mode enforce, holds {"FREEZE/lambda": 914, "FREEZE/smasync": 1}
- `22:33:46` budget: actual $2794.446 this month, cap $500.0 (budget)
- `22:33:46` ✅ last governor tick 2026-10-19T22:07:37.711070+00:00 (25 min ago)
- `22:33:46` ledger (stage/lever/status): {"FREEZE/lambda/applied": 895, "FREEZE/lambda/reapplied": 19, "FREEZE/smasync/reapplied": 1}
- `22:33:46` ✅ ledger holds not in force: none
- `22:33:46` re-held after someone changed them during the freeze: 19
- `22:33:46`   lambda:justhodl-a2a-bus: changed to {"reserved": 5}, re-held 2026-10-17T01:55:44.912478+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-alpha-calibrator: changed to {"reserved": 2}, re-held 2026-10-17T01:55:44.912478+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-alpha-compass: changed to {"reserved": 3}, re-held 2026-10-17T01:55:44.912478+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-backend-agent: changed to {"reserved": 2}, re-held 2026-10-17T01:55:44.912478+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-calibrator: changed to {"reserved": 2}, re-held 2026-10-17T01:55:44.912478+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-engine-signal-map: changed to {"reserved": 2}, re-held 2026-10-17T01:55:44.912478+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-event-coordinator: changed to {"reserved": 10}, re-held 2026-10-17T01:55:44.912478+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-jh-fusion: changed to {"reserved": 2}, re-held 2026-10-17T01:55:44.912478+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-miss-calibrator: changed to {"reserved": 1}, re-held 2026-10-17T01:55:44.912478+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-signal-board: changed to {"reserved": 2}, re-held 2026-10-17T01:55:44.912478+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-13f-positions: changed to {"reserved": 1}, re-held 2026-10-17T04:07:37.736093+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-chain-resumer: changed to {"reserved": 1}, re-held 2026-10-17T04:07:37.736093+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-crypto-intel: changed to {"reserved": 1}, re-held 2026-10-17T04:07:37.736093+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-dex-scanner: changed to {"reserved": 1}, re-held 2026-10-17T04:07:37.736093+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-price-redundancy: changed to {"reserved": 1}, re-held 2026-10-17T04:07:37.736093+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-repo-monitor: changed to {"reserved": 1}, re-held 2026-10-17T04:07:37.736093+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-sec-13f: changed to {"reserved": 1}, re-held 2026-10-17T04:07:37.736093+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-student-rsi: changed to {"reserved": 1}, re-held 2026-10-17T04:07:37.736093+00:00 (reapplied)
- `22:33:46`   lambda:justhodl-vix-curve: changed to {"reserved": 1}, re-held 2026-10-17T04:07:37.736093+00:00 (reapplied)
- `22:33:46` daily AWS cost (Cost Explorer, as of 2026-10-19T20:07:37.906341+00:00): 10-12 $336.23, 10-13 $515.88, 10-14 $555.54, 10-15 $587.85, 10-16 $537.38, 10-17 $7.94, 10-18 $13.27, 10-19 $5.53
- `22:33:46` ✅ governor schedule justhodl-spend-governor-tick cron(7 * * * ? *) ENABLED
- `22:33:47` governor dead-man alarms: justhodl-spend-governor-errors=OK, justhodl-spend-governor-heartbeat=OK
- `22:33:47` exempt by design (they keep the freeze and the login alive): justhodl-dr-snapshot, justhodl-guardrail-notify, justhodl-spend-governor, justhodl-supabase-keepalive
## Lambda engines

- `22:34:17` before: 926 functions - 922 held at 0, 4 exempt and live (justhodl-dr-snapshot, justhodl-guardrail-notify, justhodl-spend-governor, justhodl-supabase-keepalive), 0 non-exempt able to run, 0 unreadable
- `22:34:17` ✅ every non-exempt function is held at reserved concurrency 0 (922)
- `22:34:18` ✅ endpoint jh-owned-coder-async/owned: InService, async, instances 0 (desired 0), autoscaling 0..1, scale-out suspended True - off
- `22:34:18` ✅ endpoint jh-ai-tensorflow-tcembedding-bert-en-uncased-l-12-h-768/AllTraffic: InService, serverless, instances 0 (desired None) - bills only when called; every caller is frozen
- `22:34:19` ✅ endpoint jh-ai-clf-jh-ai-brain-clf-20260910-180837-297/AllTraffic: InService, serverless, instances 0 (desired None) - bills only when called; every caller is frozen
- `22:34:19` training jobs started since the freeze: none
- `22:34:19` processing jobs started since the freeze: none
- `22:34:19` transform jobs started since the freeze: none
- `22:34:19` tuning jobs started since the freeze: none
- `22:34:19` automl jobs started since the freeze: none
- `22:34:19` notebooks in service 0, Studio apps in service 0 (0 on paid instances)
- `22:34:20` ✅ EC2: nothing running
- `22:34:20` ⚠ ECS unreadable: AccessDeniedException: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: ecs:ListClusters on resource: * because no identity-based policy allo
- `22:34:20` ⚠ RDS unreadable: AccessDenied: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: rds:DescribeDBInstances on resource: arn:aws:rds:us-east-1:8576879569
- `22:34:20` ✅ App Runner: nothing running
- `22:34:21` ✅ Step Functions: nothing running
- `22:34:21` ✅ event source mappings: none enabled on a non-exempt function
## Who actually ran

- `22:34:22` ✅ invocations 22:13-22:33 UTC: non-exempt none; exempt {"justhodl-spend-governor": 5, "justhodl-supabase-keepalive": 1}
- `22:34:24` ✅ no non-exempt function ran in the last 24 h
- `22:34:24` exempt functions, last 24 h: {"justhodl-spend-governor": 29, "justhodl-guardrail-notify": 1, "justhodl-supabase-keepalive": 6, "justhodl-dr-snapshot": 2}
- `22:34:24` the Oct 17 03:30-05:00 UTC gap (another lane set concurrency 1 on 14 functions): {"justhodl-student-rsi": ["03:40=5", "03:45=5", "03:50=5", "03:55=5", "04:00=5", "04:05=3"], "justhodl-chain-resumer": ["03:40=1", "03:45=1", "03:50=1", "03:55=1", "04:00=1", "04:05=1"], "justhodl-sec-13f": ["04:00=1"]}
- `22:34:24` account invocations per hour, last 72 h (UTC): 16 22=211, 16 23=1, 17 00=7, 17 01=27, 17 02=1, 17 03=36, 17 04=1, 17 05=4, 17 06=1, 17 07=1, 17 08=1, 17 09=2, 17 10=1, 17 11=1, 17 12=1, 17 13=2, 17 14=1, 17 15=1, 17 16=1, 17 17=2, 17 18=1, 17 19=3, 17 20=1, 17 21=2, 17 22=1, 17 23=2, 18 00=1, 18 01=2, 18 02=1, 18 03=1, 18 04=1, 18 05=4, 18 06=1, 18 07=1, 18 08=1, 18 09=2, 18 10=1, 18 11=1, 18 12=1, 18 13=2, 18 14=1, 18 15=1, 18 16=1, 18 17=2, 18 18=1, 18 19=2, 18 20=1, 18 21=2, 18 22=1, 18 23=2, 19 00=1, 19 01=2, 19 02=1, 19 03=1, 19 04=1, 19 05=4, 19 06=1, 19 07=1, 19 08=1, 19 09=2, 19 10=1, 19 11=1, 19 12=1, 19 13=2, 19 14=1, 19 15=1, 19 16=1, 19 17=2, 19 18=1, 19 19=1, 19 20=1, 19 21=7
- `22:34:24` account throttles, last 24 h (scheduled runs refused by the hold): 9464
- `22:34:24` account max concurrent executions, last 24 h: 2
## Schedules and what still bills

- `22:34:25` rules another lane switched off on Oct 17 (the Nov 1 restore will NOT re-enable them): {"justhodl-daily-8am": "DISABLED", "justhodl-vix-curve-30min": "DISABLED", "justhodl-katlin-permission-refresh": "DISABLED", "justhodl-crypto-15min": "DISABLED", "justhodl-repo-30min": "DISABLED", "justhodl-dex-scanner-15min": "DISABLED", "justhodl-sec-8k-30min": "DISABLED", "justhodl-tv-bars-hourly": "DISABLED", "justhodl-v9-auto-refresh": "missing"}
- `22:34:25` EventBridge Scheduler schedules still enabled: 430 (they fire into held functions and are refused)
- `22:34:25` still billing 2026-10-16: $537.38 - AWS Lambda $377.43, Amazon Simple Storage Service $137.00, AmazonCloudWatch $10.66, CloudWatch Events $6.92, Amazon Simple Queue Service $2.84, AWS Cost Explorer $1.61, Amazon SageMaker $0.45, Amazon DynamoDB $0.21, AWS Secrets Manager $0.12, Amazon EC2 Container Registry (ECR) $0.09
- `22:34:25` still billing 2026-10-17: $7.94 - Amazon Simple Storage Service $7.31, AmazonCloudWatch $0.23, AWS Secrets Manager $0.12, Amazon EC2 Container Registry (ECR) $0.09, AWS Cost Explorer $0.08
- `22:34:25` still billing 2026-10-18: $13.27 - Amazon Simple Storage Service $12.71, AmazonCloudWatch $0.19, AWS Secrets Manager $0.12, Amazon EC2 Container Registry (ECR) $0.09
- `22:34:25` still billing 2026-10-19: $5.53 - Amazon Simple Storage Service $5.37, AmazonCloudWatch $0.05
- `22:34:25` ✅ ECS and RDS: the runner cannot list them, and Cost Explorer shows $0.00 for them since the freeze
## Verdict

- `22:34:25` ✅ ALL ENGINES OFF - every non-exempt Lambda held at reserved concurrency 0; no SageMaker / EC2 / App Runner / Step Functions compute up; ECS / RDS off by billing ($0.00 since the freeze); governor FREEZE (auto-restore 2026-11-01 00:07 UTC)
