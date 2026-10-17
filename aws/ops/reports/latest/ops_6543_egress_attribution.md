# ops 6543 - who downloads ~57 GB/day from justhodl-dashboard-live (read-only)

**Status:** success  
**Duration:** 195.1s  
**Finished:** 2026-10-17T02:14:54+00:00  

## Log
- `02:11:39` logs: s3://justhodl-s3-access-logs-857687956942/live/857687956942/us-east-1/justhodl-dashboard-live/ format={'PartitionedPrefix': {'PartitionDateSource': 'EventTime'}}
- `02:11:48` 2026/10/05: 50000 objects, 485.0 MB, first 2026-10-05-00-00-00-00005245936FBE8A, last 2026-10-05-00-00-00-48FAFAAA5A3711E7, hours [('2026-10-05-00', 50000)]
- `02:11:57` 2026/10/10: 50000 objects, 277.5 MB, first 2026-10-10-00-00-00-00002515EE0ABFF2, last 2026-10-10-00-00-00-44BB0464D9885300, hours [('2026-10-10-00', 50000)]
- `02:12:06` 2026/10/15: 50000 objects, 739.7 MB, first 2026-10-15-00-00-00-000010CD2FAE16F9, last 2026-10-15-00-00-00-08177574174167F1, hours [('2026-10-15-00', 50000)]
- `02:12:15` 2026/10/16: 50000 objects, 741.9 MB, first 2026-10-16-00-00-00-00001AF9BB517CE6, last 2026-10-16-00-00-00-0972F15A83FC1B8F, hours [('2026-10-16-00', 50000)]
- `02:12:16` 2026/10/17: 3511 objects, 852.1 MB, first 2026-10-17-00-00-00-000289FC19A8D144, last 2026-10-17-00-00-00-FFCC802A70717E53, hours [('2026-10-17-00', 3511)]
- `02:12:16` top of the log prefix: ['live/857687956942/'] | []
## 2026/10/10

- `02:12:59` 2026/10/10: 186698 log objects (read 4000, x46.7), GET bytes/day all 216.7 GB, not-from-Lambda 15.4 GB
- `02:12:59`    GB/day by requester class: [('lambda/role:lambda-execution-role', 200.95), ('anonymous-cloudflare', 10.92), ('anonymous-other', 3.36), ('iam-user:github-actions-justhodl', 1.07), ('lambda/role:lambda-role', 0.29), ('lambda/role:justhodl-student-rsi-role', 0.14)]
- `02:12:59`    non-Lambda GB/day by user agent: [('-', 10.93), ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/', 2.96), ('Boto3/1.43.111 md/Botocore#1.43.111 ua/2.1 os/linux#6.17.0-1', 1.03), ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, l', 0.36), ('curl/8.18.0', 0.05), ('Boto3/1.34.46 md/Botocore#1.34.46 ua/2.0 os/linux#6.17.0-102', 0.04)]
- `02:12:59`    non-Lambda GB/day by referer host: [('-', 12.04), ('justhodl.ai', 3.32)]
- `02:12:59`    non-Lambda GB/day by key prefix: [('data/backlog.json', 4.04), ('data/earnings-quality.json', 1.96), ('data/estimate-revisions.json', 1.52), ('data/share-flows.json', 1.42), ('data/best-setups.json', 1.31), ('data/buyback-engine.json', 0.85), ('data/statement-research', 0.76), ('data/inventory-drawdown.json', 0.62), ('audit-private/20260909-originals', 0.27), ('data/etf-desk-research.json', 0.24)]
- `02:12:59`    non-Lambda GB/day top keys: [('data/backlog.json', 4.04), ('data/earnings-quality.json', 1.96), ('data/estimate-revisions.json', 1.52), ('data/share-flows.json', 1.42), ('data/best-setups.json', 1.31), ('data/buyback-engine.json', 0.85), ('data/inventory-drawdown.json', 0.62), ('data/etf-desk-research.json', 0.24)]
- `02:12:59`    non-Lambda GB/day by network /16: [('104.22.*', 4.2), ('172.70.*', 3.1), ('17.166.*', 2.96), ('172.71.*', 2.41), ('57.154.*', 1.03), ('162.158.*', 0.46)]
## 2026/10/15

- `02:14:54` 2026/10/15: 600000 log objects (read 4000, x150.0), GET bytes/day all 29818.5 GB, not-from-Lambda 30.5 GB
- `02:14:54`    GB/day by requester class: [('lambda/role:lambda-execution-role', 29787.98), ('anonymous-other', 17.11), ('anonymous-cloudflare', 12.52), ('iam-user:github-actions-justhodl', 0.91), ('lambda/role:justhodl-student-rsi-role', 0.02)]
- `02:14:54`    non-Lambda GB/day by user agent: [('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/', 17.09), ('-', 12.53), ('Boto3/1.34.46 md/Botocore#1.34.46 ua/2.0 os/linux#6.17.0-102', 0.88), ('Boto3/1.43.114 md/Botocore#1.43.114 ua/2.1 os/linux#6.17.0-1', 0.03), ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, l', 0.01)]
- `02:14:54`    non-Lambda GB/day by referer host: [('justhodl.ai', 17.11), ('-', 13.44)]
- `02:14:54`    non-Lambda GB/day by key prefix: [('data/best-setups.json', 10.66), ('data/backlog.json', 4.4), ('data/share-flows.json', 3.42), ('data/etf-desk-research.json', 2.27), ('data/estimate-revisions.json', 2.1), ('data/buyback-engine.json', 1.88), ('data/earnings-quality.json', 1.49), ('data/forensic-screen.json', 1.44), ('data/series-cache', 0.76), ('data/risk-gate.json', 0.38)]
- `02:14:54`    non-Lambda GB/day top keys: [('data/best-setups.json', 10.66), ('data/backlog.json', 4.4), ('data/share-flows.json', 3.42), ('data/etf-desk-research.json', 2.27), ('data/estimate-revisions.json', 2.1), ('data/buyback-engine.json', 1.88), ('data/earnings-quality.json', 1.49), ('data/forensic-screen.json', 1.44)]
- `02:14:54`    non-Lambda GB/day by network /16: [('17.166.*', 17.09), ('172.71.*', 5.3), ('104.22.*', 4.57), ('172.68.*', 1.52), ('20.168.*', 0.88), ('104.23.*', 0.81)]
- `02:14:54` ✅ sampled 2 days
