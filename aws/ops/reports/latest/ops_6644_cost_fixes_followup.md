# ops 6644 - did the Oct 23-24 cost fixes show up? (Cost Explorer + Oct 24 access logs, read-only)

**Status:** success  
**Duration:** 26.2s  
**Finished:** 2026-10-24T17:10:06+00:00  

## Data

| CW:MetricInsightAlarmUsage_2026-10-21 | CW:MetricInsightAlarmUsage_2026-10-22 | CW:MetricInsightAlarmUsage_2026-10-23 | CW:MetricInsightAlarmUsage_2026-10-24 | DataTransfer-Out-Bytes_2026-10-21 | DataTransfer-Out-Bytes_2026-10-22 | DataTransfer-Out-Bytes_2026-10-23 | DataTransfer-Out-Bytes_2026-10-24 | EarlyDelete-SIA_2026-10-24 | Requests-SIA-Tier1_2026-10-21 | Requests-SIA-Tier1_2026-10-22 | Requests-SIA-Tier1_2026-10-23 | Requests-SIA-Tier1_2026-10-24 | applebot_requests_day | logs_gb_day | total_2026-10-21 | total_2026-10-22 | total_2026-10-23 | total_2026-10-24 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| (0.6, 6.0) | (0.64, 6.4) | (0.51, 5.1) | (0.0, 0.0) | (2.1, 23.4) | (1.63, 18.1) | (3.61, 40.1) | (0.12, 1.3) | (0.0, 0.3) | (13.55, 1354833.0) | (21.83, 2183028.0) | (0.83, 83148.0) | (0.08, 8182.0) | {'403': 5801} | {'direct': 0.642, 'cloudflare': 2.238, 'runner': 0.031} | 22.06 | 32.55 | 9.79 | 0.87 |

## Log
## A. Cost Explorer per day

- `17:09:40` 2026-10-21 total $22.06 | Amazon Simple Storage Servic $18.91, AWS Lambda $1.58, AmazonCloudWatch $1.15, AWS Secrets Manager $0.12, Amazon EC2 Container Registr $0.09, Amazon DynamoDB $0.07, AWS Cost Explorer $0.07
- `17:09:40` 2026-10-22 total $32.55 | Amazon Simple Storage Servic $29.47, AWS Lambda $1.39, AmazonCloudWatch $1.26, AWS Secrets Manager $0.12, AWS Cost Explorer $0.09, Amazon EC2 Container Registr $0.09, Amazon DynamoDB $0.07
- `17:09:40` 2026-10-23 total $9.79 | Amazon Simple Storage Servic $6.75, AWS Lambda $1.45, AmazonCloudWatch $1.05, AWS Cost Explorer $0.19, AWS Secrets Manager $0.12, Amazon EC2 Container Registr $0.09, Amazon DynamoDB $0.07
- `17:09:40` 2026-10-24 (partial) total $0.87 | Amazon Simple Storage Servic $0.32, AWS Lambda $0.26, AmazonCloudWatch $0.18, AWS Cost Explorer $0.03, AWS Secrets Manager $0.03, Amazon EC2 Container Registr $0.03, Amazon DynamoDB $0.02
- `17:09:40` 2026-10-21 S3 $18.91 | DataTransfer-Out-Bytes $2.10 (23.4), Requests-SIA-Tier1 $13.55 (1354833.0), Retrieval-SIA $0.02 (1.7), TimedStorage-ByteHrs $0.77 (33.5), TimedStorage-SIA-ByteHrs $0.07 (5.6), Requests-Tier1 $0.51 (102856.0), Requests-Tier2 $0.22 (552854.0), BatchOperations-Objects $1.28 (1282030.0) | other: BatchOperations-Jobs $0.25, Inventory-ObjectsListed $0.07
- `17:09:40` 2026-10-22 S3 $29.47 | DataTransfer-Out-Bytes $1.63 (18.1), Requests-SIA-Tier1 $21.83 (2183028.0), Retrieval-SIA $1.74 (173.9), TimedStorage-ByteHrs $0.78 (34.0), TimedStorage-SIA-ByteHrs $0.25 (20.0), Requests-Tier1 $0.52 (103348.0), Requests-Tier2 $0.21 (515906.0), BatchOperations-Objects $2.09 (2086159.0) | other: BatchOperations-Jobs $0.25
- `17:09:40` 2026-10-23 S3 $6.75 | DataTransfer-Out-Bytes $3.61 (40.1), Requests-SIA-Tier1 $0.83 (83148.0), Retrieval-SIA $0.02 (1.6), TimedStorage-ByteHrs $0.79 (34.3), TimedStorage-SIA-ByteHrs $0.49 (39.2), Requests-Tier1 $0.61 (122484.0), Requests-Tier2 $0.23 (578527.0) | other: TimedStorage-SIA-SmObjects $0.07
- `17:09:40` 2026-10-24 S3 $0.32 | DataTransfer-Out-Bytes $0.12 (1.3), Requests-SIA-Tier1 $0.08 (8182.0), Retrieval-SIA $0.02 (1.5), Requests-Tier1 $0.06 (11748.0), Requests-Tier2 $0.02 (47306.0)
- `17:09:40` 2026-10-21 AmazonCloudWatch $1.15 | CW:MetricInsightAlarmUsage $0.60 (6.0), CW:AlarmMonitorUsage $0.15 (1.5), CW:GMD-Metrics $0.32 (32341.0), CW:Requests $0.04 (3768.0)
- `17:09:40` 2026-10-22 AmazonCloudWatch $1.26 | CW:MetricInsightAlarmUsage $0.64 (6.4), CW:AlarmMonitorUsage $0.15 (1.5), CW:GMD-Metrics $0.39 (38857.0), CW:Requests $0.04 (3804.0)
- `17:09:40` 2026-10-23 AmazonCloudWatch $1.05 | CW:MetricInsightAlarmUsage $0.51 (5.1), CW:AlarmMonitorUsage $0.14 (1.4), CW:GMD-Metrics $0.32 (32142.0), CW:Requests $0.04 (3800.0)
- `17:09:40` 2026-10-24 AmazonCloudWatch $0.18 | CW:AlarmMonitorUsage $0.03 (0.3), CW:GMD-Metrics $0.13 (13448.0), CW:Requests $0.02 (1820.0)
## B. access logs of Oct 24 so far: billed GB by class, Applebot

- `17:10:06` log objects 103577 over 16.0 h (read 4000, x38.9 to a day)
- `17:10:06` billed GB/day by class: {'direct': 0.642, 'cloudflare': 2.238, 'runner': 0.031}
- `17:10:06` requests/day by class+status: {'direct 403': 88969, 'runner 200': 8293, 'cloudflare 200': 2336, 'cloudflare 304': 506, 'direct 200': 234, 'cloudflare 403': 195, 'runner 404': 39}
- `17:10:06` Applebot requests/day by status: {'403': 5801}
- `17:10:06` direct GB/day by user agent: [('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessC', 0.62), ('Lightpanda/1.0', 0.021), ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, lik', 0.001), ('Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)', 0.0)]
- `17:10:06` ✅ read-only; nothing changed
