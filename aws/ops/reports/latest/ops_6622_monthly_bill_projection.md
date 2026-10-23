# ops 6622 - the monthly bill at today's steady state (Cost Explorer, read-only)

**Status:** success  
**Duration:** 3.0s  
**Finished:** 2026-10-23T20:13:25+00:00  

## Data

| archive_month | october_mtd | projection_high | projection_low | projection_month | recurring_high | recurring_low | recurring_median_per_day | support_month |
|---|---|---|---|---|---|---|---|---|
| 12.9 | 1094.64 | 326 | 326 | 326 | 6.96 | 6.96 | 6.96 | 100.0 |

## Log
## 1. by service, per day

- `20:13:23` 2026-10-11  $  20.69  AmazonCloudWatch 9.26, Simple Storage Service 4.15, Lambda 3.68, Cost Explorer 1.73, SageMaker 1.35, DynamoDB 0.26
- `20:13:23` 2026-10-12  $ 263.88  Lambda 213.65, Simple Storage Service 35.17, AmazonCloudWatch 6.68, CloudWatch Events 5.34, SageMaker 1.33, Cost Explorer 1.26
- `20:13:23` 2026-10-13  $  74.84  Simple Storage Service 52.11, AmazonCloudWatch 8.16, CloudWatch Events 7.83, Lambda 2.81, Simple Queue Service 2.07, Cost Explorer 0.93
- `20:13:23` 2026-10-14  $ 107.27  Simple Storage Service 83.44, AmazonCloudWatch 8.48, CloudWatch Events 8.13, Lambda 2.91, Simple Queue Service 2.15, Cost Explorer 1.08
- `20:13:23` 2026-10-15  $ 140.93  Simple Storage Service 116.04, AmazonCloudWatch 8.96, CloudWatch Events 8.11, Lambda 2.92, Simple Queue Service 2.23, SageMaker 1.11
- `20:13:23` 2026-10-16  $ 162.59  Simple Storage Service 137.00, AmazonCloudWatch 10.66, CloudWatch Events 6.92, Simple Queue Service 2.84, Lambda 2.64, Cost Explorer 1.61
- `20:13:23` 2026-10-17  $   7.94  Simple Storage Service 7.31, AmazonCloudWatch 0.23, Secrets Manager 0.12, EC2 Container Registry (ECR) 0.09, Cost Explorer 0.08, EC2 - Other 0.04
- `20:13:23` 2026-10-18  $  13.27  Simple Storage Service 12.71, AmazonCloudWatch 0.19, Secrets Manager 0.12, EC2 Container Registry (ECR) 0.09, EC2 - Other 0.04, Cost Explorer 0.04
- `20:13:23` 2026-10-19  $  20.76  Business Support+ 11.58, Simple Storage Service 8.49, AmazonCloudWatch 0.26, Secrets Manager 0.12, Cost Explorer 0.09, EC2 Container Registry (ECR) 0.09
- `20:13:23` 2026-10-20  $  13.74  Simple Storage Service 12.83, AmazonCloudWatch 0.37, Cost Explorer 0.21, Secrets Manager 0.12, EC2 Container Registry (ECR) 0.09, EC2 - Other 0.04
- `20:13:23` 2026-10-21  $  22.06  Simple Storage Service 18.91, Lambda 1.58, AmazonCloudWatch 1.15, Secrets Manager 0.12, EC2 Container Registry (ECR) 0.09, DynamoDB 0.07
- `20:13:23` 2026-10-22  $  32.55  Simple Storage Service 29.47, Lambda 1.39, AmazonCloudWatch 1.26, Secrets Manager 0.12, Cost Explorer 0.09, EC2 Container Registry (ECR) 0.09
## 2. S3, CloudWatch and Lambda by usage type on the thawed days

- `20:13:25` S3 2026-10-21: $18.91 = Requests-SIA-Tier1 13.55, DataTransfer-Out-Bytes 2.10, BatchOperations-Objects 1.28, TimedStorage-ByteHrs 0.77, Requests-Tier1 0.51, BatchOperations-Jobs 0.25, Requests-Tier2 0.22
- `20:13:25` S3 2026-10-22: $29.47 = Requests-SIA-Tier1 21.83, BatchOperations-Objects 2.09, Retrieval-SIA 1.74, DataTransfer-Out-Bytes 1.63, TimedStorage-ByteHrs 0.78, Requests-Tier1 0.52, BatchOperations-Jobs 0.25
- `20:13:25` CloudWatch 2026-10-21: $1.15 = CW:MetricInsightAlarmUsage 0.60, CW:GMD-Metrics 0.32, CW:AlarmMonitorUsage 0.15, CW:Requests 0.04, CW:MetricMonitorUsage 0.04, VendedLog-Bytes 0.01
- `20:13:25` CloudWatch 2026-10-22: $1.26 = CW:MetricInsightAlarmUsage 0.64, CW:GMD-Metrics 0.39, CW:AlarmMonitorUsage 0.15, CW:Requests 0.04, CW:MetricMonitorUsage 0.04
- `20:13:25` Lambda 2026-10-21: $1.58 = Lambda-GB-Second 1.52, Lambda-GB-Second-ARM 0.03, DataTransfer-Out-Bytes 0.01, USW2-AWS-Out-Bytes 0.01
- `20:13:25` Lambda 2026-10-22: $1.39 = Lambda-GB-Second 1.33, Lambda-GB-Second-ARM 0.04, DataTransfer-Out-Bytes 0.01, USW2-AWS-Out-Bytes 0.01
## 3. the projection

- `20:13:25` {"day": "2026-10-21", "total": 22.06, "support": 0.0, "one_off_backfill": 15.1, "recurring": 6.96, "lambda": 1.58, "cloudwatch": 1.15, "s3_egress": 2.1, "s3_storage": 0.85, "s3_other": 0.86, "rest": 0.42}
- `20:13:25` {"day": "2026-10-22", "total": 32.55, "support": 0.0, "one_off_backfill": 24.18, "recurring": 8.37, "lambda": 1.39, "cloudwatch": 1.26, "s3_egress": 1.63, "s3_storage": 1.08, "s3_other": 2.57, "rest": 0.44}
- `20:13:25` recurring $/day on the complete thawed days ['2026-10-21']: median 6.96, low 6.96, high 6.96
- `20:13:25` fixed monthly: Business Support $100 (month-to-date charged $11.58; the plan bills the greater of $100 and 10% of usage), archive Standard-IA 1032 GB x $0.0125 = $12.90, replication PUTs ~$4.50
- `20:13:25` projection: ~$326/month (range $326 - $326); without Business Support ~$226/month
- `20:13:25` October month-to-date (Cost Explorer, through 2026-10-23): $1094.64 - carries the Oct 12-16 loop
- `20:13:25` ✅ read-only; four Cost Explorer calls
