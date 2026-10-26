# ops 6677 - the first clean day (Oct 25) against the ~$200/month target (read-only)

**Status:** success  
**Duration:** 1.5s  
**Finished:** 2026-10-26T16:16:43+00:00  

## Data

| early_delete_sia_usd | egress_gb | int_puts | metric_insights_usd | november_support | november_total | november_usage | oct25_estimated | oct25_total | projected_day | sia_puts |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.01 | 0.1 | 129199 | 0.0 | 29.0 | 208.99 | 179.99 | True | 6.27 | 6.34 | 0 |

## Log
## A. account and services per day

- `16:16:42` 2026-10-23 total $9.79 | Amazon Simple Storage Service $6.75, AWS Lambda $1.45, AmazonCloudWatch $1.05, AWS Cost Explorer $0.19, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.07, EC2 - Other $0.04, Amazon SageMaker $0.02, AWS Systems Manager $0.01
- `16:16:42` 2026-10-24 total $5.01 | Amazon Simple Storage Service $2.78, AWS Lambda $1.26, AmazonCloudWatch $0.49, AWS Cost Explorer $0.13, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, Amazon DynamoDB $0.07, EC2 - Other $0.04, Amazon SageMaker $0.02, AWS Systems Manager $0.01
- `16:16:42` 2026-10-25 total $6.27 | Amazon Simple Storage Service $3.45, AWS Lambda $1.91, AmazonCloudWatch $0.48, AWS Secrets Manager $0.12, Amazon EC2 Container Registry  $0.09, AWS Cost Explorer $0.08, Amazon DynamoDB $0.07, EC2 - Other $0.04, Amazon SageMaker $0.02, AWS Systems Manager $0.01
- `16:16:42` 2026-10-26 (today, partial) total $3.08 | Amazon Simple Storage Service $1.90, AWS Lambda $0.93, AmazonCloudWatch $0.17, AWS Secrets Manager $0.02, AWS Cost Explorer $0.02, Amazon DynamoDB $0.02, Amazon EC2 Container Registry  $0.02
## B. S3, CloudWatch and Lambda by usage type

- `16:16:42` 2026-10-23 S3             $6.75 | DataTransfer-Out-Bytes $3.605 (40.1), Requests-SIA-Tier1 $0.831 (83148.0), TimedStorage-SIA-ByteHrs $0.490 (39.2), TimedStorage-SIA-SmObjects $0.070 (5.6), TimedStorage-ByteHrs $0.790 (34.3), Requests-Tier1 $0.612 (122484.0), Requests-Tier2 $0.231 (578527.0), Retrieval-SIA $0.016 (1.6) | other: Inventory-ObjectsListed $0.04, USE1-StorageLens-ObjCount $0.03
- `16:16:42` 2026-10-24 S3             $2.78 | DataTransfer-Out-Bytes $0.282 (3.1), Requests-INT-Tier1 $0.228 (45605.0), Requests-SIA-Tier1 $0.174 (17431.0), EarlyDelete-SIA $0.003 (0.3), TimedStorage-SIA-ByteHrs $0.493 (39.5), TimedStorage-SIA-SmObjects $0.072 (5.8), TimedStorage-ByteHrs $0.799 (34.7), Requests-Tier1 $0.434 (86798.0), Requests-Tier2 $0.181 (451934.0), Retrieval-SIA $0.015 (1.5) | other: Inventory-ObjectsListed $0.03, USE1-StorageLens-ObjCount $0.03
- `16:16:42` 2026-10-25 S3             $3.45 | DataTransfer-Out-Bytes $0.299 (3.3), Requests-INT-Tier1 $0.646 (129199.0), EarlyDelete-SIA $0.007 (0.5), Monitoring-Automation-INT $0.001 (369.9), TimedStorage-INT-FA-ByteHrs $0.004 (0.2), TimedStorage-SIA-ByteHrs $0.495 (39.6), TimedStorage-SIA-SmObjects $0.072 (5.8), TimedStorage-ByteHrs $0.807 (35.1), Requests-Tier1 $0.796 (159178.0), Requests-Tier2 $0.225 (563086.0), Retrieval-SIA $0.014 (1.4) | other: USE1-StorageLens-ObjCount $0.03
- `16:16:42` 2026-10-26 S3             $1.90 | DataTransfer-Out-Bytes $0.026 (0.3), Requests-INT-Tier1 $0.887 (177337.0), Requests-Tier1 $0.901 (180139.0), Requests-Tier2 $0.070 (175360.0), Retrieval-SIA $0.009 (0.9)
- `16:16:42` 2026-10-23 AmazonCloudWat $1.05 | CW:MetricInsightAlarmUsage $0.509 (5.1), CW:AlarmMonitorUsage $0.135 (1.4), CW:GMD-Metrics $0.321 (32142.0), CW:Requests $0.038 (3800.0), CW:MetricMonitorUsage $0.035 (0.1)
- `16:16:42` 2026-10-24 AmazonCloudWat $0.49 | CW:MetricInsightAlarmUsage $0.003 (0.0), CW:AlarmMonitorUsage $0.087 (0.9), CW:GMD-Metrics $0.304 (30376.0), CW:Requests $0.037 (3747.0), CW:MetricMonitorUsage $0.035 (0.1)
- `16:16:42` 2026-10-25 AmazonCloudWat $0.48 | CW:MetricInsightAlarmUsage $0.003 (0.0), CW:AlarmMonitorUsage $0.087 (0.9), CW:GMD-Metrics $0.301 (30097.0), CW:Requests $0.037 (3746.0), CW:MetricMonitorUsage $0.035 (0.1)
- `16:16:42` 2026-10-26 AmazonCloudWat $0.17 | CW:MetricInsightAlarmUsage $0.001 (0.0), CW:AlarmMonitorUsage $0.022 (0.2), CW:GMD-Metrics $0.123 (12276.0), CW:Requests $0.018 (1830.0), CW:MetricMonitorUsage $0.004 (0.0)
- `16:16:43` 2026-10-23 AWS Lambda     $1.45 | Lambda-GB-Second $1.393 (83579.0), Lambda-GB-Second-ARM $0.034 (2538.0), Request $0.002 (12434.0), Request-ARM $0.000 (290.0)
- `16:16:43` 2026-10-24 AWS Lambda     $1.26 | Lambda-GB-Second $1.214 (72836.4), Lambda-GB-Second-ARM $0.032 (2370.5), Request $0.002 (9727.0), Request-ARM $0.000 (192.0)
- `16:16:43` 2026-10-25 AWS Lambda     $1.91 | Lambda-GB-Second $1.855 (111319.3), Lambda-GB-Second-ARM $0.034 (2534.2), Request $0.001 (3444.0), Request-ARM $0.000 (325.0)
- `16:16:43` 2026-10-26 AWS Lambda     $0.93 | Lambda-GB-Second $0.919 (55154.0), Lambda-GB-Second-ARM $0.003 (205.4), Request $0.000 (1811.0), Request-ARM $0.000 (130.0)
## C. Oct 25 against the projection

- `16:16:43` Oct 25 = $6.27 (projected steady day $6.34; estimated)
- `16:16:43` direct S3 egress 0.1 GB ($0.01); Metrics Insights alarm $0.00; archive copies: Intelligent-Tiering 129199 ($0.65), Standard-IA 0 ($0.00); IT monitoring $0.001; EarlyDelete-SIA $0.01
- `16:16:43` November from Oct 25: $6.27 x 30 = $188.17, minus the free allowances of a fresh month ~$8.19 = $179.99 of usage, plus Business Support+ $29.00 = ~$208.99
- `16:16:43` ✅ read-only; nothing changed
