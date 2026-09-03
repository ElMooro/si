# ops 5163 -- August-2026 invoice forensics ($484.26), read-only

**Status:** success  
**Duration:** 143.9s  
**Finished:** 2026-09-03T18:06:23+00:00  

## Data

| 2-Requests-SIA-Tier1 | Requests-Tier1 | Requests-Tier2 | TimedStorage-ByteHrs | USW2-AWS-Out-Bytes | avg_s | bucket | cw | dStorage-SIA-ByteHrs | day | errors | function | gb | gb_s | get_gb_per_day | invokes | lam | mem | objects | operation | other | qty | record_type | requester | s3 | section | service | standby | taTransfer-Out-Bytes | tier1_per_day | tier2_per_day | torage-SIA-SmObjects | total | usage_type | usd | usd_mtd | usd_per_month |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | Tax |  |  | A_record_type |  |  |  |  |  |  |  |  | 0.0 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | Usage |  |  | A_record_type |  |  |  |  |  |  |  |  | 484.27 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | Amazon Simple Storage Service |  |  |  |  |  |  |  | 289.23 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | AWS Lambda |  |  |  |  |  |  |  | 149.26 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | Amazon DynamoDB |  |  |  |  |  |  |  | 13.93 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | CloudWatch Events |  |  |  |  |  |  |  | 9.77 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | AmazonCloudWatch |  |  |  |  |  |  |  | 9.55 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | AWS Secrets Manager |  |  |  |  |  |  |  | 3.6 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | Amazon EC2 Container Registry (ECR) |  |  |  |  |  |  |  | 1.83 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | Amazon Route 53 |  |  |  |  |  |  |  | 1.5 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | EC2 - Other |  |  |  |  |  |  |  | 1.28 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | Amazon SageMaker |  |  |  |  |  |  |  | 1.27 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | Amazon OpenSearch Service |  |  |  |  |  |  |  | 1.13 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | Amazon Elastic Load Balancing |  |  |  |  |  |  |  | 0.65 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | Amazon Virtual Private Cloud |  |  |  |  |  |  |  | 0.35 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | Amazon Elastic Compute Cloud - Compute |  |  |  |  |  |  |  | 0.32 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | AWS Systems Manager |  |  |  |  |  |  |  | 0.32 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | AWS App Runner |  |  |  |  |  |  |  | 0.2 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | AWS Cost Explorer |  |  |  |  |  |  |  | 0.06 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | A_service | Amazon API Gateway |  |  |  |  |  |  |  | 0.01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 8019320.2 |  |  |  | B_usage_type |  |  |  |  |  |  |  | Lambda-GB-Second | 126.99 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 9488074.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USW2-Requests-SIA-Tier1 | 94.88 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 14191213.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | Requests-Tier1 | 70.95 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2301.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-USW2-AWS-Out-Bytes | 46.02 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 395.8 |  |  |  | B_usage_type |  |  |  |  |  |  |  | DataTransfer-Out-Bytes | 26.63 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1083.6 |  |  |  | B_usage_type |  |  |  |  |  |  |  | TimedStorage-ByteHrs | 26.24 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 11383200.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | Lambda-SnapStart-Cached-GB-S | 17.13 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1080.9 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USW2-TimedStorage-SIA-ByteHrs | 13.51 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 19836500.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | WriteRequestUnits | 12.4 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 28197402.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | Requests-Tier2 | 11.28 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 9772928.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-Event-64K-Chunks | 9.77 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1623366.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | CW:Requests | 6.23 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 9.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-AWSSecretsManager-Secrets | 3.6 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 30.9 |  |  |  | B_usage_type |  |  |  |  |  |  |  | CW:AlarmMonitorUsage | 2.09 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 151838.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | Lambda-GB-Second-ARM | 2.02 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 10079174.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | Request | 1.82 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 145.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USW2-TimedStorage-SIA-SmObjects | 1.81 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 3.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | HostedZone | 1.5 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 16.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | EBS:VolumeUsage.gp3 | 1.28 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 122364.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | CW:GMD-Metrics | 1.22 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 9149144.5 |  |  |  | B_usage_type |  |  |  |  |  |  |  | ReadRequestUnits | 1.14 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 30.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | ESInstance:t3.small | 1.08 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 14.2 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-Studio:JupyterLab-ml.t3.medium | 0.71 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 29.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | LoadBalancerUsage | 0.65 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 112047.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USW2-Requests-Tier1 | 0.56 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 5.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-Studio:VolumeUsage.gp3 | 0.56 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 70.5 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-PublicIPv4:InUseAddress | 0.35 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.8 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-TimedPITRStorage-ByteHrs | 0.35 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 28.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | BoxUsage:t2.micro | 0.32 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 4445.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-PS-Advanced-Param-Tier1 | 0.31 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 28.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-AppRunner-Provisioned-GB-hours | 0.2 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 776518.5 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-StorageLens-ObjCount | 0.16 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 5.4 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USW2-TimedStorage-ByteHrs | 0.12 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 3599953.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | Lambda-Storage-GB-Second | 0.11 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 6.6 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USW2-Retrieval-SIA | 0.07 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 6.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-APIRequest | 0.06 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.4 |  |  |  | B_usage_type |  |  |  |  |  |  |  | ES:GP3-Storage | 0.05 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.4 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USE1-TimedBackupStorage-ByteHrs | 0.04 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 5586965.0 |  |  |  | B_usage_type |  |  |  |  |  |  |  | Inventory-ObjectsListed | 0.01 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.3 |  |  |  | B_usage_type |  |  |  |  |  |  |  | USW2-DataTransfer-Out-Bytes | 0.01 |  |  |
|  |  |  |  |  |  |  | 0.18 |  | 2026-08-01 |  |  |  |  |  |  | 0.59 |  |  |  | 5.13 |  |  |  | 1.41 | C_daily |  |  |  |  |  |  | 7.44 |  |  |  |  |
|  |  |  |  |  |  |  | 0.01 |  | 2026-08-02 |  |  |  |  |  |  | 0.56 |  |  |  | 0.25 |  |  |  | 0.73 | C_daily |  |  |  |  |  |  | 1.6 |  |  |  |  |
|  |  |  |  |  |  |  | 0.05 |  | 2026-08-03 |  |  |  |  |  |  | 0.57 |  |  |  | 0.24 |  |  |  | 0.84 | C_daily |  |  |  |  |  |  | 1.76 |  |  |  |  |
|  |  |  |  |  |  |  | 0.01 |  | 2026-08-04 |  |  |  |  |  |  | 0.59 |  |  |  | 0.25 |  |  |  | 0.71 | C_daily |  |  |  |  |  |  | 1.62 |  |  |  |  |
|  |  |  |  |  |  |  | 0.02 |  | 2026-08-05 |  |  |  |  |  |  | 0.61 |  |  |  | 0.25 |  |  |  | 0.67 | C_daily |  |  |  |  |  |  | 1.6 |  |  |  |  |
|  |  |  |  |  |  |  | 0.07 |  | 2026-08-06 |  |  |  |  |  |  | 0.62 |  |  |  | 0.25 |  |  |  | 1.94 | C_daily |  |  |  |  |  |  | 2.96 |  |  |  |  |
|  |  |  |  |  |  |  | 0.06 |  | 2026-08-07 |  |  |  |  |  |  | 4.81 |  |  |  | 0.25 |  |  |  | 2.37 | C_daily |  |  |  |  |  |  | 7.56 |  |  |  |  |
|  |  |  |  |  |  |  | 0.05 |  | 2026-08-08 |  |  |  |  |  |  | 9.74 |  |  |  | 0.25 |  |  |  | 1.49 | C_daily |  |  |  |  |  |  | 11.6 |  |  |  |  |
|  |  |  |  |  |  |  | 0.45 |  | 2026-08-09 |  |  |  |  |  |  | 2.66 |  |  |  | 0.25 |  |  |  | 9.05 | C_daily |  |  |  |  |  |  | 12.98 |  |  |  |  |
|  |  |  |  |  |  |  | 0.57 |  | 2026-08-10 |  |  |  |  |  |  | 5.15 |  |  |  | 0.25 |  |  |  | 10.56 | C_daily |  |  |  |  |  |  | 17.2 |  |  |  |  |
|  |  |  |  |  |  |  | 0.65 |  | 2026-08-11 |  |  |  |  |  |  | 5.7 |  |  |  | 0.25 |  |  |  | 11.19 | C_daily |  |  |  |  |  |  | 18.48 |  |  |  |  |
|  |  |  |  |  |  |  | 0.66 |  | 2026-08-12 |  |  |  |  |  |  | 5.5 |  |  |  | 0.25 |  |  |  | 11.33 | C_daily |  |  |  |  |  |  | 18.44 |  |  |  |  |
|  |  |  |  |  |  |  | 0.66 |  | 2026-08-13 |  |  |  |  |  |  | 5.51 |  |  |  | 0.25 |  |  |  | 11.54 | C_daily |  |  |  |  |  |  | 18.67 |  |  |  |  |
|  |  |  |  |  |  |  | 0.63 |  | 2026-08-14 |  |  |  |  |  |  | 4.01 |  |  |  | 0.25 |  |  |  | 11.19 | C_daily |  |  |  |  |  |  | 16.75 |  |  |  |  |
|  |  |  |  |  |  |  | 0.6 |  | 2026-08-15 |  |  |  |  |  |  | 2.65 |  |  |  | 0.25 |  |  |  | 10.81 | C_daily |  |  |  |  |  |  | 14.94 |  |  |  |  |
|  |  |  |  |  |  |  | 0.8 |  | 2026-08-16 |  |  |  |  |  |  | 2.6 |  |  |  | 0.25 |  |  |  | 12.02 | C_daily |  |  |  |  |  |  | 16.32 |  |  |  |  |
|  |  |  |  |  |  |  | 1.28 |  | 2026-08-17 |  |  |  |  |  |  | 2.79 |  |  |  | 0.25 |  |  |  | 13.57 | C_daily |  |  |  |  |  |  | 18.56 |  |  |  |  |
|  |  |  |  |  |  |  | 1.25 |  | 2026-08-18 |  |  |  |  |  |  | 3.82 |  |  |  | 0.25 |  |  |  | 13.16 | C_daily |  |  |  |  |  |  | 19.12 |  |  |  |  |
|  |  |  |  |  |  |  | 1.24 |  | 2026-08-19 |  |  |  |  |  |  | 6.46 |  |  |  | 0.25 |  |  |  | 13.32 | C_daily |  |  |  |  |  |  | 21.92 |  |  |  |  |
|  |  |  |  |  |  |  | 1.24 |  | 2026-08-20 |  |  |  |  |  |  | 5.2 |  |  |  | 0.25 |  |  |  | 13.22 | C_daily |  |  |  |  |  |  | 20.56 |  |  |  |  |
|  |  |  |  |  |  |  | 1.24 |  | 2026-08-21 |  |  |  |  |  |  | 4.95 |  |  |  | 0.25 |  |  |  | 13.19 | C_daily |  |  |  |  |  |  | 20.28 |  |  |  |  |
|  |  |  |  |  |  |  | 1.24 |  | 2026-08-22 |  |  |  |  |  |  | 4.27 |  |  |  | 0.25 |  |  |  | 13.47 | C_daily |  |  |  |  |  |  | 19.87 |  |  |  |  |
|  |  |  |  |  |  |  | 1.25 |  | 2026-08-23 |  |  |  |  |  |  | 5.34 |  |  |  | 0.25 |  |  |  | 14.42 | C_daily |  |  |  |  |  |  | 21.92 |  |  |  |  |
|  |  |  |  |  |  |  | 1.33 |  | 2026-08-24 |  |  |  |  |  |  | 6.43 |  |  |  | 0.25 |  |  |  | 22.03 | C_daily |  |  |  |  |  |  | 30.75 |  |  |  |  |
|  |  |  |  |  |  |  | 1.32 |  | 2026-08-25 |  |  |  |  |  |  | 6.21 |  |  |  | 0.25 |  |  |  | 19.02 | C_daily |  |  |  |  |  |  | 27.55 |  |  |  |  |
|  |  |  |  |  |  |  | 0.75 |  | 2026-08-26 |  |  |  |  |  |  | 7.24 |  |  |  | 0.25 |  |  |  | 12.83 | C_daily |  |  |  |  |  |  | 21.78 |  |  |  |  |
|  |  |  |  |  |  |  | 0.67 |  | 2026-08-27 |  |  |  |  |  |  | 4.94 |  |  |  | 0.25 |  |  |  | 6.51 | C_daily |  |  |  |  |  |  | 13.05 |  |  |  |  |
|  |  |  |  |  |  |  | 0.44 |  | 2026-08-28 |  |  |  |  |  |  | 5.56 |  |  |  | 0.28 |  |  |  | 6.43 | C_daily |  |  |  |  |  |  | 13.08 |  |  |  |  |
|  |  |  |  |  |  |  | 0.18 |  | 2026-08-29 |  |  |  |  |  |  | 7.75 |  |  |  | 0.26 |  |  |  | 13.87 | C_daily |  |  |  |  |  |  | 22.13 |  |  |  |  |
|  |  |  |  |  |  |  | 0.18 |  | 2026-08-30 |  |  |  |  |  |  | 8.72 |  |  |  | 0.25 |  |  |  | 8.17 | C_daily |  |  |  |  |  |  | 17.39 |  |  |  |  |
|  |  |  |  |  |  |  | 0.22 |  | 2026-08-31 |  |  |  |  |  |  | 17.71 |  |  |  | 0.25 |  |  |  | 8.19 | C_daily |  |  |  |  |  |  | 26.44 |  |  |  |  |
|  |  |  |  |  |  |  | 0.04 |  | 2026-09-01 |  |  |  |  |  |  | 9.26 |  |  |  | 1.75 |  |  |  | 6.99 | C_daily |  |  |  |  |  |  | 18.16 |  |  |  |  |
|  |  |  |  |  |  |  | 0.08 |  | 2026-09-02 |  |  |  |  |  |  | 16.34 |  |  |  | 0.21 |  |  |  | 6.53 | C_daily |  |  |  |  |  |  | 23.28 |  |  |  |  |
| 8.85 | 4.78 | 0.4 | 1.42 | 4.33 |  |  |  | 0.76 | 2026-08-24 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | D_s3_daily |  |  | 1.4 |  |  | 0.07 |  |  |  |  |  |
| 6.46 | 3.78 | 0.41 | 1.55 | 4.43 |  |  |  | 0.83 | 2026-08-25 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | D_s3_daily |  |  | 1.47 |  |  | 0.08 |  |  |  |  |  |
| 3.36 | 3.22 | 0.37 | 1.7 | 2.01 |  |  |  | 0.93 | 2026-08-26 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | D_s3_daily |  |  | 1.16 |  |  | 0.09 |  |  |  |  |  |
| 0.02 | 2.93 | 0.33 | 1.8 | 0 |  |  |  | 0.96 | 2026-08-27 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | D_s3_daily |  |  | 0.38 |  |  | 0.09 |  |  |  |  |  |
| 0.02 | 2.93 | 0.23 | 1.88 | 0 |  |  |  | 0.96 | 2026-08-28 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | D_s3_daily |  |  | 0.32 |  |  | 0.09 |  |  |  |  |  |
| 0.02 | 7.83 | 0.6 | 1.98 | 0 |  |  |  | 0.96 | 2026-08-29 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | D_s3_daily |  |  | 2.33 |  |  | 0.09 |  |  |  |  |  |
| 0.02 | 0.89 | 1.1 | 1.15 | 0 |  |  |  | 0.96 | 2026-08-30 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | D_s3_daily |  |  | 3.9 |  |  | 0.09 |  |  |  |  |  |
| 0.02 | 3.84 | 0.42 | 1.19 | 0 |  |  |  | 0.96 | 2026-08-31 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | D_s3_daily |  |  | 1.65 |  |  | 0.09 |  |  |  |  |  |
| 0.02 | 4.04 | 0.45 | 1.38 | 0 |  |  |  | 0.99 | 2026-09-01 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | D_s3_daily |  |  | 0.0 |  |  | 0.09 |  |  |  |  |  |
| 0.02 | 3.46 | 0.54 | 1.41 | 0.0 |  |  |  | 0.99 | 2026-09-02 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | D_s3_daily |  |  | 0.0 |  |  | 0.09 |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PutObject |  | 6581108 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 32.99 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | PutObjectForRepl |  | 1840252 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 18.4 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | GetObject |  | 11848757 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 17.27 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | StandardStorage |  | 674 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 15.51 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | GetObjectForRepl |  | 538 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 10.77 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | StandardIAStorage |  | 742 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 9.28 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | ListBucket |  | 943257 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 4.81 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | StandardIASizeOverhead |  | 68 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 0.85 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | StorageLens |  | 1000639 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 0.2 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | UploadPartForRepl |  | 14836 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 0.15 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | UploadPart |  | 22500 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 0.11 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | HeadObject |  | 264876 |  |  |  | D_s3_ops10d |  |  |  |  |  |  |  |  | 0.11 |  |  |
|  |  |  |  |  | 39.1 |  |  |  |  | 17672 | justhodl-series-extractor |  | 8972895 |  | 22945 |  | 10240 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 149.55 |  |  |
|  |  |  |  |  | 178.8 |  |  |  |  | 67 | justhodl-ecb-deep |  | 1430755 |  | 2000 |  | 4096 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 23.85 |  |  |
|  |  |  |  |  | 2.6 |  |  |  |  | 88 | justhodl-sdmx-walker |  | 1136917 |  | 43182 |  | 10240 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 18.96 |  |  |
|  |  |  |  |  | 88.2 |  |  |  |  | 66 | justhodl-fred-catalog |  | 834651 |  | 4730 |  | 2048 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 13.91 |  |  |
|  |  |  |  |  | 211.4 |  |  |  |  | 1502 | justhodl-census-us |  | 629472 |  | 2978 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 10.49 |  |  |
|  |  |  |  |  | 0.1 |  |  |  |  | 0 | justhodl-signal-registry-ingest |  | 135131 |  | 9771372 |  | 256 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 4.21 |  |  |
|  |  |  |  |  | 639.6 |  |  |  |  | 0 | justhodl-news-velocity |  | 238568 |  | 746 |  | 512 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 3.98 |  |  |
|  |  |  |  |  | 336.5 |  |  |  |  | 0 | justhodl-repo |  | 152926 |  | 303 |  | 1536 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 2.55 |  |  |
|  |  |  |  |  | 4.8 |  |  |  |  | 18216 | justhodl-boj-full |  | 145173 |  | 19976 |  | 1536 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 2.42 |  |  |
|  |  |  |  |  | 178.0 |  |  |  |  | 5 | justhodl-provider-catalog |  | 121962 |  | 685 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 2.03 |  |  |
|  |  |  |  |  | 221.2 |  |  |  |  | 213 | justhodl-fleet-monitor |  | 105066 |  | 475 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 1.75 |  |  |
|  |  |  |  |  | 66.4 |  |  |  |  | 26 | justhodl-backend-agent |  | 103308 |  | 3110 |  | 512 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 1.72 |  |  |
|  |  |  |  |  | 3.1 |  |  |  |  | 11 | justhodl-a2a-bus |  | 93134 |  | 60189 |  | 512 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 1.56 |  |  |
|  |  |  |  |  | 418.3 |  |  |  |  | 0 | justhodl-worldbank-full |  | 85336 |  | 204 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 1.42 |  |  |
|  |  |  |  |  | 118.2 |  |  |  |  | 108 | justhodl-gdelt-full |  | 84074 |  | 711 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 1.4 |  |  |
|  |  |  |  |  | 262.2 |  |  |  |  | 0 | justhodl-13f-positions |  | 47977 |  | 183 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.8 |  |  |
|  |  |  |  |  | 69.6 |  |  |  |  | 3 | justhodl-risk-gate |  | 47628 |  | 684 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.79 |  |  |
|  |  |  |  |  | 347.5 |  |  |  |  | 0 | justhodl-data-census |  | 37769 |  | 37 |  | 3008 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.63 |  |  |
|  |  |  |  |  | 190.7 |  |  |  |  | 48 | justhodl-calibrator |  | 36238 |  | 95 |  | 2048 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.6 |  |  |
|  |  |  |  |  | 117.3 |  |  |  |  | 0 | justhodl-te-fred-mirror |  | 36234 |  | 412 |  | 768 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.6 |  |  |
|  |  |  |  |  | 51.9 |  |  |  |  | 33 | justhodl-repo-monitor |  | 35393 |  | 1364 |  | 512 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.59 |  |  |
|  |  |  |  |  | 295.0 |  |  |  |  | 0 | justhodl-fundamental-census |  | 35395 |  | 80 |  | 1536 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.59 |  |  |
|  |  |  |  |  | 606.9 |  |  |  |  | 36 | justhodl-imf-full |  | 34595 |  | 57 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.58 |  |  |
|  |  |  |  |  | 118.7 |  |  |  |  | 184 | justhodl-signal-scorecard |  | 33110 |  | 186 |  | 1536 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.55 |  |  |
|  |  |  |  |  | 85.3 |  |  |  |  | 12 | justhodl-real-economy-collector |  | 31126 |  | 365 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.52 |  |  |
|  |  |  |  |  | 26.2 |  |  |  |  | 13 | justhodl-nyfed-markets-full |  | 31053 |  | 1184 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.52 |  |  |
|  |  |  |  |  | 230.7 |  |  |  |  | 0 | justhodl-thesis-engine |  | 28459 |  | 42 |  | 3008 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.47 |  |  |
|  |  |  |  |  | 305.2 |  |  |  |  | 0 | justhodl-tradingview |  | 28074 |  | 46 |  | 2048 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.47 |  |  |
|  |  |  |  |  | 36.6 |  |  |  |  | 4 | justhodl-plumbing-aggregator |  | 23684 |  | 647 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.39 |  |  |
|  |  |  |  |  | 27.8 |  |  |  |  | 0 | justhodl-research-critique |  | 22473 |  | 1616 |  | 512 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.37 |  |  |
|  |  |  |  |  | 17.9 |  |  |  |  | 1297 | justhodl-insider-trades |  | 21618 |  | 2421 |  | 512 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.36 |  |  |
|  |  |  |  |  | 4.3 |  |  |  |  | 0 | justhodl-crypto-intel |  | 21210 |  | 4962 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.35 |  |  |
|  |  |  |  |  | 281.3 |  |  |  |  | 0 | justhodl-wl-engines |  | 17350 |  | 21 |  | 3008 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.29 |  |  |
|  |  |  |  |  | 88.2 |  |  |  |  | 4 | justhodl-canary-macro |  | 17331 |  | 131 |  | 1536 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.29 |  |  |
|  |  |  |  |  | 4.7 |  |  |  |  | 52 | justhodl-equity-research |  | 16039 |  | 3437 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.27 |  |  |
|  |  |  |  |  | 119.6 |  |  |  |  | 0 | justhodl-feed-catalog |  | 14590 |  | 61 |  | 2048 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.24 |  |  |
|  |  |  |  |  | 171.5 |  |  |  |  | 16 | justhodl-outcome-checker |  | 13546 |  | 79 |  | 1024 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.23 |  |  |
|  |  |  |  |  | 73.5 |  |  |  |  | 10 | justhodl-liquidity-reversal |  | 13015 |  | 354 |  | 512 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.22 |  |  |
|  |  |  |  |  | 36.8 |  |  |  |  | 0 | fmp-stock-picks-agent |  | 12607 |  | 685 |  | 512 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.21 |  |  |
|  |  |  |  |  | 39.3 |  |  |  |  | 0 | justhodl-fleet-error-monitor |  | 12450 |  | 634 |  | 512 |  |  |  |  |  |  |  | E_AUGUST |  |  |  |  |  |  |  |  | 0.21 |  |  |
|  |  |  |  |  | 99.6 |  |  |  |  | 390 | justhodl-census-us |  | 1133695 |  | 11378 |  | 1024 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 18.9 |  |  |
|  |  |  |  |  | 330.2 |  |  |  |  | 0 | justhodl-repo |  | 392278 |  | 792 |  | 1536 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 6.54 |  |  |
|  |  |  |  |  | 134.2 |  |  |  |  | 3 | justhodl-ecb-deep |  | 214251 |  | 399 |  | 4096 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 3.57 |  |  |
|  |  |  |  |  | 13.1 |  |  |  |  | 9 | justhodl-symdir |  | 189914 |  | 2413 |  | 6144 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 3.17 |  |  |
|  |  |  |  |  | 4.8 |  |  |  |  | 20790 | justhodl-boj-full |  | 164355 |  | 22926 |  | 1536 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 2.74 |  |  |
|  |  |  |  |  | 1.3 |  |  |  |  | 0 | justhodl-sdmx-walker |  | 70993 |  | 5413 |  | 10240 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 1.18 |  |  |
|  |  |  |  |  | 365.9 |  |  |  |  | 0 | justhodl-provider-catalog |  | 25613 |  | 70 |  | 1024 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.43 |  |  |
|  |  |  |  |  | 99.2 |  |  |  |  | 0 | justhodl-fundamental-census |  | 20377 |  | 137 |  | 1536 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.34 |  |  |
|  |  |  |  |  | 87.0 |  |  |  |  | 6 | justhodl-fortress |  | 19480 |  | 28 |  | 8192 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.32 |  |  |
|  |  |  |  |  | 475.8 |  |  |  |  | 0 | justhodl-news-velocity |  | 15703 |  | 66 |  | 512 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.26 |  |  |
|  |  |  |  |  | 308.2 |  |  |  |  | 0 | justhodl-13f-positions |  | 10480 |  | 34 |  | 1024 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.17 |  |  |
|  |  |  |  |  | 222.6 |  |  |  |  | 30 | justhodl-fleet-monitor |  | 10240 |  | 46 |  | 1024 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.17 |  |  |
|  |  |  |  |  | 123.1 |  |  |  |  | 0 | justhodl-repo-monitor |  | 8558 |  | 139 |  | 512 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.14 |  |  |
|  |  |  |  |  | 89.4 |  |  |  |  | 0 | justhodl-risk-gate |  | 6171 |  | 69 |  | 1024 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.1 |  |  |
|  |  |  |  |  | 680.0 |  |  |  |  | 9 | justhodl-research-backtest |  | 6120 |  | 9 |  | 1024 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.1 |  |  |
|  |  |  |  |  | 846.1 |  |  |  |  | 6 | justhodl-imf-full |  | 5923 |  | 7 |  | 1024 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.1 |  |  |
|  |  |  |  |  | 44.8 |  |  |  |  | 0 | justhodl-backend-agent |  | 5917 |  | 264 |  | 512 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.1 |  |  |
|  |  |  |  |  | 119.3 |  |  |  |  | 0 | justhodl-te-fred-mirror |  | 5906 |  | 66 |  | 768 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.1 |  |  |
|  |  |  |  |  | 658.5 |  |  |  |  | 0 | justhodl-data-census |  | 5803 |  | 3 |  | 3008 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.1 |  |  |
|  |  |  |  |  | 258.2 |  |  |  |  | 5 | justhodl-portwatch |  | 4907 |  | 19 |  | 1024 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.08 |  |  |
|  |  |  |  |  | 3.6 |  |  |  |  | 1 | justhodl-a2a-bus |  | 4685 |  | 2611 |  | 512 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.08 |  |  |
|  |  |  |  |  | 435.3 |  |  |  |  | 0 | justhodl-feed-catalog |  | 4353 |  | 5 |  | 2048 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.07 |  |  |
|  |  |  |  |  | 43.0 |  |  |  |  | 0 | justhodl-research-critique |  | 4060 |  | 189 |  | 512 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.07 |  |  |
|  |  |  |  |  | 235.1 |  |  |  |  | 6 | justhodl-calibrator |  | 3761 |  | 8 |  | 2048 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.06 |  |  |
|  |  |  |  |  | 29.3 |  |  |  |  | 101 | justhodl-insider-trades |  | 3101 |  | 212 |  | 512 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.05 |  |  |
|  |  |  |  |  | 39.3 |  |  |  |  | 0 | justhodl-plumbing-aggregator |  | 2636 |  | 67 |  | 1024 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.04 |  |  |
|  |  |  |  |  | 51.0 |  |  |  |  | 0 | justhodl-canary-macro |  | 2526 |  | 33 |  | 1536 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.04 |  |  |
|  |  |  |  |  | 412.4 |  |  |  |  | 0 | justhodl-causality-scanner |  | 2474 |  | 3 |  | 2048 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.04 |  |  |
|  |  |  |  |  | 206.9 |  |  |  |  | 0 | justhodl-thesis-engine |  | 2432 |  | 4 |  | 3008 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.04 |  |  |
|  |  |  |  |  | 2.1 |  |  |  |  | 0 | justhodl-fred-catalog |  | 2259 |  | 528 |  | 2048 |  |  |  |  |  |  |  | E_SEPTEMBER_MTD |  |  |  |  |  |  |  |  | 0.04 |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 1.55 |  |  |  |  |  |  |  |  | justhodl-repo |  | F_s3_requesters |  |  |  | 14544 | 20941 |  |  |  |  |  | 2.43 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.48 |  |  |  |  |  |  |  |  | anonymous |  | F_s3_requesters |  |  |  | 0 | 3823 |  |  |  |  |  | 1.33 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.01 |  |  |  |  |  |  |  |  | justhodl-census-us |  | F_s3_requesters |  |  |  | 3124 | 2062 |  |  |  |  |  | 0.49 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.35 |  |  |  |  |  |  |  |  | justhodl-boj-full |  | F_s3_requesters |  |  |  | 187 | 1874 |  |  |  |  |  | 0.05 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.01 |  |  |  |  |  |  |  |  | justhodl-streaming-fanout |  | F_s3_requesters |  |  |  | 225 | 650 |  |  |  |  |  | 0.04 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 |  |  |  |  |  |  |  |  | justhodl-risk-gate |  | F_s3_requesters |  |  |  | 0 | 2636 |  |  |  |  |  | 0.03 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.08 |  |  |  |  |  |  |  |  | justhodl-provider-catalog |  | F_s3_requesters |  |  |  | 175 | 75 |  |  |  |  |  | 0.03 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.03 |  |  |  |  |  |  |  |  | justhodl-sdmx-walker |  | F_s3_requesters |  |  |  | 150 | 262 |  |  |  |  |  | 0.03 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 |  |  |  |  |  |  |  |  | justhodl-fleet-freshness-monitor |  | F_s3_requesters |  |  |  | 162 | 25 |  |  |  |  |  | 0.02 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.04 |  |  |  |  |  |  |  |  | justhodl-market-tape |  | F_s3_requesters |  |  |  | 75 | 50 |  |  |  |  |  | 0.01 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.03 |  |  |  |  |  |  |  |  | justhodl-a2a-bus |  | F_s3_requesters |  |  |  | 12 | 812 |  |  |  |  |  | 0.01 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.03 |  |  |  |  |  |  |  |  | justhodl-stock-screener |  | F_s3_requesters |  |  |  | 62 | 62 |  |  |  |  |  | 0.01 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 |  |  |  |  |  |  |  |  | justhodl-trade-ticket-monitor |  | F_s3_requesters |  |  |  | 50 | 25 |  |  |  |  |  | 0.01 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 |  |  |  |  |  |  |  |  | justhodl-ecb-deep |  | F_s3_requesters |  |  |  | 50 | 25 |  |  |  |  |  | 0.01 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 |  |  |  |  |  |  |  |  | justhodl-asia-trade-full |  | F_s3_requesters |  |  |  | 50 | 12 |  |  |  |  |  | 0.01 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.01 |  |  |  |  |  |  |  |  | justhodl-convergence-radar |  | F_s3_requesters |  |  |  | 37 | 137 |  |  |  |  |  | 0.01 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.02 |  |  |  |  |  |  |  |  | justhodl-us10y-sentinel |  | F_s3_requesters |  |  |  | 25 | 37 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.08 |  |  |  |  |  |  |  |  | justhodl-stress-scenarios |  | F_s3_requesters |  |  |  | 12 | 112 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.01 |  |  |  |  |  |  |  |  | justhodl-ticker-deep-research |  | F_s3_requesters |  |  |  | 12 | 12 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 |  |  |  |  |  |  |  |  | justhodl-portfolio-risk |  | F_s3_requesters |  |  |  | 12 | 12 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 |  |  |  |  |  |  |  |  | justhodl-aaii-sentiment |  | F_s3_requesters |  |  |  | 12 | 12 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.15 |  |  |  |  |  |  |  |  | justhodl-lobbying-intel |  | F_s3_requesters |  |  |  | 12 | 12 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.03 |  |  |  |  |  |  |  |  | justhodl-symdir |  | F_s3_requesters |  |  |  | 0 | 162 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 |  |  |  |  |  |  |  |  | justhodl-daily-report-v3 |  | F_s3_requesters |  |  |  | 12 | 0 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.04 |  |  |  |  |  |  |  |  | justhodl-page-ai |  | F_s3_requesters |  |  |  | 0 | 125 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 |  |  |  |  |  |  |  |  | justhodl-backend-agent |  | F_s3_requesters |  |  |  | 0 | 62 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.06 |  |  |  |  |  |  |  |  | justhodl-fundamental-census |  | F_s3_requesters |  |  |  | 0 | 50 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 |  |  |  |  |  |  |  |  | justhodl-signal-backtest |  | F_s3_requesters |  |  |  | 0 | 37 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.0 |  |  |  |  |  |  |  |  | justhodl-forced-selling-bounce |  | F_s3_requesters |  |  |  | 0 | 25 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 0.02 |  |  |  |  |  |  |  |  | justhodl-crypto-intel |  | F_s3_requesters |  |  |  | 0 | 12 |  |  |  |  |  | 0.0 |
|  |  |  |  |  |  | justhodl-dashboard-live |  |  |  |  |  | 1954.4 |  |  |  |  |  | 4414210 |  |  |  |  |  |  | G_storage |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | justhodl-s3-access-logs-857687956942 |  |  |  |  |  | 9.0 |  |  |  |  |  | 727805 |  |  |  |  |  |  | G_storage |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | H_sep_services | AWS Lambda | False |  |  |  |  |  |  |  | 25.6 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | H_sep_services | Amazon Simple Storage Service | False |  |  |  |  |  |  |  | 13.53 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | H_sep_services | Amazon Route 53 | False |  |  |  |  |  |  |  | 1.5 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | H_sep_services | Amazon DynamoDB | False |  |  |  |  |  |  |  | 0.23 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | H_sep_services | AWS Secrets Manager | False |  |  |  |  |  |  |  | 0.23 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | H_sep_services | Amazon EC2 Container Registry (ECR) | True |  |  |  |  |  |  |  | 0.12 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | H_sep_services | AmazonCloudWatch | False |  |  |  |  |  |  |  | 0.11 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | H_sep_services | EC2 - Other | True |  |  |  |  |  |  |  | 0.07 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | H_sep_services | Amazon SageMaker | True |  |  |  |  |  |  |  | 0.04 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | H_sep_services | AWS Systems Manager | False |  |  |  |  |  |  |  | 0.02 |  |

## Log
- `18:03:59` account 857687956942  now=2026-09-03 18:03Z  invoice window 2026-08-01..2026-09-01 (End exclusive)
## A. Invoice reconciliation -- Cost Explorer, August by record type and service

- `18:03:59`   record_type Tax            $0.00
- `18:03:59`   record_type Usage          $484.27
- `18:03:59` 
- `18:03:59` AUGUST BY SERVICE (unblended, all record types):
- `18:03:59`    Amazon Simple Storage Service                       $289.23   59.7%
- `18:03:59`    AWS Lambda                                          $149.26   30.8%
- `18:03:59`    Amazon DynamoDB                                      $13.93    2.9%
- `18:03:59`    CloudWatch Events                                     $9.77    2.0%
- `18:03:59`    AmazonCloudWatch                                      $9.55    2.0%
- `18:03:59`    AWS Secrets Manager                                   $3.60    0.7%
- `18:03:59`    Amazon EC2 Container Registry (ECR)                   $1.83    0.4%
- `18:03:59`    Amazon Route 53                                       $1.50    0.3%
- `18:03:59`    EC2 - Other                                           $1.28    0.3%
- `18:03:59`    Amazon SageMaker                                      $1.27    0.3%
- `18:03:59`    Amazon OpenSearch Service                             $1.13    0.2%
- `18:03:59`    Amazon Elastic Load Balancing                         $0.65    0.1%
- `18:03:59`    Amazon Virtual Private Cloud                          $0.35    0.1%
- `18:03:59`    Amazon Elastic Compute Cloud - Compute                $0.32    0.1%
- `18:03:59`    AWS Systems Manager                                   $0.32    0.1%
- `18:03:59`    AWS App Runner                                        $0.20    0.0%
- `18:03:59`    AWS Cost Explorer                                     $0.06    0.0%
- `18:03:59`    Amazon API Gateway                                    $0.01    0.0%
- `18:03:59` 
- `18:03:59` ✅ CE August total $484.27 reconciles to the $484.26 invoice (delta +0.01)
## B. August top usage types (cost + quantity)

- `18:03:59` USAGE_TYPE                                                  USD           QUANTITY
- `18:03:59`    Lambda-GB-Second                                     $126.99          8,019,320
- `18:03:59`    USW2-Requests-SIA-Tier1                               $94.88          9,488,074
- `18:03:59`    Requests-Tier1                                        $70.95         14,191,213
- `18:03:59`    USE1-USW2-AWS-Out-Bytes                               $46.02              2,301
- `18:03:59`    DataTransfer-Out-Bytes                                $26.63                396
- `18:03:59`    TimedStorage-ByteHrs                                  $26.24              1,084
- `18:03:59`    Lambda-SnapStart-Cached-GB-S                          $17.13         11,383,200
- `18:03:59`    USW2-TimedStorage-SIA-ByteHrs                         $13.51              1,081
- `18:03:59`    WriteRequestUnits                                     $12.40         19,836,500
- `18:03:59`    Requests-Tier2                                        $11.28         28,197,402
- `18:03:59`    USE1-Event-64K-Chunks                                  $9.77          9,772,928
- `18:03:59`    CW:Requests                                            $6.23          1,623,366
- `18:03:59`    USE1-AWSSecretsManager-Secrets                         $3.60                  9
- `18:03:59`    CW:AlarmMonitorUsage                                   $2.09                 31
- `18:03:59`    Lambda-GB-Second-ARM                                   $2.02            151,838
- `18:03:59`    Request                                                $1.82         10,079,174
- `18:03:59`    USW2-TimedStorage-SIA-SmObjects                        $1.81                145
- `18:03:59`    HostedZone                                             $1.50                  3
- `18:03:59`    EBS:VolumeUsage.gp3                                    $1.28                 16
- `18:03:59`    CW:GMD-Metrics                                         $1.22            122,364
- `18:03:59`    ReadRequestUnits                                       $1.14          9,149,144
- `18:03:59`    ESInstance:t3.small                                    $1.08                 30
- `18:03:59`    USE1-Studio:JupyterLab-ml.t3.medium                    $0.71                 14
- `18:03:59`    LoadBalancerUsage                                      $0.65                 29
- `18:03:59`    USW2-Requests-Tier1                                    $0.56            112,047
- `18:03:59`    USE1-Studio:VolumeUsage.gp3                            $0.56                  5
- `18:03:59`    USE1-PublicIPv4:InUseAddress                           $0.35                 70
- `18:03:59`    USE1-TimedPITRStorage-ByteHrs                          $0.35                  2
- `18:03:59`    BoxUsage:t2.micro                                      $0.32                 28
- `18:03:59`    USE1-PS-Advanced-Param-Tier1                           $0.31              4,445
- `18:03:59`    USE1-AppRunner-Provisioned-GB-hours                    $0.20                 28
- `18:03:59`    USE1-StorageLens-ObjCount                              $0.16            776,518
- `18:03:59`    USW2-TimedStorage-ByteHrs                              $0.12                  5
- `18:03:59`    Lambda-Storage-GB-Second                               $0.11          3,599,953
- `18:03:59`    USW2-Retrieval-SIA                                     $0.07                  7
- `18:03:59`    USE1-APIRequest                                        $0.06                  6
- `18:03:59`    ES:GP3-Storage                                         $0.05                  0
- `18:03:59`    USE1-TimedBackupStorage-ByteHrs                        $0.04                  0
- `18:03:59`    Inventory-ObjectsListed                                $0.01          5,586,965
- `18:03:59`    USW2-DataTransfer-Out-Bytes                            $0.01                  0
## C. Daily curve Aug-01..yesterday by service -- anomaly window priced

- `18:04:00` baseline Aug-01..08: all-services $4.52/day, S3 $1.27/day
- `18:04:00` 
- `18:04:00` DAY           TOTAL       S3   LAMBDA       CW      DDB    OTHER  
- `18:04:00` 2026-08-01     7.44     1.41     0.59     0.18     0.12     5.13  ####### 
- `18:04:00` 2026-08-02     1.60     0.73     0.56     0.01     0.05     0.25  # 
- `18:04:00` 2026-08-03     1.76     0.84     0.57     0.05     0.06     0.24  # 
- `18:04:00` 2026-08-04     1.62     0.71     0.59     0.01     0.06     0.25  # 
- `18:04:00` 2026-08-05     1.60     0.67     0.61     0.02     0.06     0.25  # 
- `18:04:00` 2026-08-06     2.96     1.94     0.62     0.07     0.09     0.25  ## 
- `18:04:00` 2026-08-07     7.56     2.37     4.81     0.06     0.07     0.25  ####### 
- `18:04:00` 2026-08-08    11.60     1.49     9.74     0.05     0.07     0.25  ########### 
- `18:04:00` 2026-08-09    12.98     9.05     2.66     0.45     0.57     0.25  ############ << churn window
- `18:04:00` 2026-08-10    17.20    10.56     5.15     0.57     0.67     0.25  ################# << churn window
- `18:04:00` 2026-08-11    18.48    11.19     5.70     0.65     0.70     0.25  ################## << churn window
- `18:04:00` 2026-08-12    18.44    11.33     5.50     0.66     0.71     0.25  ################## << churn window
- `18:04:00` 2026-08-13    18.67    11.54     5.51     0.66     0.71     0.25  ################## << churn window
- `18:04:00` 2026-08-14    16.75    11.19     4.01     0.63     0.67     0.25  ################ << churn window
- `18:04:00` 2026-08-15    14.94    10.81     2.65     0.60     0.64     0.25  ############## << churn window
- `18:04:00` 2026-08-16    16.32    12.02     2.60     0.80     0.66     0.25  ################ << churn window
- `18:04:00` 2026-08-17    18.56    13.57     2.79     1.28     0.67     0.25  ################## << churn window
- `18:04:00` 2026-08-18    19.12    13.16     3.82     1.25     0.65     0.25  ################### << churn window
- `18:04:00` 2026-08-19    21.92    13.32     6.46     1.24     0.65     0.25  ##################### << churn window
- `18:04:00` 2026-08-20    20.56    13.22     5.20     1.24     0.65     0.25  #################### << churn window
- `18:04:00` 2026-08-21    20.28    13.19     4.95     1.24     0.65     0.25  #################### << churn window
- `18:04:00` 2026-08-22    19.87    13.47     4.27     1.24     0.64     0.25  ################### << churn window
- `18:04:00` 2026-08-23    21.92    14.42     5.34     1.25     0.66     0.25  ##################### << churn window
- `18:04:00` 2026-08-24    30.75    22.03     6.43     1.33     0.72     0.25  ############################## << churn window
- `18:04:00` 2026-08-25    27.55    19.02     6.21     1.32     0.75     0.25  ########################### << churn window
- `18:04:00` 2026-08-26    21.78    12.83     7.24     0.75     0.72     0.25  ##################### << churn window
- `18:04:00` 2026-08-27    13.05     6.51     4.94     0.67     0.69     0.25  ############# << churn window
- `18:04:00` 2026-08-28    13.08     6.43     5.56     0.44     0.38     0.28  ############# << churn window
- `18:04:00` 2026-08-29    22.13    13.87     7.75     0.18     0.07     0.26  ###################### << churn window
- `18:04:00` 2026-08-30    17.39     8.17     8.72     0.18     0.07     0.25  ################# 
- `18:04:00` 2026-08-31    26.44     8.19    17.71     0.22     0.07     0.25  ########################## 
- `18:04:00` 2026-09-01    18.16     6.99     9.26     0.04     0.11     1.75  ################## SEP
- `18:04:00` 2026-09-02    23.28     6.53    16.34     0.08     0.12     0.21  ####################### SEP
- `18:04:00` 
- `18:04:00` EXCESS above the Aug-01..08 baseline inside the churn window 2026-08-09..2026-08-29: S3 $236.05, all services $309.48
- `18:04:00` post-fix Aug-30/31 average $21.91/day; September MTD average $20.72/day over 2 complete day(s) -> $621.61/month projected
## D. S3 last 10 days -- usage types and operations back to baseline?

- `18:04:00` DAY        Requests-Tier1 ests-SIA-Tier1 torage-ByteHrs sfer-Out-Bytes -AWS-Out-Bytes ge-SIA-ByteHrs Requests-Tier2 -SIA-SmObjects
- `18:04:00` 2026-08-24           4.78           8.85           1.42           1.40           4.33           0.76           0.40           0.07
- `18:04:00` 2026-08-25           3.78           6.46           1.55           1.47           4.43           0.83           0.41           0.08
- `18:04:00` 2026-08-26           3.22           3.36           1.70           1.16           2.01           0.93           0.37           0.09
- `18:04:00` 2026-08-27           2.93           0.02           1.80           0.38           0.00           0.96           0.33           0.09
- `18:04:00` 2026-08-28           2.93           0.02           1.88           0.32           0.00           0.96           0.23           0.09
- `18:04:00` 2026-08-29           7.83           0.02           1.98           2.33           0.00           0.96           0.60           0.09
- `18:04:00` 2026-08-30           0.89           0.02           1.15           3.90           0.00           0.96           1.10           0.09
- `18:04:00` 2026-08-31           3.84           0.02           1.19           1.65           0.00           0.96           0.42           0.09
- `18:04:00` 2026-09-01           4.04           0.02           1.38           0.00           0.00           0.99           0.45           0.09
- `18:04:00` 2026-09-02           3.46           0.02           1.41           0.00           0.00           0.99           0.54           0.09
- `18:04:00` 
- `18:04:00` S3 by OPERATION, last 10 days (cost / requests):
- `18:04:00`    PutObject                       $32.99  qty        6,581,108
- `18:04:00`    PutObjectForRepl                $18.40  qty        1,840,252
- `18:04:00`    GetObject                       $17.27  qty       11,848,757
- `18:04:00`    StandardStorage                 $15.51  qty              674
- `18:04:00`    GetObjectForRepl                $10.77  qty              538
- `18:04:00`    StandardIAStorage                $9.28  qty              742
- `18:04:00`    ListBucket                       $4.81  qty          943,257
- `18:04:00`    StandardIASizeOverhead           $0.85  qty               68
- `18:04:00`    StorageLens                      $0.20  qty        1,000,639
- `18:04:00`    UploadPartForRepl                $0.15  qty           14,836
- `18:04:00`    UploadPart                       $0.11  qty           22,500
- `18:04:00`    HeadObject                       $0.11  qty          264,876
## E. Lambda burn per function -- August and September MTD

- `18:04:09` fleet: 874 functions
- `18:05:16` AUGUST: computed Lambda cost $260.77 across 10,112,243 invocations, 823 active functions
- `18:05:16` FUNCTION                                           INVOKES     GB-SEC      USD    MEM   AVG_S    ERR
- `18:05:16` justhodl-series-extractor                           22,945    8972895   149.55  10240    39.1  17672
- `18:05:16` justhodl-ecb-deep                                    2,000    1430755    23.85   4096   178.8     67
- `18:05:16` justhodl-sdmx-walker                                43,182    1136917    18.96  10240     2.6     88
- `18:05:16` justhodl-fred-catalog                                4,730     834651    13.91   2048    88.2     66
- `18:05:16` justhodl-census-us                                   2,978     629472    10.49   1024   211.4   1502
- `18:05:16` justhodl-signal-registry-ingest                  9,771,372     135131     4.21    256     0.1      0
- `18:05:16` justhodl-news-velocity                                 746     238568     3.98    512   639.6      0
- `18:05:16` justhodl-repo                                          303     152926     2.55   1536   336.5      0
- `18:05:16` justhodl-boj-full                                   19,976     145173     2.42   1536     4.8  18216
- `18:05:16` justhodl-provider-catalog                              685     121962     2.03   1024   178.0      5
- `18:05:16` justhodl-fleet-monitor                                 475     105066     1.75   1024   221.2    213
- `18:05:16` justhodl-backend-agent                               3,110     103308     1.72    512    66.4     26
- `18:05:16` justhodl-a2a-bus                                    60,189      93134     1.56    512     3.1     11
- `18:05:16` justhodl-worldbank-full                                204      85336     1.42   1024   418.3      0
- `18:05:16` justhodl-gdelt-full                                    711      84074     1.40   1024   118.2    108
- `18:05:16` justhodl-13f-positions                                 183      47977     0.80   1024   262.2      0
- `18:05:16` justhodl-risk-gate                                     684      47628     0.79   1024    69.6      3
- `18:05:16` justhodl-data-census                                    37      37769     0.63   3008   347.5      0
- `18:05:16` justhodl-calibrator                                     95      36238     0.60   2048   190.7     48
- `18:05:16` justhodl-te-fred-mirror                                412      36234     0.60    768   117.3      0
- `18:05:16` justhodl-repo-monitor                                1,364      35393     0.59    512    51.9     33
- `18:05:16` justhodl-fundamental-census                             80      35395     0.59   1536   295.0      0
- `18:05:16` justhodl-imf-full                                       57      34595     0.58   1024   606.9     36
- `18:05:16` justhodl-signal-scorecard                              186      33110     0.55   1536   118.7    184
- `18:05:16` justhodl-real-economy-collector                        365      31126     0.52   1024    85.3     12
- `18:05:16` justhodl-nyfed-markets-full                          1,184      31053     0.52   1024    26.2     13
- `18:05:16` justhodl-thesis-engine                                  42      28459     0.47   3008   230.7      0
- `18:05:16` justhodl-tradingview                                    46      28074     0.47   2048   305.2      0
- `18:05:16` justhodl-plumbing-aggregator                           647      23684     0.39   1024    36.6      4
- `18:05:16` justhodl-research-critique                           1,616      22473     0.37    512    27.8      0
- `18:05:16` justhodl-insider-trades                              2,421      21618     0.36    512    17.9   1297
- `18:05:16` justhodl-crypto-intel                                4,962      21210     0.35   1024     4.3      0
- `18:05:16` justhodl-wl-engines                                     21      17350     0.29   3008   281.3      0
- `18:05:16` justhodl-canary-macro                                  131      17331     0.29   1536    88.2      4
- `18:05:16` justhodl-equity-research                             3,437      16039     0.27   1024     4.7     52
- `18:05:16` justhodl-feed-catalog                                   61      14590     0.24   2048   119.6      0
- `18:05:16` justhodl-outcome-checker                                79      13546     0.23   1024   171.5     16
- `18:05:16` justhodl-liquidity-reversal                            354      13015     0.22    512    73.5     10
- `18:05:16` fmp-stock-picks-agent                                  685      12607     0.21    512    36.8      0
- `18:05:16` justhodl-fleet-error-monitor                           634      12450     0.21    512    39.3      0
- `18:05:16` CE says Lambda service August = $149.26; computed compute+requests = $260.77 (difference = tiered/other Lambda usage types, see B)
- `18:05:37` 
- `18:05:37` SEPTEMBER MTD: computed Lambda cost $40.45 across 65,172 invocations, 783 active functions
- `18:05:37` FUNCTION                                           INVOKES     GB-SEC      USD    MEM   AVG_S    ERR
- `18:05:37` justhodl-census-us                                  11,378    1133695    18.90   1024    99.6    390
- `18:05:37` justhodl-repo                                          792     392278     6.54   1536   330.2      0
- `18:05:37` justhodl-ecb-deep                                      399     214251     3.57   4096   134.2      3
- `18:05:37` justhodl-symdir                                      2,413     189914     3.17   6144    13.1      9
- `18:05:37` justhodl-boj-full                                   22,926     164355     2.74   1536     4.8  20790
- `18:05:37` justhodl-sdmx-walker                                 5,413      70993     1.18  10240     1.3      0
- `18:05:37` justhodl-provider-catalog                               70      25613     0.43   1024   365.9      0
- `18:05:37` justhodl-fundamental-census                            137      20377     0.34   1536    99.2      0
- `18:05:37` justhodl-fortress                                       28      19480     0.32   8192    87.0      6
- `18:05:37` justhodl-news-velocity                                  66      15703     0.26    512   475.8      0
- `18:05:37` justhodl-13f-positions                                  34      10480     0.17   1024   308.2      0
- `18:05:37` justhodl-fleet-monitor                                  46      10240     0.17   1024   222.6     30
- `18:05:37` justhodl-repo-monitor                                  139       8558     0.14    512   123.1      0
- `18:05:37` justhodl-risk-gate                                      69       6171     0.10   1024    89.4      0
- `18:05:37` justhodl-research-backtest                               9       6120     0.10   1024   680.0      9
- `18:05:37` justhodl-imf-full                                        7       5923     0.10   1024   846.1      6
- `18:05:37` justhodl-backend-agent                                 264       5917     0.10    512    44.8      0
- `18:05:37` justhodl-te-fred-mirror                                 66       5906     0.10    768   119.3      0
- `18:05:37` justhodl-data-census                                     3       5803     0.10   3008   658.5      0
- `18:05:37` justhodl-portwatch                                      19       4907     0.08   1024   258.2      5
- `18:05:37` justhodl-a2a-bus                                     2,611       4685     0.08    512     3.6      1
- `18:05:37` justhodl-feed-catalog                                    5       4353     0.07   2048   435.3      0
- `18:05:37` justhodl-research-critique                             189       4060     0.07    512    43.0      0
- `18:05:37` justhodl-calibrator                                      8       3761     0.06   2048   235.1      6
- `18:05:37` justhodl-insider-trades                                212       3101     0.05    512    29.3    101
- `18:05:37` justhodl-plumbing-aggregator                            67       2636     0.04   1024    39.3      0
- `18:05:37` justhodl-canary-macro                                   33       2526     0.04   1536    51.0      0
- `18:05:37` justhodl-causality-scanner                               3       2474     0.04   2048   412.4      0
- `18:05:37` justhodl-thesis-engine                                   4       2432     0.04   3008   206.9      0
- `18:05:37` justhodl-fred-catalog                                  528       2259     0.04   2048     2.1      0
- `18:05:37` September MTD Lambda run-rate: $14.70/day -> $440.86/month
- `18:05:37` 
- `18:05:37` invocation outliers in September MTD (>5,000/day):
- `18:05:37` ⚠    justhodl-boj-full                              8,328/day  (22,926 invocations)
## F. S3 request attribution by requester -- server access logs (ops 5024)

- `18:05:38` access-log latest partition: s3://justhodl-s3-access-logs-857687956942/live/857687956942/us-east-1/justhodl-dashboard-live/2026/09/03/
- `18:06:01` log objects under that partition: 126790 total, 26095 delivered in the last 3h
- `18:06:20` parsed 500 files, 3.3 MB, 4,278 request lines, time span 1.92h (2026-09-03 16:00:08..2026-09-03 17:55:23)
- `18:06:20` 
- `18:06:20` REQUESTER (session == Lambda name)           T1/day     T2/day  GET GB/d     $/day    $/month
- `18:06:20` justhodl-repo                                14,544     20,941      1.55     0.081       2.43
- `18:06:20`       ops: REST.GET.OBJECT=1526, REST.PUT.OBJECT=1164, REST.HEAD.OBJECT=150
- `18:06:20` anonymous                                         0      3,823      0.48     0.044       1.33
- `18:06:20`       ops: REST.GET.OBJECT=306
- `18:06:20` justhodl-census-us                            3,124      2,062      0.01     0.016       0.49
- `18:06:20`       ops: REST.PUT.OBJECT=250, REST.GET.OBJECT=165
- `18:06:20` justhodl-boj-full                               187      1,874      0.35     0.002       0.05
- `18:06:20`       ops: REST.GET.OBJECT=150, REST.PUT.OBJECT=15
- `18:06:20` justhodl-streaming-fanout                       225        650      0.01     0.001       0.04
- `18:06:20`       ops: REST.GET.OBJECT=52, REST.PUT.OBJECT=18
- `18:06:20` justhodl-risk-gate                                0      2,636      0.00     0.001       0.03
- `18:06:20`       ops: REST.GET.OBJECT=211
- `18:06:20` justhodl-provider-catalog                       175         75      0.08     0.001       0.03
- `18:06:20`       ops: REST.PUT.OBJECT=7, REST.GET.BUCKET=7, REST.GET.OBJECT=6
- `18:06:20` justhodl-sdmx-walker                            150        262      0.03     0.001       0.03
- `18:06:20`       ops: REST.GET.OBJECT=21, REST.PUT.OBJECT=12
- `18:06:20` justhodl-fleet-freshness-monitor                162         25      0.00     0.001       0.02
- `18:06:20`       ops: REST.GET.BUCKET=10, REST.PUT.OBJECT=3, REST.GET.OBJECT=2
- `18:06:20` justhodl-market-tape                             75         50      0.04     0.000       0.01
- `18:06:20`       ops: REST.PUT.OBJECT=6, REST.GET.OBJECT=4
- `18:06:20` justhodl-a2a-bus                                 12        812      0.03     0.000       0.01
- `18:06:20`       ops: REST.GET.OBJECT=65, REST.GET.BUCKET=1
- `18:06:20` justhodl-stock-screener                          62         62      0.03     0.000       0.01
- `18:06:20`       ops: REST.GET.OBJECT=5, REST.PUT.OBJECT=4, REST.GET.BUCKET=1
- `18:06:20` justhodl-trade-ticket-monitor                    50         25      0.00     0.000       0.01
- `18:06:20`       ops: REST.PUT.OBJECT=4, REST.GET.OBJECT=2
- `18:06:20` justhodl-ecb-deep                                50         25      0.00     0.000       0.01
- `18:06:20`       ops: REST.GET.OBJECT=2, REST.PUT.PART=2, REST.PUT.OBJECT=1
- `18:06:20` justhodl-asia-trade-full                         50         12      0.00     0.000       0.01
- `18:06:20`       ops: REST.PUT.OBJECT=4, REST.GET.OBJECT=1
- `18:06:20` justhodl-convergence-radar                       37        137      0.01     0.000       0.01
- `18:06:20`       ops: REST.GET.OBJECT=11, REST.PUT.OBJECT=3
- `18:06:20` justhodl-us10y-sentinel                          25         37      0.02     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=3, REST.PUT.OBJECT=2
- `18:06:20` justhodl-stress-scenarios                        12        112      0.08     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=9, REST.PUT.OBJECT=1
- `18:06:20` justhodl-ticker-deep-research                    12         12      0.01     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=1, REST.PUT.OBJECT=1
- `18:06:20` justhodl-portfolio-risk                          12         12      0.00     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=1, REST.PUT.OBJECT=1
- `18:06:20` justhodl-aaii-sentiment                          12         12      0.00     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=1, REST.PUT.OBJECT=1
- `18:06:20` justhodl-lobbying-intel                          12         12      0.15     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=1, REST.PUT.OBJECT=1
- `18:06:20` justhodl-symdir                                   0        162      0.03     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=13
- `18:06:20` justhodl-daily-report-v3                         12          0      0.00     0.000       0.00
- `18:06:20`       ops: REST.PUT.OBJECT=1
- `18:06:20` justhodl-page-ai                                  0        125      0.04     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=10
- `18:06:20` justhodl-backend-agent                            0         62      0.00     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=5
- `18:06:20` justhodl-fundamental-census                       0         50      0.06     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=4
- `18:06:20` justhodl-signal-backtest                          0         37      0.00     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=3
- `18:06:20` justhodl-forced-selling-bounce                    0         25      0.00     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=2
- `18:06:20` justhodl-crypto-intel                             0         12      0.02     0.000       0.00
- `18:06:20`       ops: REST.GET.OBJECT=1
- `18:06:20` 
- `18:06:20` all requesters: $0.15/day -> $4.55/month in S3 request + egress charges (storage byte-hours excluded)
- `18:06:20` top PUT prefixes in sample:
- `18:06:20`    data/warm/census-econ                                 2,974 puts/day
- `18:06:20`    data/warm/fred-scoped                                 2,861 puts/day
- `18:06:20`    data/warm/boj-full                                      187 puts/day
- `18:06:20`    data/market-tape.json                                    75 puts/day
- `18:06:20`    data/warm/sdmx-walker-summary.json                       62 puts/day
- `18:06:20`    data/search/providers                                    50 puts/day
- `18:06:20`    data/warm/asia-trade                                     50 puts/day
- `18:06:20`    data/repo-history/BAMLEMLLLCRPILAUSOAS.json              37 puts/day
- `18:06:20`    data/repo-history/BAMLEMIBHGCRPIEY.json                  37 puts/day
- `18:06:20`    data/repo-history/BAMLEMLLLCRPILAUSEY.json               37 puts/day
- `18:06:20`    data/repo-history/BAMLEMHYHYLCRPIUSTRIV.json             37 puts/day
- `18:06:20`    data/repo-history/BAMLEMIBHGCRPISYTW.json                37 puts/day
## G. Storage state -- live bucket now vs the Aug-26 peak, purge rule, versioning

- `18:06:21`   justhodl-dashboard-live                        1954.4 GB       4,414,210 objects  (as of 2026-09-01 18:03)
- `18:06:21`      Aug-26 peak was ~2,590 GB / 9.69M objects (ops 5024); storage at $0.023/GB-mo = $44.95/month now
- `18:06:22`   justhodl-s3-access-logs-857687956942              9.0 GB         727,805 objects  (as of 2026-09-01 18:03)
- `18:06:22`   versioning: Enabled
- `18:06:22`   lifecycle archive-to-glacier-deep-after-90d            Enabled prefix='archive/' noncurrent_exp=None exp=None
- `18:06:22`   lifecycle expire-old-versions-after-30d                Enabled prefix='' noncurrent_exp=None exp=None
- `18:06:22`   lifecycle expire-screener-snapshots-30d                Enabled prefix='screener/snapshots/' noncurrent_exp=None exp=30
- `18:06:22`   lifecycle jh-noncurrent-14d                            Enabled prefix='' noncurrent_exp=None exp=None
- `18:06:22`   lifecycle justhodl-e9-warm-ia                          Enabled prefix='data/warm/' noncurrent_exp=None exp=None
- `18:06:22`   lifecycle justhodl-e9-raw-glacier-ir                   Enabled prefix='data/raw/' noncurrent_exp=None exp=None
- `18:06:22`   lifecycle justhodl-e9-attic-ia                         Enabled prefix='data/attic/' noncurrent_exp=None exp=None
- `18:06:22`   lifecycle ops5027-purge-dead-versions-providers        Enabled prefix='data/providers/' noncurrent_exp=1 exp=None
## H. Standby / non-platform services still billing in September

- `18:06:23`    AWS Lambda                                     $25.60 MTD
- `18:06:23`    Amazon Simple Storage Service                  $13.53 MTD
- `18:06:23`    Amazon Route 53                                $1.50 MTD
- `18:06:23`    Amazon DynamoDB                                $0.23 MTD
- `18:06:23`    AWS Secrets Manager                            $0.23 MTD
- `18:06:23` ⚠    Amazon EC2 Container Registry (ECR)            $0.12 MTD  (standby-class)
- `18:06:23`    AmazonCloudWatch                               $0.11 MTD
- `18:06:23` ⚠    EC2 - Other                                    $0.07 MTD  (standby-class)
- `18:06:23` ⚠    Amazon SageMaker                               $0.04 MTD  (standby-class)
- `18:06:23`    AWS Systems Manager                            $0.02 MTD
## I. VERDICT

- `18:06:23` August CE total ............ $484.27 (invoice $484.26)
- `18:06:23`   S3 ....................... $289.23
- `18:06:23`   Lambda ................... $149.26
- `18:06:23`   CloudWatch ............... $19.32
- `18:06:23`   everything else .......... $26.46
- `18:06:23` Churn-window excess (Aug 09-29) above the Aug 01-08 baseline: S3 $236.05 / all $309.48
- `18:06:23` Baseline run-rate before the anomaly: $4.52/day = $135.48/month
- `18:06:23` September MTD run-rate: $20.72/day = $621.61/month projected
- `18:06:23` top Lambda by August cost: justhodl-series-extractor $149.55, justhodl-ecb-deep $23.85, justhodl-sdmx-walker $18.96, justhodl-fred-catalog $13.91, justhodl-census-us $10.49
- `18:06:23` ✅ wrote 5163_aug_bill_forensics.json
- `18:06:23` ✅ ops 5163 complete -- read-only, nothing mutated
