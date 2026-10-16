# ops 6527 - attribute the pre-loop baseline (read-only)

**Status:** success  
**Duration:** 33.1s  
**Finished:** 2026-10-16T23:07:28+00:00  

## Log
## Cost Explorer

- `23:06:56` normal-window total $23.65/day over 22 days
- `23:06:56`   AmazonCloudWatch                 CW:Requests                              $  7.396/day  qty 785100.36
- `23:06:56`   Amazon Simple Storage Service    DataTransfer-Out-Bytes                   $  4.700/day  qty 56.73
- `23:06:56`   AWS Lambda                       Lambda-GB-Second                         $  3.928/day  qty 253888.28
- `23:06:56`   AWS Cost Explorer                USE1-APIRequest                          $  1.714/day  qty 171.45
- `23:06:56`   Amazon Simple Storage Service    Requests-Tier1                           $  1.505/day  qty 300957.95
- `23:06:56`   AmazonCloudWatch                 CW:MetricMonitorUsage                    $  0.942/day  qty 3.59
- `23:06:56`   Amazon SageMaker                 USE1-AsyncInf:ml.g5.xlarge               $  0.618/day  qty 0.44
- `23:06:56`   Amazon Simple Storage Service    TimedStorage-ByteHrs                     $  0.588/day  qty 25.55
- `23:06:56`   Amazon Simple Storage Service    Requests-Tier2                           $  0.464/day  qty 1159770.36
- `23:06:56`   Amazon SageMaker                 USE1-Train:ml.g5.2xlarge                 $  0.405/day  qty 0.27
- `23:06:56`   AWS Lambda                       Lambda-GB-Second-ARM                     $  0.229/day  qty 17187.96
- `23:06:56`   Amazon DynamoDB                  WriteRequestUnits                        $  0.190/day  qty 303867.91
- `23:06:56`   Amazon SageMaker                 USE1-TrSpt:ml.g5.2xlarge                 $  0.165/day  qty 0.11
- `23:06:56`   AWS Secrets Manager              USE1-AWSSecretsManager-Secrets           $  0.118/day  qty 0.3
- `23:06:56`   Amazon EC2 Container Registry (E TimedStorage-ByteHrs                     $  0.089/day  qty 0.89
- `23:06:56`   AmazonCloudWatch                 CW:AlarmMonitorUsage                     $  0.086/day  qty 1.31
- `23:06:56`   Amazon Route 53                  HostedZone                               $  0.068/day  qty 0.14
- `23:06:56`   Amazon Simple Storage Service    TimedStorage-SIA-ByteHrs                 $  0.059/day  qty 4.76
- `23:06:56`   Amazon Simple Storage Service    Requests-Tier4                           $  0.050/day  qty 4981.64
- `23:06:56`   Amazon DynamoDB                  ReadRequestUnits                         $  0.046/day  qty 371128.2
- `23:06:56`   AmazonCloudWatch                 CW:GMD-Metrics                           $  0.044/day  qty 4388.14
- `23:06:56`   EC2 - Other                      EBS:VolumeUsage.gp3                      $  0.042/day  qty 0.52
- `23:06:56`   Amazon Simple Storage Service    USW2-Requests-SIA-Tier1                  $  0.035/day  qty 3511.18
- `23:06:56`   AWS Lambda                       DataTransfer-Out-Bytes                   $  0.024/day  qty 0.31
- `23:06:56`   Amazon DynamoDB                  USE1-TimedPITRStorage-ByteHrs            $  0.023/day  qty 0.11
- `23:06:56` AmazonCloudWatch by operation:
- `23:06:56`     GetMetricStatistics                CW:Requests                        $  7.395/day  qty/day 784901.1
- `23:06:56`     MetricStorage                      CW:MetricMonitorUsage              $  0.927/day  qty/day 3.5
- `23:06:56`     Unknown                            CW:AlarmMonitorUsage               $  0.086/day  qty/day 1.3
- `23:06:56`     GetMetricData                      CW:GMD-Metrics                     $  0.044/day  qty/day 4388.1
- `23:06:56`     MetricStorage:AWS/Logs-EMF         CW:MetricMonitorUsage              $  0.008/day  qty/day 0.0
- `23:06:56`     MetricStorage:AWS/SES              CW:MetricMonitorUsage              $  0.006/day  qty/day 0.0
- `23:06:56`     GetMetricData                      CW:MetricInsightAlarmUsage         $  0.003/day  qty/day 0.0
- `23:06:56`     PutMetricData                      CW:Requests                        $  0.002/day  qty/day 199.2
- `23:06:56`     trib/receiver/prometheusreceiver   CW:OTEL:Bytes                      $  0.001/day  qty/day 0.0
- `23:06:56` Amazon Simple Storage Service by operation:
- `23:06:56`     GetObject                          DataTransfer-Out-Bytes             $  4.697/day  qty/day 56.7
- `23:06:56`     PutObject                          Requests-Tier1                     $  1.127/day  qty/day 225345.6
- `23:06:56`     StandardStorage                    TimedStorage-ByteHrs               $  0.588/day  qty/day 25.5
- `23:06:56`     GetObject                          Requests-Tier2                     $  0.448/day  qty/day 1120747.4
- `23:06:56`     ListBucket                         Requests-Tier1                     $  0.372/day  qty/day 74398.2
- `23:06:56`     StandardIAStorage                  TimedStorage-SIA-ByteHrs           $  0.059/day  qty/day 4.8
- `23:06:56`     S3-SIATransition                   Requests-Tier4                     $  0.050/day  qty/day 4962.5
- `23:06:56`     PutObject                          USW2-Requests-SIA-Tier1            $  0.035/day  qty/day 3511.2
- `23:06:56`     StorageLens                        USE1-StorageLens-ObjCount          $  0.019/day  qty/day 94359.6
- `23:06:56`     GetObject                          Retrieval-SIA                      $  0.018/day  qty/day 1.7
- `23:06:57` AWS Lambda by operation:
- `23:06:57`     Invoke                             Lambda-GB-Second                   $  3.928/day  qty/day 253888.3
- `23:06:57`     Invoke                             Lambda-GB-Second-ARM               $  0.229/day  qty/day 17188.0
- `23:06:57`     Invoke                             DataTransfer-Out-Bytes             $  0.024/day  qty/day 0.3
- `23:06:57`     Invoke                             USE1-USW2-AWS-Out-Bytes            $  0.012/day  qty/day 0.6
- `23:06:57`     Invoke                             Lambda-Storage-GB-Second           $  0.002/day  qty/day 53167.9
- `23:06:57`     Invoke                             Lambda-Storage-GB-Second-ARM       $  0.000/day  qty/day 29.6
- `23:06:57`     Invoke                             USE1-EUC1-AWS-Out-Bytes            $  0.000/day  qty/day 0.0
- `23:06:57`     Invoke                             USE1-UGW1-AWS-Out-Bytes            $  0.000/day  qty/day 0.0
- `23:06:57` Amazon SageMaker by operation:
- `23:06:57`     RunInstance                        USE1-AsyncInf:ml.g5.xlarge         $  0.618/day  qty/day 0.4
- `23:06:57`     RunInstance                        USE1-Train:ml.g5.2xlarge           $  0.405/day  qty/day 0.3
- `23:06:57`     RunInstance                        USE1-TrSpt:ml.g5.2xlarge           $  0.165/day  qty/day 0.1
- `23:06:57`     CreateVolume-Gp3                   USE1-Studio:VolumeUsage.gp3        $  0.018/day  qty/day 0.2
- `23:06:57`     Serverless                         USE1-ServerlessInf:Mem-4GB         $  0.004/day  qty/day 47.9
- `23:06:57`     Invoke-Endpoint                    USE1-Hst:Data-Bytes-In             $  0.000/day  qty/day 0.0
- `23:06:57`     Invoke-Endpoint                    USE1-Hst:Data-Bytes-Out            $  0.000/day  qty/day 0.0
## CloudTrail - Cost Explorer callers

## Invocations

- `23:07:28` fleet invocations/day 26139; top: [('justhodl-symdir', 7350.8), ('justhodl-sdmx-walker', 1825.2), ('justhodl-student-rsi', 1440.0), ('justhodl-equity-research', 1364.8), ('justhodl-boj-full', 1105.0), ('justhodl-tv-bars', 1052.5), ('justhodl-a2a-bus', 952.6), ('justhodl-edgar-insiders', 575.8), ('justhodl-wl-series-api', 557.0), ('justhodl-fundamental-graphs', 499.5), ('openbb-websocket-handler', 447.0), ('justhodl-event-coordinator', 445.7)]
- `23:07:28` pollers invocations/day: {'justhodl-fleet-error-monitor': 289.2, 'justhodl-ai': 170.4, 'justhodl-health-monitor': 3.0, 'justhodl-event-flow-monitor': 26.0, 'justhodl-import-sentinel': 144.0, 'justhodl-cost-anomaly': 1.0, 'justhodl-fleet-integrity': 1.1, 'justhodl-fleet-monitor': 9.0, 'justhodl-fleet-freshness-monitor': 56.0, 'justhodl-feed-heartbeat': 41.0}
## S3 access logs

- `23:07:28` bucket logging: {'target': 'justhodl-s3-access-logs-857687956942', 'prefix': 'live/', 'format': {'PartitionedPrefix': {'PartitionDateSource': 'EventTime'}}}
- `23:07:28` log objects for 2026-10-10T15:00:00+00:00: 0 (prefix live/857687956942/us-east-1/justhodl-dashboard-live/2026/10/10/2026-10-10-15-)
- `23:07:28` ⚠ no logs for the sample hour; first keys under the prefix: [('live/857687956942/us-east-1/justhodl-dashboard-live/2026/09/14/2026-09-14-00-00-00-281456C4585EB158', '2026-09-15T00:55:37+00:00'), ('live/857687956942/us-east-1/justhodl-dashboard-live/2026/09/14/2026-09-14-00-00-00-44CB71AF33AA4008', '2026-09-15T00:46:12+00:00'), ('live/857687956942/us-east-1/justhodl-dashboard-live/2026/09/14/2026-09-14-00-00-00-44E09BEC1E1A82D9', '2026-09-15T00:38:33+00:00'), ('live/857687956942/us-east-1/justhodl-dashboard-live/2026/09/14/2026-09-14-00-00-00-44E3B9BAC174F0E6', '2026-09-15T00:57:34+00:00'), ('live/857687956942/us-east-1/justhodl-dashboard-live/2026/09/14/2026-09-14-00-00-00-44F0126B96059735', '2026-09-15T00:46:03+00:00')]
## Section errors

- `23:07:28` ⚠ cloudtrail_ce: AccessDeniedException: An error occurred (AccessDeniedException) when calling the LookupEvents operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: cloudtrail:LookupEvents because no identity-based policy allows the cloudtrail:LookupEvents action
- `23:07:28` ✅ wrote aws/ops/reports/6527.json
