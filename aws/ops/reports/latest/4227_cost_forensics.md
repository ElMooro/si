# ops 4227 — AWS cost forensics (read-only)

**Status:** success  
**Duration:** 60.9s  
**Finished:** 2026-08-01T05:15:18+00:00  

## Data

| avg_ms | bucket | dropped | errors | events | expr | filter | function | gb | gb_sec | group | inv_14d | invokes | mem | retention | rule | section | service | timeout | usage_type | usd | usd_14d |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | Amazon OpenSearch Service |  |  | 80.64 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | AmazonCloudWatch |  |  | 63.79 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | Amazon SageMaker |  |  | 54.3 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | Amazon Elastic Load Balancing |  |  | 48.16 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | AWS Lambda |  |  | 44.95 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | Amazon Virtual Private Cloud |  |  | 26.76 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | Amazon Elastic Compute Cloud - Compute |  |  | 24.77 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | AWS App Runner |  |  | 14.98 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | Amazon Simple Storage Service |  |  | 13.08 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | AWS Secrets Manager |  |  | 5.23 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | Amazon EC2 Container Registry (ECR) |  |  | 2.66 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | EC2 - Other |  |  | 1.85 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | Amazon DynamoDB |  |  | 1.85 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | service_45d | Amazon Route 53 |  |  | 1.5 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | CW:Requests | 30.23 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | ESInstance:t3.small | 23.47 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | USE1-Studio:JupyterLab-ml.t3.medium | 16.29 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | LoadBalancerUsage | 14.67 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | Lambda-GB-Second | 9.52 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | USE1-PublicIPv4:InUseAddress | 8.15 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | BoxUsage:t2.micro | 7.52 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | Lambda-SnapStart-Cached-GB-S | 7.5 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | USE1-AppRunner-Provisioned-GB-hours | 4.56 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | USW2-Requests-SIA-Tier1 | 1.85 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | USE1-AWSSecretsManager-Secrets | 1.58 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | TimedStorage-ByteHrs | 1.48 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | Lambda-GB-Second-ARM | 1.22 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | CW:AlarmMonitorUsage | 1.14 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | ES:GP3-Storage | 1.07 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | USE1-USW2-AWS-Out-Bytes | 0.87 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | Requests-Tier1 | 0.86 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | Requests-Tier2 | 0.64 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | EBS:VolumeUsage.gp3 | 0.55 |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | usage_type_14d |  |  | ReadRequestUnits | 0.49 |  |
|  |  | 5 |  |  |  |  | justhodl-fundamental-census |  |  |  |  |  |  |  |  | recursive |  |  |  |  |  |
| 664395.3 |  |  | 0 |  |  |  | justhodl-news-velocity |  | 116269.2 |  |  | 350 | 512 |  |  | burn |  |  |  |  | 1.94 |
| 499730.2 |  |  | 30 |  |  |  | justhodl-tradingview |  | 115937.4 |  |  | 116 | 2048 |  |  | burn |  |  |  |  | 1.93 |
| 551523.7 |  |  | 21 |  |  |  | justhodl-data-census |  | 59943.7 |  |  | 37 | 3008 |  |  | burn |  |  |  |  | 1.0 |
| 34661.4 |  |  | 0 |  |  |  | justhodl-fleet-error-monitor |  | 23777.7 |  |  | 1372 | 512 |  |  | burn |  |  |  |  | 0.4 |
| 214855.1 |  |  | 0 |  |  |  | justhodl-thesis-engine |  | 20196.4 |  |  | 32 | 3008 |  |  | burn |  |  |  |  | 0.34 |
| 631610.8 |  |  | 0 |  |  |  | justhodl-insider-cluster-scanner |  | 15158.7 |  |  | 16 | 1536 |  |  | burn |  |  |  |  | 0.25 |
| 249038.8 |  |  | 0 |  |  |  | justhodl-wl-engines |  | 14631.0 |  |  | 20 | 3008 |  |  | burn |  |  |  |  | 0.24 |
| 93038.9 |  |  | 4 |  |  |  | bond-indices-agent |  | 14002.4 |  |  | 301 | 512 |  |  | burn |  |  |  |  | 0.23 |
| 300000.0 |  |  | 84 |  |  |  | justhodl-search-attention |  | 12600.0 |  |  | 84 | 512 |  |  | burn |  |  |  |  | 0.21 |
| 411514.6 |  |  | 0 |  |  |  | justhodl-daily-report-v3 |  | 11522.4 |  |  | 28 | 1024 |  |  | burn |  |  |  |  | 0.19 |
| 648620.7 |  |  | 0 |  |  |  | justhodl-patent-velocity |  | 9080.7 |  |  | 28 | 512 |  |  | burn |  |  |  |  | 0.15 |
| 62216.2 |  |  | 0 |  |  |  | justhodl-fleet-monitor |  | 8274.7 |  |  | 266 | 512 |  |  | burn |  |  |  |  | 0.14 |
| 53163.7 |  |  | 1 |  |  |  | justhodl-fundamental-census |  | 8373.3 |  |  | 105 | 1536 |  |  | burn |  |  |  |  | 0.14 |
| 41551.3 |  |  | 0 |  |  |  | justhodl-research-critique |  | 8247.9 |  |  | 397 | 512 |  |  | burn |  |  |  |  | 0.14 |
| 284425.5 |  |  | 0 |  |  |  | justhodl-sovereign-stress |  | 7963.9 |  |  | 28 | 1024 |  |  | burn |  |  |  |  | 0.13 |
| 50073.9 |  |  | 3 |  |  |  | xccy-basis-agent |  | 7811.5 |  |  | 312 | 512 |  |  | burn |  |  |  |  | 0.13 |
| 525.0 |  |  | 0 |  |  |  | justhodl-tv-notes-ingest |  | 6863.8 |  |  | 13075 | 1024 |  |  | burn |  |  |  |  | 0.12 |
| 51891.9 |  |  | 209 |  |  |  | dollar-strength-agent |  | 6616.2 |  |  | 255 | 512 |  |  | burn |  |  |  |  | 0.11 |
| 35091.8 |  |  | 0 |  |  |  | justhodl-carry-surface |  | 6878.0 |  |  | 196 | 1024 |  |  | burn |  |  |  |  | 0.11 |
| 152293.0 |  |  | 0 |  |  |  | justhodl-finviz-signals |  | 6396.3 |  |  | 84 | 512 |  |  | burn |  |  |  |  | 0.11 |
| 53996.4 |  |  | 211 |  |  |  | manufacturing-global-agent |  | 6749.5 |  |  | 250 | 512 |  |  | burn |  |  |  |  | 0.11 |
| 52171.0 |  |  | 212 |  |  |  | securities-banking-agent |  | 6599.6 |  |  | 253 | 512 |  |  | burn |  |  |  |  | 0.11 |
| 49966.9 |  |  | 176 |  |  |  | volatility-monitor-agent |  | 6345.8 |  |  | 254 | 512 |  |  | burn |  |  |  |  | 0.11 |
| 39582.3 |  |  | 6 |  |  |  | bls-labor-agent |  | 5897.8 |  |  | 298 | 512 |  |  | burn |  |  |  |  | 0.1 |
| 91020.8 |  |  | 2 |  |  |  | justhodl-outcome-checker |  | 5461.2 |  |  | 60 | 1024 |  |  | burn |  |  |  |  | 0.09 |
| 99.1 |  |  | 0 |  |  |  | justhodl-signal-registry-ingest |  | 3483.0 |  |  | 140515 | 256 |  |  | burn |  |  |  |  | 0.09 |
| 594573.3 |  |  | 6 |  |  |  | justhodl-bagger-engine |  | 4756.6 |  |  | 8 | 1024 |  |  | burn |  |  |  |  | 0.08 |
| 25524.9 |  |  | 1 |  |  |  | justhodl-fundamental-graphs |  | 5053.9 |  |  | 396 | 512 |  |  | burn |  |  |  |  | 0.08 |
| 166041.3 |  |  | 0 |  |  |  | justhodl-industry-rotation |  | 4649.2 |  |  | 28 | 1024 |  |  | burn |  |  |  |  | 0.08 |
| 171985.8 |  |  | 0 |  |  |  | justhodl-13f-positions |  | 4127.7 |  |  | 24 | 1024 |  |  | burn |  |  |  |  | 0.07 |
| 80428.3 |  |  | 0 |  |  |  | justhodl-crypto-opportunities |  | 3941.0 |  |  | 98 | 512 |  |  | burn |  |  |  |  | 0.07 |
| 225464.6 |  |  | 0 |  |  |  | justhodl-stock-screener |  | 3945.6 |  |  | 14 | 1280 |  |  | burn |  |  |  |  | 0.07 |
| 112671.0 |  |  | 2 |  |  |  | justhodl-eurodollar-plumbing |  | 3830.8 |  |  | 34 | 1024 |  |  | burn |  |  |  |  | 0.06 |
| 4344.8 |  |  | 138 |  |  |  | justhodl-market-tape |  | 3397.9 |  |  | 4171 | 192 |  |  | burn |  |  |  |  | 0.06 |
| 53726.0 |  |  | 0 |  |  |  | justhodl-portwatch |  | 3331.0 |  |  | 62 | 1024 |  |  | burn |  |  |  |  | 0.06 |
|  | justhodl-dashboard-live |  |  | s3:ObjectCreated:* |  | Prefix=data/report.json | openbb-websocket-broadcast |  |  |  |  |  |  |  |  | s3_trigger |  |  |  |  |  |
|  | justhodl-dashboard-live |  |  | s3:ObjectCreated:* |  | Prefix=data/macro-nowcast.json | openbb-websocket-broadcast |  |  |  |  |  |  |  |  | s3_trigger |  |  |  |  |  |
|  | justhodl-dashboard-live |  |  | s3:ObjectCreated:* |  | Prefix=data/compound-signals.json | openbb-websocket-broadcast |  |  |  |  |  |  |  |  | s3_trigger |  |  |  |  |  |
|  | justhodl-dashboard-live |  |  | s3:ObjectCreated:* |  | Prefix=data/cross-asset-regime.json | openbb-websocket-broadcast |  |  |  |  |  |  |  |  | s3_trigger |  |  |  |  |  |
|  | justhodl-dashboard-live |  |  | s3:ObjectCreated:* |  | Prefix=data/options-flow.json | openbb-websocket-broadcast |  |  |  |  |  |  |  |  | s3_trigger |  |  |  |  |  |
|  | justhodl-dashboard-live |  |  | s3:ObjectCreated:* |  | Prefix=data/eurodollar-stress.json | openbb-websocket-broadcast |  |  |  |  |  |  |  |  | s3_trigger |  |  |  |  |  |
|  | justhodl-dashboard-live |  |  | s3:ObjectCreated:* |  | Prefix=data/nobrainers.json | openbb-websocket-broadcast |  |  |  |  |  |  |  |  | s3_trigger |  |  |  |  |  |
|  | justhodl-dashboard-live |  |  | s3:ObjectCreated:* |  | Prefix=data/narrative-density.json | openbb-websocket-broadcast |  |  |  |  |  |  |  |  | s3_trigger |  |  |  |  |  |
|  |  |  |  |  | cron(0/10 14-20 ? * MON-FRI *) |  | justhodl-trade-ticket-monitor |  |  |  |  |  |  |  | justhodl-trade-ticket-monitor-10min | hot_schedule |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.162 |  | /aws/lambda/justhodl-signal-registry-ingest |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.011 |  | /aws/lambda/justhodl-streaming-fanout |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.009 |  | /aws/lambda/justhodl-fleet-error-monitor |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.006 |  | /aws/lambda/justhodl-news-velocity |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.006 |  | /aws/lambda/justhodl-trade-evaluator |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.005 |  | /aws/lambda/justhodl-equity-research |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.004 |  | /aws/lambda/justhodl-fleet-freshness-monitor |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.004 |  | /aws/lambda/justhodl-intraday-pulse |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.004 |  | /aws/lambda/justhodl-news-wire |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.003 |  | /aws/lambda/justhodl-alpha-compass |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.003 |  | /aws/lambda/justhodl-convergence-radar |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.003 |  | /aws/lambda/justhodl-edgar-insiders |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.003 |  | /aws/lambda/justhodl-event-coordinator |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.003 |  | /aws/lambda/justhodl-history-snapshotter |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 0.003 |  | /aws/lambda/justhodl-market-tape |  |  |  | None |  | logs |  |  |  |  |  |
|  |  |  |  |  |  |  | aiapi-market-analyzer |  |  |  | 302 |  | 10240 |  |  | heavy |  | 900 |  |  | 0.0 |
|  |  |  |  |  |  |  | justhodl-thesis-engine |  |  |  | 32 |  | 3008 |  |  | heavy |  | 900 |  |  | 0.34 |
|  |  |  |  |  |  |  | justhodl-market-internals |  |  |  | 58 |  | 3008 |  |  | heavy |  | 900 |  |  | 0.02 |
|  |  |  |  |  |  |  | justhodl-transcript-indexer |  |  |  | 14 |  | 3008 |  |  | heavy |  | 900 |  |  | 0.0 |
|  |  |  |  |  |  |  | scrapeMacroData |  |  |  | 14 |  | 3008 |  |  | heavy |  | 900 |  |  | 0.0 |
|  |  |  |  |  |  |  | justhodl-wl-engines |  |  |  | 20 |  | 3008 |  |  | heavy |  | 900 |  |  | 0.24 |
|  |  |  |  |  |  |  | justhodl-data-census |  |  |  | 37 |  | 3008 |  |  | heavy |  | 900 |  |  | 1.0 |
|  |  |  |  |  |  |  | justhodl-symbol-dictionary |  |  |  | 6 |  | 2048 |  |  | heavy |  | 900 |  |  | 0.03 |
|  |  |  |  |  |  |  | justhodl-tradingview |  |  |  | 116 |  | 2048 |  |  | heavy |  | 900 |  |  | 1.93 |
|  |  |  |  |  |  |  | justhodl-phase-detector |  |  |  | 14 |  | 2048 |  |  | heavy |  | 900 |  |  | 0.01 |
|  |  |  |  |  |  |  | justhodl-13f-clone-alpha |  |  |  | 2 |  | 2048 |  |  | heavy |  | 880 |  |  | 0.0 |
|  |  |  |  |  |  |  | justhodl-feed-catalog |  |  |  | 21 |  | 2048 |  |  | heavy |  | 840 |  |  | 0.02 |
|  |  |  |  |  |  |  | justhodl-magic-formula |  |  |  | 14 |  | 1536 |  |  | heavy |  | 900 |  |  | 0.0 |
|  |  |  |  |  |  |  | justhodl-gf-value |  |  |  | 14 |  | 1536 |  |  | heavy |  | 900 |  |  | 0.01 |
|  |  |  |  |  |  |  | justhodl-insider-cluster-scanner |  |  |  | 16 |  | 1536 |  |  | heavy |  | 900 |  |  | 0.25 |
|  |  |  |  |  |  |  | justhodl-starmine |  |  |  | 14 |  | 1536 |  |  | heavy |  | 900 |  |  | 0.02 |
|  |  |  |  |  |  |  | justhodl-global-business-cycle |  |  |  | 30 |  | 1536 |  |  | heavy |  | 900 |  |  | 0.04 |
|  |  |  |  |  |  |  | justhodl-master-ranker |  |  |  | 23 |  | 1536 |  |  | heavy |  | 900 |  |  | 0.0 |
|  |  |  |  |  |  |  | justhodl-fundamental-census |  |  |  | 105 |  | 1536 |  |  | heavy |  | 900 |  |  | 0.14 |
|  |  |  |  |  |  |  | justhodl-accumulation-radar |  |  |  | 20 |  | 1536 |  |  | heavy |  | 900 |  |  | 0.0 |

## Log
## A. Cost Explorer — daily cost by service (45d)

- `05:14:17` TOP SERVICES over 45d (total $):
- `05:14:17`    Amazon OpenSearch Service                      $   80.64
- `05:14:17`    AmazonCloudWatch                               $   63.79
- `05:14:17`    Amazon SageMaker                               $   54.30
- `05:14:17`    Amazon Elastic Load Balancing                  $   48.16
- `05:14:17`    AWS Lambda                                     $   44.95
- `05:14:17`    Amazon Virtual Private Cloud                   $   26.76
- `05:14:17`    Amazon Elastic Compute Cloud - Compute         $   24.77
- `05:14:17`    AWS App Runner                                 $   14.98
- `05:14:17`    Amazon Simple Storage Service                  $   13.08
- `05:14:17`    AWS Secrets Manager                            $    5.23
- `05:14:17`    Amazon EC2 Container Registry (ECR)            $    2.66
- `05:14:17`    EC2 - Other                                    $    1.85
- `05:14:17`    Amazon DynamoDB                                $    1.85
- `05:14:17`    Amazon Route 53                                $    1.50
- `05:14:17` 
- `05:14:17` DAILY TOTALS (all services):
- `05:14:17`    2026-06-17  $   7.45 ########                                         Service=1.81 SageMaker=1.22 Lambda=1.19
- `05:14:17`    2026-06-18  $   6.99 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-06-19  $   7.01 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-06-20  $   6.87 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-06-21  $   6.87 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-06-22  $   6.96 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-06-23  $   7.01 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-06-24  $   6.96 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-06-25  $   6.95 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-06-26  $   7.13 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-06-27  $   7.12 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-06-28  $   8.42 ##########                                       Service=1.81 AmazonCloudW=1.23 SageMaker=1.22
- `05:14:17`    2026-06-29  $   9.02 ##########                                       AmazonCloudW=2.01 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-06-30  $   9.71 ###########                                      AmazonCloudW=2.03 Service=1.81 Lambda=1.54
- `05:14:17`    2026-07-01  $   8.22 #########                                        Service=1.81 53=1.50 SageMaker=1.22
- `05:14:17`    2026-07-02  $   6.74 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-07-03  $   6.73 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-07-04  $   6.69 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-07-05  $   6.83 ########                                         Service=1.81 SageMaker=1.22 Balancing=1.08
- `05:14:17`    2026-07-06  $   8.79 ##########                                       AmazonCloudW=2.03 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-07  $   8.80 ##########                                       AmazonCloudW=2.04 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-08  $   8.81 ##########                                       AmazonCloudW=2.06 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-09  $   8.80 ##########                                       AmazonCloudW=2.06 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-10  $   8.83 ##########                                       AmazonCloudW=2.07 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-11  $   8.90 ##########                                       AmazonCloudW=2.07 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-12  $   9.34 ###########                                      AmazonCloudW=2.08 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-13  $   9.77 ###########                                      AmazonCloudW=2.18 Service=1.81 Lambda=1.30
- `05:14:17`    2026-07-14  $   9.68 ###########                                      AmazonCloudW=2.19 Service=1.81 Lambda=1.28
- `05:14:17`    2026-07-15  $   9.72 ###########                                      AmazonCloudW=2.19 Service=1.81 Lambda=1.33
- `05:14:17`    2026-07-16  $   9.51 ###########                                      AmazonCloudW=2.20 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-17  $   9.58 ###########                                      AmazonCloudW=2.20 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-18  $   9.44 ###########                                      AmazonCloudW=2.23 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-19  $   9.51 ###########                                      AmazonCloudW=2.24 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-20  $   9.71 ###########                                      AmazonCloudW=2.26 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-21  $   9.72 ###########                                      AmazonCloudW=2.28 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-22  $   9.71 ###########                                      AmazonCloudW=2.29 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-23  $   9.97 ###########                                      AmazonCloudW=2.31 Service=1.81 Lambda=1.43
- `05:14:17`    2026-07-24  $   9.78 ###########                                      AmazonCloudW=2.31 Service=1.81 Lambda=1.23
- `05:14:17`    2026-07-25  $   9.56 ###########                                      AmazonCloudW=2.31 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-26  $   9.68 ###########                                      AmazonCloudW=2.32 Service=1.81 SageMaker=1.22
- `05:14:17`    2026-07-27  $  10.90 #############                                    AmazonCloudW=2.33 Lambda=2.23 Service=1.81
- `05:14:17`    2026-07-28  $   9.89 ###########                                      AmazonCloudW=2.33 Service=1.81 Lambda=1.26
- `05:14:17`    2026-07-29  $  10.21 ############                                     AmazonCloudW=2.34 Service=1.81 Lambda=1.36
- `05:14:17`    2026-07-30  $  10.95 #############                                    AmazonCloudW=2.35 Lambda=1.98 Service=1.81
- `05:14:17`    2026-07-31  $   5.94 #######                                          AmazonCloudW=1.48 Service=1.05 Lambda=0.76
## A2. Cost Explorer — top USAGE_TYPE (14d)

- `05:14:18`    CW:Requests                                          $   30.23
- `05:14:18`    ESInstance:t3.small                                  $   23.47
- `05:14:18`    USE1-Studio:JupyterLab-ml.t3.medium                  $   16.29
- `05:14:18`    LoadBalancerUsage                                    $   14.67
- `05:14:18`    Lambda-GB-Second                                     $    9.52
- `05:14:18`    USE1-PublicIPv4:InUseAddress                         $    8.15
- `05:14:18`    BoxUsage:t2.micro                                    $    7.52
- `05:14:18`    Lambda-SnapStart-Cached-GB-S                         $    7.50
- `05:14:18`    USE1-AppRunner-Provisioned-GB-hours                  $    4.56
- `05:14:18`    USW2-Requests-SIA-Tier1                              $    1.85
- `05:14:18`    USE1-AWSSecretsManager-Secrets                       $    1.58
- `05:14:18`    TimedStorage-ByteHrs                                 $    1.48
- `05:14:18`    Lambda-GB-Second-ARM                                 $    1.22
- `05:14:18`    CW:AlarmMonitorUsage                                 $    1.14
- `05:14:18`    ES:GP3-Storage                                       $    1.07
- `05:14:18`    USE1-USW2-AWS-Out-Bytes                              $    0.87
- `05:14:18`    Requests-Tier1                                       $    0.86
- `05:14:18`    Requests-Tier2                                       $    0.64
- `05:14:18`    EBS:VolumeUsage.gp3                                  $    0.55
- `05:14:18`    ReadRequestUnits                                     $    0.49
## B. RECURSIVE LOOP — which function did AWS break?

- `05:14:18` functions with a RecursiveInvocationsDropped metric: 1
- `05:14:18` ✗   ROGUE  justhodl-fundamental-census                    dropped=5
## C. Fleet burn — invocations + GB-seconds per function (14d)

- `05:14:25` fleet size: 765 functions
- `05:15:03` LAMBDA COMPUTE 14d = $11.87 across 216,240 invocations (active functions: 729)
- `05:15:03` 
- `05:15:03` FUNCTION                                          INVOKES    GB-SEC   USD14d     MEM   AVG_MS
- `05:15:03` justhodl-news-velocity                                350    116269     1.94     512   664395
- `05:15:03` justhodl-tradingview                                  116    115937     1.93    2048   499730
- `05:15:03` justhodl-data-census                                   37     59944     1.00    3008   551524
- `05:15:03` justhodl-fleet-error-monitor                        1,372     23778     0.40     512    34661
- `05:15:03` justhodl-thesis-engine                                 32     20196     0.34    3008   214855
- `05:15:03` justhodl-insider-cluster-scanner                       16     15159     0.25    1536   631611
- `05:15:03` justhodl-wl-engines                                    20     14631     0.24    3008   249039
- `05:15:03` bond-indices-agent                                    301     14002     0.23     512    93039
- `05:15:03` justhodl-search-attention                              84     12600     0.21     512   300000
- `05:15:03` justhodl-daily-report-v3                               28     11522     0.19    1024   411515
- `05:15:03` justhodl-patent-velocity                               28      9081     0.15     512   648621
- `05:15:03` justhodl-fleet-monitor                                266      8275     0.14     512    62216
- `05:15:03` justhodl-fundamental-census                           105      8373     0.14    1536    53164
- `05:15:03` justhodl-research-critique                            397      8248     0.14     512    41551
- `05:15:03` justhodl-sovereign-stress                              28      7964     0.13    1024   284426
- `05:15:03` xccy-basis-agent                                      312      7812     0.13     512    50074
- `05:15:03` justhodl-tv-notes-ingest                           13,075      6864     0.12    1024      525
- `05:15:03` dollar-strength-agent                                 255      6616     0.11     512    51892
- `05:15:03` justhodl-carry-surface                                196      6878     0.11    1024    35092
- `05:15:03` justhodl-finviz-signals                                84      6396     0.11     512   152293
- `05:15:03` manufacturing-global-agent                            250      6750     0.11     512    53996
- `05:15:03` securities-banking-agent                              253      6600     0.11     512    52171
- `05:15:03` volatility-monitor-agent                              254      6346     0.11     512    49967
- `05:15:03` bls-labor-agent                                       298      5898     0.10     512    39582
- `05:15:03` justhodl-outcome-checker                               60      5461     0.09    1024    91021
- `05:15:03` justhodl-signal-registry-ingest                   140,515      3483     0.09     256       99
- `05:15:03` justhodl-bagger-engine                                  8      4757     0.08    1024   594573
- `05:15:03` justhodl-fundamental-graphs                           396      5054     0.08     512    25525
- `05:15:03` justhodl-industry-rotation                             28      4649     0.08    1024   166041
- `05:15:03` justhodl-13f-positions                                 24      4128     0.07    1024   171986
- `05:15:03` justhodl-crypto-opportunities                          98      3941     0.07     512    80428
- `05:15:03` justhodl-stock-screener                                14      3946     0.07    1280   225465
- `05:15:03` justhodl-eurodollar-plumbing                           34      3831     0.06    1024   112671
- `05:15:03` justhodl-market-tape                                4,171      3398     0.06     192     4345
- `05:15:03` justhodl-portwatch                                     62      3331     0.06    1024    53726
- `05:15:03` 
- `05:15:03` INVOCATION OUTLIERS (>20,000 in 14d):
- `05:15:03` ⚠   justhodl-signal-registry-ingest                140,515 invokes (10037/day)
## D1. S3 -> Lambda notification wiring (loop surface)

- `05:15:06` ⚠   S3 justhodl-dashboard-live -> openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/report.json]
- `05:15:06` ⚠   S3 justhodl-dashboard-live -> openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/macro-nowcast.json]
- `05:15:06` ⚠   S3 justhodl-dashboard-live -> openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/compound-signals.json]
- `05:15:06` ⚠   S3 justhodl-dashboard-live -> openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/cross-asset-regime.json]
- `05:15:06` ⚠   S3 justhodl-dashboard-live -> openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/options-flow.json]
- `05:15:06` ⚠   S3 justhodl-dashboard-live -> openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/eurodollar-stress.json]
- `05:15:06` ⚠   S3 justhodl-dashboard-live -> openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/nobrainers.json]
- `05:15:06` ⚠   S3 justhodl-dashboard-live -> openbb-websocket-broadcast events=s3:ObjectCreated:* filter=[Prefix=data/narrative-density.json]
## D2. Event source mappings (SQS/DDB-stream/Kinesis pumps)

- `05:15:12` ✅ no enabled event source mappings
## D3. High-frequency schedules (rate < 5 min)

- `05:15:13` ⚠   HOT justhodl-trade-ticket-monitor-10min    cron(0/10 14-20 ? * MON-FRI *) -> justhodl-trade-ticket-monitor
- `05:15:13` total EventBridge rules: 445
- `05:15:13` total EventBridge Scheduler schedules: 276
## D4. CloudWatch Logs — storage + retention (silent cost)

- `05:15:18` log groups: 1070   stored: 0.3 GB   never-expiring: 974 (0.3 GB)
- `05:15:18` storage cost ~$0.01/mo at $0.03/GB
- `05:15:18`    /aws/lambda/justhodl-signal-registry-ingest                       0.16 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-streaming-fanout                             0.01 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-fleet-error-monitor                          0.01 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-news-velocity                                0.01 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-trade-evaluator                              0.01 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-equity-research                              0.01 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-fleet-freshness-monitor                      0.00 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-intraday-pulse                               0.00 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-news-wire                                    0.00 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-alpha-compass                                0.00 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-convergence-radar                            0.00 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-edgar-insiders                               0.00 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-event-coordinator                            0.00 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-history-snapshotter                          0.00 GB  ret=None
- `05:15:18`    /aws/lambda/justhodl-market-tape                                  0.00 GB  ret=None
## D5. Functions with unbounded config (900s timeout, big mem)

- `05:15:18` functions with timeout>=600s or mem>=3008MB: 166
- `05:15:18`    aiapi-market-analyzer                          t= 900s mem=10240 inv14d=302 usd=0.00
- `05:15:18`    justhodl-thesis-engine                         t= 900s mem= 3008 inv14d=32 usd=0.34
- `05:15:18`    justhodl-market-internals                      t= 900s mem= 3008 inv14d=58 usd=0.02
- `05:15:18`    justhodl-transcript-indexer                    t= 900s mem= 3008 inv14d=14 usd=0.00
- `05:15:18`    scrapeMacroData                                t= 900s mem= 3008 inv14d=14 usd=0.00
- `05:15:18`    justhodl-wl-engines                            t= 900s mem= 3008 inv14d=20 usd=0.24
- `05:15:18`    justhodl-data-census                           t= 900s mem= 3008 inv14d=37 usd=1.00
- `05:15:18`    justhodl-symbol-dictionary                     t= 900s mem= 2048 inv14d=6 usd=0.03
- `05:15:18`    justhodl-tradingview                           t= 900s mem= 2048 inv14d=116 usd=1.93
- `05:15:18`    justhodl-phase-detector                        t= 900s mem= 2048 inv14d=14 usd=0.01
- `05:15:18`    justhodl-13f-clone-alpha                       t= 880s mem= 2048 inv14d=2 usd=0.00
- `05:15:18`    justhodl-feed-catalog                          t= 840s mem= 2048 inv14d=21 usd=0.02
- `05:15:18`    justhodl-magic-formula                         t= 900s mem= 1536 inv14d=14 usd=0.00
- `05:15:18`    justhodl-gf-value                              t= 900s mem= 1536 inv14d=14 usd=0.01
- `05:15:18`    justhodl-insider-cluster-scanner               t= 900s mem= 1536 inv14d=16 usd=0.25
- `05:15:18`    justhodl-starmine                              t= 900s mem= 1536 inv14d=14 usd=0.02
- `05:15:18`    justhodl-global-business-cycle                 t= 900s mem= 1536 inv14d=30 usd=0.04
- `05:15:18`    justhodl-master-ranker                         t= 900s mem= 1536 inv14d=23 usd=0.00
- `05:15:18`    justhodl-fundamental-census                    t= 900s mem= 1536 inv14d=105 usd=0.14
- `05:15:18`    justhodl-accumulation-radar                    t= 900s mem= 1536 inv14d=20 usd=0.00
- `05:15:18` ✅ wrote 4227_cost_forensics.json
- `05:15:18` OPS 4227 COMPLETE — read-only, nothing mutated.
