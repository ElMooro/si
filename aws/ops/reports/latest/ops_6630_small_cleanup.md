# ops 6630 - small cleanup: unused secrets, retired alarms (stopped servers held)

**Status:** success  
**Duration:** 18.1s  
**Finished:** 2026-10-23T18:48:46+00:00  

## Data

| alarms_deleted | alarms_kept | failures | secrets_scheduled | servers_held |
|---|---|---|---|---|
| 20 | 6 | 0 | 0 | 2 |

## Log
## A. Secrets Manager

- `18:48:38` scanned the environment of every Lambda function (2211 values)
- `18:48:38` economic-data-api-keys: AccessDeniedException (skipped)
- `18:48:38` fred-api-key: AccessDeniedException (skipped)
- `18:48:38` polygon-api-key: AccessDeniedException (skipped)
- `18:48:38` liquidity-agent-keys: AccessDeniedException (skipped)
- `18:48:38` multi-agent-api-keys: AccessDeniedException (skipped)
- `18:48:38` alphavantage-api-key: AccessDeniedException (skipped)
- `18:48:38` newsapi-key: AccessDeniedException (skipped)
- `18:48:39` ai-assistant/api-keys: AccessDeniedException (skipped)
- `18:48:39` autonomous-ai/keys: AccessDeniedException (skipped)
## B. alarms of retired systems

- `18:48:39` candidates by name: 26
- `18:48:39` retired: EnhancedAPI-5xxErrors (AWS/ApiGateway 5XXError ApiName=rg9o6exoi8,Stage=prod) - no datapoint in 30 days, state INSUFFICIENT_DATA
- `18:48:39` retired: FinancialDashboardAPI-4xxErrors (AWS/ApiGateway 4XXError ApiName=i3y8tfdp1k,Stage=prod) - no datapoint in 30 days, state OK
- `18:48:39` retired: FinancialDashboardAPI-5xxErrors (AWS/ApiGateway 5XXError ApiName=i3y8tfdp1k,Stage=prod) - no datapoint in 30 days, state INSUFFICIENT_DATA
- `18:48:40` retired: JustHodl-Gateway-Errors (AWS/Lambda Errors FunctionName=universal-agent-gateway) - no datapoint in 30 days, state INSUFFICIENT_DATA
- `18:48:40` retired: JustHodl-Gateway-SlowResponse (AWS/Lambda Duration FunctionName=universal-agent-gateway) - no datapoint in 30 days, state INSUFFICIENT_DATA
- `18:48:40` retired: JustHodl-High-Errors (AWS/Lambda Errors FunctionName=universal-agent-gateway) - no datapoint in 30 days, state INSUFFICIENT_DATA
- `18:48:40` retired: OpenBB-API-4XX-Errors (AWS/ApiGateway 4XXError ApiName=rg9o6exoi8) - no datapoint in 30 days, state OK
- `18:48:41` retired: OpenBB-System2-Health (AWS/ECS MemoryUtilization ServiceName=openbb-api-service,ClusterName=openbb-cluster) - no datapoint in 30 days, state OK
- `18:48:42` retired: OpenBB-System2-Unhealthy (AWS/ApplicationELB UnHealthyHostCount LoadBalancer=app/openbb-prod-alb/openbb-prod-alb/8229d68ebb8cc659) - no datapoint in 30 days, state OK
- `18:48:42` retired: OpenBB-System3-Errors (AWS/Lambda Errors FunctionName=enhanced-openbb-handler) - no datapoint in 30 days, state OK
- `18:48:42` retired: OpenBB-System3-Latency (AWS/Lambda Duration FunctionName=enhanced-openbb-handler) - no datapoint in 30 days, state OK
- `18:48:42` retired: aiapi-lambda-duration (AWS/Lambda Duration FunctionName=aiapi-market-analyzer) - no datapoint in 30 days, state INSUFFICIENT_DATA
- `18:48:43` retired: enhanced-openbb-handler-Duration (AWS/Lambda Duration FunctionName=enhanced-openbb-handler) - no datapoint in 30 days, state OK
- `18:48:43` retired: enhanced-openbb-handler-Errors (AWS/Lambda Errors FunctionName=enhanced-openbb-handler) - no datapoint in 30 days, state OK
- `18:48:43` retired: enhanced-openbb-handler-NoInvoke (AWS/Lambda Invocations FunctionName=enhanced-openbb-handler) - no datapoint in 30 days, state INSUFFICIENT_DATA
- `18:48:43` retired: enhanced-openbb-handler-Throttles (AWS/Lambda Throttles FunctionName=enhanced-openbb-handler) - no datapoint in 30 days, state OK
- `18:48:43` retired: indexToOpenSearch-Duration (AWS/Lambda Duration FunctionName=indexToOpenSearch) - no datapoint in 30 days, state OK
- `18:48:43` retired: indexToOpenSearch-Errors (AWS/Lambda Errors FunctionName=indexToOpenSearch) - no datapoint in 30 days, state OK
- `18:48:43` retired: indexToOpenSearch-NoInvoke (AWS/Lambda Invocations FunctionName=indexToOpenSearch) - no datapoint in 30 days, state INSUFFICIENT_DATA
- `18:48:43` retired: indexToOpenSearch-Throttles (AWS/Lambda Throttles FunctionName=indexToOpenSearch) - no datapoint in 30 days, state OK
- `18:48:45` kept: OpenBB-High-Error-Rate - 34326515 datapoints in 30 days
- `18:48:45` kept: OpenBB-High-Latency - 33716572 datapoints in 30 days
- `18:48:45` kept: aiapi-lambda-errors - 327 datapoints in 30 days
- `18:48:45` kept: scrapeMacroData-Duration - 23 datapoints in 30 days
- `18:48:45` kept: scrapeMacroData-Errors - 23 datapoints in 30 days
- `18:48:45` kept: scrapeMacroData-Throttles - 23 datapoints in 30 days
- `18:48:45` ✅ backup of 20 definitions: s3://justhodl-ops-private-857687956942/backups/alarms/retired-systems-20261023T184828Z.json
- `18:48:46` ✅ deleted 20 alarms of retired systems (~$2.00/month)
## C. the two stopped servers (read-only)

- `18:48:46` i-0a38d50ce030fed84 (web server): stopped since User initiated (2026-08-01 13:59:38 GMT); disks [('vol-0411e1740566ce3ea', True)]
- `18:48:46` i-0236308c6be77cee8 (OpenBB Server): stopped since User initiated (2026-08-01 13:59:38 GMT); disks [('vol-03bdc73599783cdf1', True)]
- `18:48:46` Elastic IPs attached to them: 0
- `18:48:46` held: terminating deletes the disks for good and the runner may not snapshot them (seal of 2026-10-20) - waiting for Khalid
- `18:48:46` ✅ cleanup done; secrets recoverable for 30 days, alarm definitions backed up
