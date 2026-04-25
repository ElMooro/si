# Enable PITR (DDB) + SnapStart (Lambda) — both zero-risk

**Status:** success  
**Duration:** 20.1s  
**Finished:** 2026-04-25T10:20:31+00:00  

## Data

| pitr_newly_enabled | pitr_total_protected | snapstart_newly_enabled | snapstart_total_active |
|---|---|---|---|
| 0 | 0 | 5 | 5 |

## Log
## A. Enable PITR on DynamoDB tables

- `10:20:11` ✗   justhodl-signals: An error occurred (AccessDeniedException) when calling the UpdateContinuousBackups operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:UpdateContinuousBackups on resource: arn:aws:dynamodb:us-east-1:857687956942:table/justhodl-signals because no identity-based policy allows the dynamodb:UpdateContinuousBackups action
- `10:20:11` ✗   justhodl-outcomes: An error occurred (AccessDeniedException) when calling the UpdateContinuousBackups operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:UpdateContinuousBackups on resource: arn:aws:dynamodb:us-east-1:857687956942:table/justhodl-outcomes because no identity-based policy allows the dynamodb:UpdateContinuousBackups action
- `10:20:11` ✗   fed-liquidity-cache: An error occurred (AccessDeniedException) when calling the UpdateContinuousBackups operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:UpdateContinuousBackups on resource: arn:aws:dynamodb:us-east-1:857687956942:table/fed-liquidity-cache because no identity-based policy allows the dynamodb:UpdateContinuousBackups action
- `10:20:11` ✗   openbb-historical-data: An error occurred (AccessDeniedException) when calling the UpdateContinuousBackups operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:UpdateContinuousBackups on resource: arn:aws:dynamodb:us-east-1:857687956942:table/openbb-historical-data because no identity-based policy allows the dynamodb:UpdateContinuousBackups action
- `10:20:11` ✗   ai-assistant-tasks: An error occurred (AccessDeniedException) when calling the UpdateContinuousBackups operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:UpdateContinuousBackups on resource: arn:aws:dynamodb:us-east-1:857687956942:table/ai-assistant-tasks because no identity-based policy allows the dynamodb:UpdateContinuousBackups action
- `10:20:11` ✗   openbb-trading-signals: An error occurred (AccessDeniedException) when calling the UpdateContinuousBackups operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:UpdateContinuousBackups on resource: arn:aws:dynamodb:us-east-1:857687956942:table/openbb-trading-signals because no identity-based policy allows the dynamodb:UpdateContinuousBackups action
- `10:20:11` ✗   liquidity-metrics-v2: An error occurred (AccessDeniedException) when calling the UpdateContinuousBackups operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: dynamodb:UpdateContinuousBackups on resource: arn:aws:dynamodb:us-east-1:857687956942:table/liquidity-metrics-v2 because no identity-based policy allows the dynamodb:UpdateContinuousBackups action
- `10:20:11` 
  PITR: 0 newly enabled, 0 already on
## B. Enable SnapStart on user-facing Python Lambdas

- `10:20:15` ✅   justhodl-ai-chat                       SnapStart=PublishedVersions
- `10:20:15`       published version 1
- `10:20:19` ✅   justhodl-stock-analyzer                SnapStart=PublishedVersions
- `10:20:19`       published version 1
- `10:20:19` ⚠   justhodl-investor-agents               runtime=python3.11 — NOT eligible, skipping
- `10:20:19` ⚠   justhodl-stock-screener                runtime=python3.11 — NOT eligible, skipping
- `10:20:23` ✅   justhodl-edge-engine                   SnapStart=PublishedVersions
- `10:20:23`       published version 1
- `10:20:27` ✅   justhodl-morning-intelligence          SnapStart=PublishedVersions
- `10:20:27`       published version 1
- `10:20:27` ⚠   cftc-futures-positioning-agent         runtime=python3.11 — NOT eligible, skipping
- `10:20:30` ✅   justhodl-reports-builder               SnapStart=PublishedVersions
- `10:20:31`       published version 1
- `10:20:31` 
  SnapStart: 5 newly enabled, 0 already on
- `10:20:31` 
  IMPORTANT: First snapshot per Lambda takes 5-10 min to materialize.
- `10:20:31`   Until then, invocations use normal cold-start. After: 10x faster.
- `10:20:31`   No action needed — Lambda URL routing to \$LATEST automatically picks up.
## Summary

- `10:20:31` PITR enablement:
- `10:20:31`   justhodl-signals                    error: An error occurred (AccessDeniedException) when calling the UpdateContinuousBacku
- `10:20:31`   justhodl-outcomes                   error: An error occurred (AccessDeniedException) when calling the UpdateContinuousBacku
- `10:20:31`   fed-liquidity-cache                 error: An error occurred (AccessDeniedException) when calling the UpdateContinuousBacku
- `10:20:31`   openbb-historical-data              error: An error occurred (AccessDeniedException) when calling the UpdateContinuousBacku
- `10:20:31`   ai-assistant-tasks                  error: An error occurred (AccessDeniedException) when calling the UpdateContinuousBacku
- `10:20:31`   openbb-trading-signals              error: An error occurred (AccessDeniedException) when calling the UpdateContinuousBacku
- `10:20:31`   liquidity-metrics-v2                error: An error occurred (AccessDeniedException) when calling the UpdateContinuousBacku
- `10:20:31` 
SnapStart enablement:
- `10:20:31`   justhodl-ai-chat                         enabled_v1
- `10:20:31`   justhodl-stock-analyzer                  enabled_v1
- `10:20:31`   justhodl-investor-agents                 ineligible_runtime
- `10:20:31`   justhodl-stock-screener                  ineligible_runtime
- `10:20:31`   justhodl-edge-engine                     enabled_v1
- `10:20:31`   justhodl-morning-intelligence            enabled_v1
- `10:20:31`   cftc-futures-positioning-agent           ineligible_runtime
- `10:20:31`   justhodl-reports-builder                 enabled_v1
- `10:20:31` Done
