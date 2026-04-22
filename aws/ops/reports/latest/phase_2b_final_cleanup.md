# Phase 2b final cleanup — warmers + handler + dup email rule

**Status:** success  
**Duration:** 5.2s  
**Finished:** 2026-04-22T23:59:30+00:00  

## Data

| action | metric | status | target | value |
|---|---|---|---|---|
| delete-rule |  | failed | lambda-warmer-system3 |  |
| delete-rule |  | failed | lambda-warmer-system3-frequent |  |
| delete-lambda |  | deleted | enhanced-openbb-handler |  |
| delete-rule |  | failed | DailyEmailReportsV2_8AMET |  |
|  | total-lambdas |  |  | 95 |

## Log
## Action 1: Delete enhanced-openbb-handler warmer rules

- `23:59:25`   Rule: lambda-warmer-system3
- `23:59:25` ⚠     remove_targets failed: An error occurred (AccessDeniedException) when calling the RemoveTargets operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: events:RemoveTargets on resource: arn:aws:events:us-east-1:857687956942:rule/lambda-warmer-system3 because no identity-based policy allows the events:RemoveTargets action
- `23:59:26`     removed Lambda permission statement 'AllowEventBridge'
- `23:59:26`     removed Lambda permission statement 'AllowEventBridge1751488041'
- `23:59:26`     removed Lambda permission statement 'AllowEventBridge1751488050'
- `23:59:26`     removed Lambda permission statement 'AllowEventBridgeFrequent1751488793'
- `23:59:27` ✗     delete_rule(lambda-warmer-system3) failed: An error occurred (AccessDeniedException) when calling the DeleteRule operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: events:DeleteRule on resource: arn:aws:events:us-east-1:857687956942:rule/lambda-warmer-system3 because no identity-based policy allows the events:DeleteRule action
- `23:59:27`   Rule: lambda-warmer-system3-frequent
- `23:59:27` ⚠     remove_targets failed: An error occurred (AccessDeniedException) when calling the RemoveTargets operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: events:RemoveTargets on resource: arn:aws:events:us-east-1:857687956942:rule/lambda-warmer-system3-frequent because no identity-based policy allows the events:RemoveTargets action
- `23:59:27` ✗     delete_rule(lambda-warmer-system3-frequent) failed: An error occurred (AccessDeniedException) when calling the DeleteRule operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: events:DeleteRule on resource: arn:aws:events:us-east-1:857687956942:rule/lambda-warmer-system3-frequent because no identity-based policy allows the events:DeleteRule action
## Action 2: Delete enhanced-openbb-handler Lambda

- `23:59:28` ✅   Lambda enhanced-openbb-handler deleted
- `23:59:28`     Log group /aws/lambda/enhanced-openbb-handler deleted
## Action 3: Delete duplicate email rule (DailyEmailReportsV2_8AMET)

- `23:59:28`   Keeping: DailyEmailReportsV2  (same cron — fires once, not twice)
- `23:59:28` ⚠     remove_targets failed: An error occurred (AccessDeniedException) when calling the RemoveTargets operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: events:RemoveTargets on resource: arn:aws:events:us-east-1:857687956942:rule/DailyEmailReportsV2_8AMET because no identity-based policy allows the events:RemoveTargets action
- `23:59:29`     removed Lambda permission statement 'DailyEmailReportsV2_8AMET-InvokePermission'
- `23:59:29` ✗     delete_rule(DailyEmailReportsV2_8AMET) failed: An error occurred (AccessDeniedException) when calling the DeleteRule operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: events:DeleteRule on resource: arn:aws:events:us-east-1:857687956942:rule/DailyEmailReportsV2_8AMET because no identity-based policy allows the events:DeleteRule action
## Verification

- `23:59:29` ✅   DailyEmailReportsV2 still exists (the one email rule we want to keep)
- `23:59:29` ✅   justhodl-email-reports-v2 still exists (LastModified: 2025-10-05T15:14:46.000+0000)
- `23:59:30` 
- `23:59:30`   Total Lambdas remaining in account: 95
- `23:59:30` Done
