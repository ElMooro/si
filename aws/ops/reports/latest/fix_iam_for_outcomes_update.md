# Add UpdateItem on outcomes to github-actions-justhodl

**Status:** success  
**Duration:** 181.3s  
**Finished:** 2026-04-25T20:50:04+00:00  

## Data

| failed | legacy_after | n_null | tagged | untagged |
|---|---|---|---|---|
| 0 | 4410 | 4410 | 4410 | 0 |

## Log
## A. Current policies on github-actions-justhodl

- `20:47:03`   Attached managed policies:
- `20:47:03`     AmazonSSMFullAccess → arn:aws:iam::aws:policy/AmazonSSMFullAccess
- `20:47:03`     CloudWatchReadOnlyAccess → arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess
- `20:47:03`     IAMFullAccess → arn:aws:iam::aws:policy/IAMFullAccess
- `20:47:03`     CloudWatchLogsFullAccess → arn:aws:iam::aws:policy/CloudWatchLogsFullAccess
- `20:47:03`     AmazonDynamoDBReadOnlyAccess → arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess
- `20:47:03`     AmazonS3FullAccess → arn:aws:iam::aws:policy/AmazonS3FullAccess
- `20:47:03`     AmazonEventBridgeFullAccess → arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess
- `20:47:03`     AWSLambda_FullAccess → arn:aws:iam::aws:policy/AWSLambda_FullAccess
- `20:47:03`     AmazonEventBridgeReadOnlyAccess → arn:aws:iam::aws:policy/AmazonEventBridgeReadOnlyAccess
- `20:47:03` 
  Inline policies:
- `20:47:03`     CostExplorerRead
- `20:47:03`       Allow ['ce:GetCostAndUsage', 'ce:GetCostForecast', 'ce:GetDimensionValues', 'ce:GetReservationCoverage', 'ce:GetReservationUtilization'] ON *
- `20:47:03`     DynamoDBContinuousBackups
- `20:47:03`       Allow ['dynamodb:UpdateContinuousBackups', 'dynamodb:DescribeContinuousBackups', 'dynamodb:RestoreTableToPointInTime', 'dynamodb:RestoreTableFromBackup', 'dynamodb:CreateBackup'] ON *
- `20:47:03`     DynamoDBManageTables
- `20:47:03`       Allow ['dynamodb:DeleteTable', 'dynamodb:DescribeTable', 'dynamodb:ListTables', 'dynamodb:UpdateTable'] ON *
## B. Attach inline policy: outcomes-updateitem

- `20:47:03` ✅   Inline policy 'outcomes-updateitem' attached
- `20:47:03`   Waiting 8s for IAM propagation...
## C. Re-tag all correct=None outcomes

- `20:47:12`   Found 4410 correct=None outcomes
- `20:47:51`     Tagged 1000/4410...
- `20:48:30`     Tagged 2000/4410...
- `20:49:09`     Tagged 3000/4410...
- `20:49:47`     Tagged 4000/4410...
- `20:50:03` 
  Tagged: 4410
- `20:50:03`   Failed: 0
- `20:50:03` ✅   ✅ All legacy records tagged successfully
## D. Verify by re-scanning

- `20:50:04`   Tagged legacy:        4410
- `20:50:04`   Untagged correct=None: 0
- `20:50:04`   Real outcomes (T/F):  0
- `20:50:04` ✅   ✅ All correct=None outcomes are tagged
- `20:50:04` Done
