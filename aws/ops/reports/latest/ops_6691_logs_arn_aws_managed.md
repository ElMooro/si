# ops 6691 - AWS-managed policies attached in this account vs the CloudWatch Logs authorization change (read-only)

**Status:** success  
**Duration:** 3.6s  
**Finished:** 2026-10-27T18:33:07+00:00  

## Log
- `18:33:06` attached AWS-managed policies: 73
- `18:33:06` aws-managed AWSLambdaExecute (attached 2) stmt 0 (Sid None, Allow): unaffected (still matched after) | AssociateKmsKey@arn:aws:logs:*:*:* allowed->allowed; CreateExportTask@arn:aws:logs:*:*:* allowed->allowed; CreateLogGroup@arn:aws:logs:*:*:* allowed->allowed; DeleteDataProtectionPolicy@arn:aws:logs:*:*:* allowed->allowed; DeleteLogGroup@arn:aws:logs:*:*:* allowed->allowed; DeleteMetricFilter@arn:aws:logs:*:*:* allowed->allowed
- `18:33:06` ⚠ aws-managed AmazonECSServiceRolePolicy (attached 1) stmt 9 (Sid CWLogStreamManagement, Allow): AFFECTED -> + CWLogStreamManagementLogGroupArn | DescribeLogStreams@/aws/ecs/*:log-stream:* allowed->implicitDeny
- `18:33:06` ⚠ AmazonECSServiceRolePolicy: AWS updates this policy; attached to roles ['AWSServiceRoleForECS'] users []
- `18:33:06` ⚠ aws-managed AmazonSageMakerAdmin-ServiceCatalogProductsServiceRolePolicy (attached 1) stmt 25 (Sid AmazonSageMakerServiceCatalogLogGroupPermission, Allow): AFFECTED -> + AmazonSageMakerServiceCatalogLogGroupPermissionLogGroupArn | CreateLogGroup@:log-stream:* allowed->implicitDeny; DeleteLogGroup@:log-stream:* allowed->implicitDeny; DescribeLogGroups@:log-stream:* implicitDeny->implicitDeny; DescribeLogStreams@:log-stream:* allowed->implicitDeny; PutRetentionPolicy@:log-stream:* allowed->implicitDeny
- `18:33:07` ⚠ AmazonSageMakerAdmin-ServiceCatalogProductsServiceRolePolicy: AWS updates this policy; attached to roles ['AmazonSageMakerServiceCatalogProductsLaunchRole'] users []
- `18:33:07` ⚠ aws-managed AmazonSageMakerCanvasDataPrepFullAccess (attached 3) stmt 29 (Sid LoggingOperation, Allow): AFFECTED -> + LoggingOperationLogGroupArn | CreateLogGroup@/aws/sagemaker/studio:* allowed->implicitDeny
- `18:33:07` ⚠ AmazonSageMakerCanvasDataPrepFullAccess: AWS updates this policy; attached to roles ['AmazonSageMaker-ExecutionRole-20250528T181825', 'AmazonSageMaker-ExecutionRole-20250528T094736', 'AmazonSageMaker-ExecutionRole-20250528T180673'] users []
- `18:33:07` ⚠ aws-managed AppRunnerServiceRolePolicy (attached 1) stmt 1 (Sid None, Allow): AFFECTED -> + Statement1LogGroupArn | DescribeLogStreams@/aws/apprunner/*:log-stream:* allowed->implicitDeny
- `18:33:07` ⚠ AppRunnerServiceRolePolicy: AWS updates this policy; attached to roles ['AWSServiceRoleForAppRunner'] users []
- `18:33:07` affected attached AWS-managed policies: 4
- `18:33:07` ✅ ops 6691 complete (read-only)
