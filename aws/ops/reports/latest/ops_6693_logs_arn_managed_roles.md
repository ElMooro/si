# ops 6693 - the roles behind the four AWS-managed policies with Log-Stream-ARN statements (read-only)

**Status:** success  
**Duration:** 11.3s  
**Finished:** 2026-10-27T18:49:04+00:00  

## Log
- `18:49:03` lambda functions: 914 on 14 distinct roles
- `18:49:03` AWSServiceRoleForAppRunner (AppRunnerServiceRolePolicy): trust ['{"Service": "apprunner.amazonaws.com"}'] | last used 2026-08-01 14:00:15+00:00 us-east-1 | Lambda functions on it: 0 []
- `18:49:03` AWSServiceRoleForECS (AmazonECSServiceRolePolicy): trust ['{"Service": "ecs.amazonaws.com"}'] | last used 2025-07-28 20:34:38+00:00 us-east-1 | Lambda functions on it: 0 []
- `18:49:03` AmazonSageMaker-ExecutionRole-20250528T094736 (AmazonSageMakerCanvasDataPrepFullAccess): trust ['{"Service": "sagemaker.amazonaws.com"}'] | last used 2026-08-01 14:09:33+00:00 us-east-1 | Lambda functions on it: 0 []
- `18:49:03` AmazonSageMaker-ExecutionRole-20250528T180673 (AmazonSageMakerCanvasDataPrepFullAccess): trust ['{"Service": "sagemaker.amazonaws.com"}'] | last used 2025-05-28 22:23:32+00:00 us-east-1 | Lambda functions on it: 0 []
- `18:49:04` AmazonSageMaker-ExecutionRole-20250528T181825 (AmazonSageMakerCanvasDataPrepFullAccess): trust ['{"Service": "sagemaker.amazonaws.com"}'] | last used None  | Lambda functions on it: 0 []
- `18:49:04` AmazonSageMakerServiceCatalogProductsLaunchRole (AmazonSageMakerAdmin-ServiceCatalogProductsServiceRolePolicy): trust ['{"Service": "servicecatalog.amazonaws.com"}'] | last used None  | Lambda functions on it: 0 []
- `18:49:04` roles: 6; used by a Lambda function: 0
- `18:49:04` ✅ ops 6693 complete (read-only)
