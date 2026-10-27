# ops 6689 - CloudWatch Logs authorization change: Log Group ARN statements next to every affected statement

**Status:** success  
**Duration:** 5.4s  
**Finished:** 2026-10-27T18:28:41+00:00  

## Log
## 1. stricter re-audit of every customer IAM policy (Condition removed for the ARN-matching simulation)

- `18:28:37` customer policies: 121; statements naming a Log Stream ARN with a group-level logs action: 11 (with a Condition: 0; Deny: 0)
- `18:28:37` role_inline justhodl-spend-governor-role / justhodl-spend-governor stmt 0 (Sid Logs, Allow): unaffected (still matched after) | CreateLogGroup@arn:aws:logs:us-east-1:857687956942:* allowed->allowed
- `18:28:37` role_inline lambda-execution-role / justhodl-ai-sagemaker-control stmt 9 (Sid EndpointLogs, Allow): unaffected (still matched after) | FilterLogEvents@/aws/sagemaker/Endpoints/*:log-stream:* allowed->allowed; DescribeLogStreams@/aws/sagemaker/Endpoints/*:log-stream:* allowed->allowed
- `18:28:37` role_inline lambda-liquidity-test-role / liquidity-test-policy stmt 2 (Sid None, Allow): unaffected (still matched after) | CreateLogGroup@arn:aws:logs:us-east-1:857687956942:* allowed->allowed
- `18:28:37` ⚠ role_inline MacroFinancialIntelligenceRole / MacroFinancialIntelligencePolicy stmt 0 (Sid None, Allow): AFFECTED -> + Statement0LogGroupArn | CreateLogGroup@/aws/lambda/macro-financial-intelligence:* allowed->implicitDeny
- `18:28:37` role_inline ofr-lambda-role / ofr-lambda-policy stmt 2 (Sid None, Allow): unaffected (still matched after) | CreateLogGroup@arn:aws:logs:us-east-1:857687956942:* allowed->allowed
- `18:28:37` role_inline openbb-lambda-role / openbb-features-policy stmt 3 (Sid None, Allow): unaffected (still matched after) | CreateLogGroup@arn:aws:logs:us-east-1:857687956942:* allowed->allowed
- `18:28:37` managed /service-role/ / AWSLambdaBasicExecutionRole-df0da75f-68c0-4420-9841-d88508f049a8 stmt 0 (Sid None, Allow): unaffected (still matched after) | CreateLogGroup@arn:aws:logs:us-east-1:857687956942:* allowed->allowed
- `18:28:37` managed /service-role/ / AWSLambdaBasicExecutionRole-2c671c7b-df75-4ee0-ad8d-7e57a8f91e0c stmt 0 (Sid None, Allow): unaffected (still matched after) | CreateLogGroup@arn:aws:logs:us-east-1:857687956942:* allowed->allowed
- `18:28:37` managed / / ScrapeMacroDataS3Policy stmt 1 (Sid None, Allow): unaffected (still matched after) | CreateLogGroup@arn:aws:logs:*:*:* allowed->allowed
- `18:28:37` managed /service-role/ / AWSLambdaBasicExecutionRole-5dd586d6-bbef-4709-ba7e-e71655c552fa stmt 0 (Sid None, Allow): unaffected (still matched after) | CreateLogGroup@arn:aws:logs:us-east-1:857687956942:* allowed->allowed
- `18:28:37` managed /service-role/ / AWSLambdaBasicExecutionRole-8745601f-3564-44f0-bfe6-438dfd391029 stmt 0 (Sid None, Allow): unaffected (still matched after) | CreateLogGroup@arn:aws:logs:us-east-1:857687956942:* allowed->allowed
- `18:28:37` affected IAM policies: 1 (1 statements)
## 1b. resource-based policies: CloudWatch Logs resource policies, CloudWatch Logs VPC endpoints

- `18:28:38` account-scope logs resource policies: 0
- `18:28:38` log-group-scope logs resource policies: 0
- `18:28:38` CloudWatch Logs VPC endpoints: 0
## 2. would a principal actually lose access without the patch (iam:SimulatePrincipalPolicy, informational)

- `18:28:38` role/MacroFinancialIntelligenceRole: 1 of 1 lost grant(s) already allowed by another of its policies (would lose 0)
## 3. patch: back up, check quotas, write, read back, prove

- `18:28:38` ✅ role_inline MacroFinancialIntelligenceRole / MacroFinancialIntelligencePolicy: + {"Sid": "Statement0LogGroupArn", "Effect": "Allow", "Action": "logs:CreateLogGroup", "Resource": "arn:aws:logs:us-east-1:857687956942:log-group:/aws/lambda/macro-financial-intelligence"} (after statement 0) - 1 lost grant(s) now matched on the Log Group ARN, original unchanged (inline aggregate 1000 / 10240)
- `18:28:38` backups: s3://justhodl-ops-private-857687956942/backups/iam-logs-group-arn/20261027T182835Z/ (1 objects)
## 4. whole-account re-audit after the patch

- `18:28:41` statements naming a Log Stream ARN with a group-level logs action: 11 -> {'covered': 1, 'unaffected': 10}
- `18:28:41` ✅ 0 affected statements remain in 121 customer policies
- `18:28:41` RESULT_JSON {"patched": [{"policy": "role_inline MacroFinancialIntelligenceRole / MacroFinancialIntelligencePolicy", "added": [{"Sid": "Statement0LogGroupArn", "Effect": "Allow", "Action": "logs:CreateLogGroup", "Resource": "arn:aws:logs:us-east-1:857687956942:log-group:/aws/lambda/macro-financial-intelligence"}]}], "skipped": [], "review": [], "remaining": []}
- `18:28:41` ✅ ops 6689 complete
