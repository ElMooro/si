# ops 6688 - CloudWatch Logs authorization change (2027-04-26): which IAM statements grant Log Group actions on Log Stream ARNs (read-only)

**Status:** success  
**Duration:** 2.9s  
**Finished:** 2026-10-27T18:09:36+00:00  

## Log
## 1. AWS Health: the event and its affected entities

- `18:09:33` AWS Health API not available to this account/role (ClientError) - the IAM scan below is the authority
## 2. every customer policy in the account

- `18:09:35` policies scanned: 121 ({'role_inline': 72, 'user_inline': 12, 'group_inline': 2, 'managed': 35})
## 3. Allow statements granting logs actions on Log-Stream-format resources, simulated today vs after

- `18:09:35` ok role_inline justhodl-spend-governor-role / justhodl-spend-governor statement 0 (Sid Logs): 1 group actions still granted after the change
- `18:09:35` ok role_inline lambda-execution-role / justhodl-ai-sagemaker-control statement 9 (Sid EndpointLogs): 2 group actions still granted after the change
- `18:09:35` ok role_inline lambda-liquidity-test-role / liquidity-test-policy statement 2 (Sid None): 1 group actions still granted after the change
- `18:09:35` ⚠ AFFECTED role_inline MacroFinancialIntelligenceRole / MacroFinancialIntelligencePolicy statement 0 (Sid None): ['logs:CreateLogGroup'] on ['arn:aws:logs:us-east-1:857687956942:log-group:/aws/lambda/macro-financial-intelligence:*']
- `18:09:35` ok role_inline ofr-lambda-role / ofr-lambda-policy statement 2 (Sid None): 1 group actions still granted after the change
- `18:09:36` ok role_inline openbb-lambda-role / openbb-features-policy statement 3 (Sid None): 1 group actions still granted after the change
- `18:09:36` ok managed /service-role/ / AWSLambdaBasicExecutionRole-df0da75f-68c0-4420-9841-d88508f049a8 statement 0 (Sid None): 1 group actions still granted after the change
- `18:09:36` ok managed /service-role/ / AWSLambdaBasicExecutionRole-2c671c7b-df75-4ee0-ad8d-7e57a8f91e0c statement 0 (Sid None): 1 group actions still granted after the change
- `18:09:36` ok managed / / ScrapeMacroDataS3Policy statement 1 (Sid None): 1 group actions still granted after the change
- `18:09:36` ok managed /service-role/ / AWSLambdaBasicExecutionRole-5dd586d6-bbef-4709-ba7e-e71655c552fa statement 0 (Sid None): 1 group actions still granted after the change
- `18:09:36` ok managed /service-role/ / AWSLambdaBasicExecutionRole-8745601f-3564-44f0-bfe6-438dfd391029 statement 0 (Sid None): 1 group actions still granted after the change
- `18:09:36` candidate statements: 11; affected: 1
## 4. would the role lose the access (no other policy of the role covers it)?

- `18:09:36` MacroFinancialIntelligenceRole (MacroFinancialIntelligencePolicy / None): would lose 0 action-resource grants []; covered elsewhere 1
## 5. the patch (ops 6689): add these statements, keep the originals

- `18:09:36` role_inline MacroFinancialIntelligenceRole / MacroFinancialIntelligencePolicy: + {"Sid": "Statement0LogGroupArn", "Effect": "Allow", "Action": ["logs:CreateLogGroup"], "Resource": ["arn:aws:logs:us-east-1:857687956942:log-group:/aws/lambda/macro-financial-intelligence"]}
- `18:09:36` PLAN_JSON [{"kind": "role_inline", "owner": "MacroFinancialIntelligenceRole", "policy": "MacroFinancialIntelligencePolicy", "arn": "arn:aws:iam::857687956942:role/MacroFinancialIntelligenceRole", "after_statement_index": 0, "add": {"Sid": "Statement0LogGroupArn", "Effect": "Allow", "Action": ["logs:CreateLogGroup"], "Resource": ["arn:aws:logs:us-east-1:857687956942:log-group:/aws/lambda/macro-financial-intelligence"]}}]
- `18:09:36` ✅ ops 6688 complete (read-only): 1 affected statement(s)
