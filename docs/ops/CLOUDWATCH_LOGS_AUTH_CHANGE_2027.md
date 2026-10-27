# CloudWatch Logs authorization change (effective 2027-04-26) - handled 2026-10-27

AWS Health, "CloudWatch planned lifecycle event" (scheduled change, action required, account-specific, start
2027-04-26 00:00 UTC-7, "Affected resources: 2"), pasted by Khalid on 2026-10-27.  From that date an IAM statement
grants a Log **Group** action only on the Log Group ARN `arn:aws:logs:<region>:<account>:log-group:<name>`; a Log
Stream ARN (`...:log-group:<name>:*` or `...:log-stream:<name>`) stops granting it.  Four actions stay stream-scoped:
PutLogEvents, GetLogEvents, CreateLogStream, DeleteLogStream.  AWS's recommendation: next to every such Allow
statement add an equivalent statement on the Log Group ARN and keep the original.

## What was checked (evidence: `aws/ops/reports/latest/ops_668{8,9}_*.md`, `ops_669{1,3}_*.md`)

| Scope | Count | Result |
|---|---|---|
| Customer IAM policies (role / user / group inline, customer-managed default versions) | 121 (72 / 12 / 2 / 35) | 11 statements name a Log Stream ARN with a group action; each simulated (iam:SimulateCustomPolicy) against today's ARN and the post-change Log Group ARN, **with any Condition removed** so a conditioned statement cannot hide (there were 0), Allow **and** Deny (a Deny on a stream ARN would silently stop denying - there were 0) |
| - unaffected | 10 | `CreateLogGroup` on the account-wide `arn:aws:logs:us-east-1:857687956942:*` (spend-governor, liquidity-test, ofr, openbb, four console-made `AWSLambdaBasicExecutionRole-*`, `ScrapeMacroDataS3Policy` on `arn:aws:logs:*:*:*`) or a group-level resource in the same statement (`lambda-execution-role` / `EndpointLogs`) |
| - **affected** | **1** | `MacroFinancialIntelligenceRole` / inline `MacroFinancialIntelligencePolicy`, statement 0: `logs:CreateLogGroup` on `log-group:/aws/lambda/macro-financial-intelligence:*` |
| CloudWatch Logs resource policies (account and log-group scope) | 0 | - |
| CloudWatch Logs VPC endpoints | 0 | - |
| AWS-managed policies attached in the account | 73 | 4 carry such a statement - **AWS maintains these**: `AmazonECSServiceRolePolicy` (AWSServiceRoleForECS), `AmazonSageMakerAdmin-ServiceCatalogProductsServiceRolePolicy` (AmazonSageMakerServiceCatalogProductsLaunchRole), `AmazonSageMakerCanvasDataPrepFullAccess` (three `AmazonSageMaker-ExecutionRole-20250528*` roles), `AppRunnerServiceRolePolicy` (AWSServiceRoleForAppRunner).  ops 6693: each of the six roles trusts only its own service (ecs / apprunner / sagemaker / servicecatalog), none of the 914 Lambda functions runs on any of them (the fleet uses 14 roles), last use 2025-05-28 to 2026-08-01 |

## The fix (ops 6689, 2026-10-27 18:28 UTC)

Added right after statement 0 of `MacroFinancialIntelligencePolicy`, the original kept:

```json
{"Sid": "Statement0LogGroupArn", "Effect": "Allow", "Action": "logs:CreateLogGroup",
 "Resource": "arn:aws:logs:us-east-1:857687956942:log-group:/aws/lambda/macro-financial-intelligence"}
```

Backed up first to `s3://justhodl-ops-private-857687956942/backups/iam-logs-group-arn/20261027T182835Z/`; quota
checked (role inline aggregate 1,000 / 10,240 characters); read back and proven (original statements unchanged and in
order, the lost grant now matched on the Log Group ARN, the companion inside the original's action / group /
Condition); whole-account re-audit: **0 affected statements remain in 121 policies**.  No access was at risk even
before the patch: another policy of the role already grants CreateLogGroup there (iam:SimulatePrincipalPolicy), and the
engine is switched off.

The repository's design files for the unexecuted justhodl-ai cutover stack carried the same pattern
(`WriteSageMakerLogs`, `ReadEndpointLogs`); each now has its Log Group ARN companion (`WriteSageMakerLogGroups`,
`ReadEndpointLogGroups`) in the policy files and the CloudFormation template (commit 820c940f2), so that stack cannot
create an affected statement if it is ever deployed.  No deploy lane or script writes IAM policies with logs group
actions.

## Why the event says 2 and the scan says 1

The Health API that lists an event's affected entities (`health:DescribeAffectedEntities`) needs a Business support
plan (ops 6688: `ClientError`), so the two names are not readable programmatically.  The account-wide scan above is the
authority: one customer statement could lose a grant and it is fixed; the only other statements of this shape in the
account are inside AWS-managed policies, which AWS updates.

## Re-check before the deadline

`aws/ops/STAGED/ops_6689_logs_group_arn_fix.py` is idempotent - on a clean account it writes nothing and reports
0 affected - and `ops_6691_logs_arn_aws_managed.py` re-reads the AWS-managed policies.  Dispatch both after any IAM
change and once in March 2027:

```
gh api -X POST repos/ElMooro/si/actions/workflows/run-ops-direct.yml/dispatches -f ref=main -f 'inputs[script]=STAGED/ops_6689_logs_group_arn_fix.py'
```
