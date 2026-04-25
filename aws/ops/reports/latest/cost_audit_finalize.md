# Cost audit — fix perms, capture full picture, write final doc

**Status:** success  
**Duration:** 19.6s  
**Finished:** 2026-04-25T01:42:01+00:00  

## Data

| lambda_cost_estimate | lambda_gb_seconds | log_groups_no_retention | recs_count | total_30d |
|---|---|---|---|---|
| $30.04 | 2,202,138 | 107 | 6 | $0.00 |

## Log
## 1. Grant Cost Explorer read perm

- `01:41:42` ✅   Attached CostExplorerRead inline policy to github-actions-justhodl
## 2. Cost by service (last 30 days)

- `01:41:47` ⚠   CE still failing: An error occurred (AccessDeniedException) when calling the GetCostAndUsage operation: User: arn:aws:iam::857687956942:user/github-actions-justhodl is not authorized to perform: ce:GetCostAndUsage on resource: arn:aws:ce:us-east-1:857687956942:/GetCostAndUsage because no identity-based policy allows the ce:GetCostAndUsage action
## 3. Top Lambdas by GB-seconds

- `01:42:00`   Total GB-s (30d): 2,202,138
- `01:42:00`   Free tier: 400,000 GB-s
- `01:42:00`   Over free tier: 1,802,138 GB-s
- `01:42:00`   Estimated Lambda cost: $30.04
## 4. Log groups without retention policy

- `01:42:01`   Total log storage: 1.56 GB
- `01:42:01`   Groups without retention: 107
- `01:42:01`     /aws/apprunner/openbb-api/1ccdfbc8a3ab43cca282e6a6fd10a72f/application   803.0MB
- `01:42:01`     /aws/lambda/scrapeMacroData                                    309.2MB
- `01:42:01`     /ecs/openbb-api                                                 64.2MB
- `01:42:01`     /aws/lambda/justhodl-daily-report-v3                            50.5MB
- `01:42:01`     /aws/lambda/justhodl-crypto-intel                               44.3MB
- `01:42:01`     /aws/lambda/justhodl-ultimate-orchestrator                      35.2MB
- `01:42:01`     /aws/lambda/cftc-futures-positioning-agent                      29.8MB
- `01:42:01`     /aws/lambda/openbb-system2-api                                  25.0MB
- `01:42:01`     /aws/lambda/fedliquidityapi                                     22.8MB
- `01:42:01`     /aws/lambda/bond-indices-agent                                  22.3MB
## 5. Build canonical cost audit doc

- `01:42:01` ✅   Wrote: aws/ops/audit/cost_audit_2026-04-25.md (90 lines)
- `01:42:01` Done
