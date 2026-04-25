# Execute safe cost optimizations

**Status:** success  
**Duration:** 21.1s  
**Finished:** 2026-04-25T01:44:04+00:00  

## Data

| eb_rules_disabled | estimated_savings | log_groups_retention_set |
|---|---|---|
| 1 | $4-6/mo | 107 |

## Log
## 1. Set 14-day retention on log groups with no policy

- `01:43:44`   Found 107 log groups without retention
- `01:44:04`   Set 14d retention on 107/107 log groups
- `01:44:04` 
  Top 10 by size (these will free the most space over time):
- `01:44:04`     /aws/apprunner/openbb-api/1ccdfbc8a3ab43cca282e6a6fd10a72f/application   803.0MB
- `01:44:04`     /aws/lambda/scrapeMacroData                                    309.2MB
- `01:44:04`     /ecs/openbb-api                                                 64.2MB
- `01:44:04`     /aws/lambda/justhodl-daily-report-v3                            50.5MB
- `01:44:04`     /aws/lambda/justhodl-crypto-intel                               44.3MB
- `01:44:04`     /aws/lambda/justhodl-ultimate-orchestrator                      35.2MB
- `01:44:04`     /aws/lambda/cftc-futures-positioning-agent                      29.8MB
- `01:44:04`     /aws/lambda/openbb-system2-api                                  25.0MB
- `01:44:04`     /aws/lambda/fedliquidityapi                                     22.8MB
- `01:44:04`     /aws/lambda/bond-indices-agent                                  22.3MB
## 2. Disable scrapeMacroData EventBridge schedule(s)

- `01:44:04`   Found 1 rule(s) targeting scrapeMacroData: ['DailyMacroScraper']
- `01:44:04`     Disabled rule: DailyMacroScraper (was: cron(0 12 * * ? *))
- `01:44:04` ✅   Disabled 1 rule(s) targeting scrapeMacroData
- `01:44:04` 
  To re-enable later (after fixing the Lambda):
- `01:44:04`     aws events enable-rule --name DailyMacroScraper
## 3. Right-size justhodl-health-monitor memory

- `01:44:04`   Skipping right-sizing health-monitor — savings <$0.10/mo, not worth risk
## 4. Cost Explorer access requirement

- `01:44:04`   IMPORTANT: Cost Explorer API access requires a one-time activation in
- `01:44:04`   the AWS Billing console (root account). IAM policies alone aren't enough.
- `01:44:04`   
- `01:44:04`   Steps to enable (one-time, manual):
- `01:44:04`     1. Sign in as root or admin to AWS console
- `01:44:04`     2. Go to Billing & Cost Management → Cost Explorer
- `01:44:04`     3. Click 'Enable Cost Explorer' (free)
- `01:44:04`     4. Then go to Account → IAM User and Role Access to Billing
- `01:44:04`     5. Edit → check 'Activate IAM Access' → Save
- `01:44:04`   
- `01:44:04`   Once done, the cost audit script will pull real $-figures from Cost Explorer.
- `01:44:04`   Until then, we have Lambda GB-second estimates which match well.
- `01:44:04` Done
