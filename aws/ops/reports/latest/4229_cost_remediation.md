# ops 4229 — cost remediation wave 1

**Status:** success  
**Duration:** 1099.9s  
**Finished:** 2026-08-01T05:46:13+00:00  

## Data

| applyon | function | new | old | rule | section |
|---|---|---|---|---|---|
|  | justhodl-fleet-error-monitor | rate(1 hour) | cron(13 13 * * ? *) | justhodl-fleet-error-6h | retune |
|  | justhodl-fleet-error-monitor | rate(1 hour) | rate(15 minutes) | fleet-error-monitor-sched | retune |
|  | justhodl-fleet-monitor | rate(6 hours) | cron(20 */3 * * ? *) | fleet-monitor-3h | retune |
|  | justhodl-fleet-monitor | rate(6 hours) | cron(36 19 * * ? *) | justhodl-fleet-monitor-6h | retune |
|  | justhodl-fleet-monitor | rate(6 hours) | cron(38 14 * * ? *) | justhodl-fleet-monitor-sched | retune |
|  | justhodl-fleet-monitor | rate(6 hours) | rate(3 hours) | fleet-monitor-sched | retune |
|  | justhodl-event-flow-monitor | rate(6 hours) | cron(53 12 * * ? *) | event-flow-monitor-hourly | retune |
|  | justhodl-event-flow-monitor | rate(6 hours) | cron(53 12 * * ? *) | event-flow-monitor-hourly | retune |
|  | justhodl-fleet-freshness-monitor | rate(3 hours) | cron(12 12 * * ? *) | fleet-freshness-monitor-30min | retune |
|  | justhodl-fleet-freshness-monitor | rate(3 hours) | cron(52 19 * * ? *) | justhodl-fleet-freshness-hourly | retune |
|  | justhodl-fleet-freshness-monitor | rate(3 hours) | cron(10 20 * * ? *) | justhodl-fleet-freshness-monitor-sched | retune |
|  | justhodl-fleet-freshness-monitor | rate(3 hours) | cron(56 17 * * ? *) | justhodl-freshness-30m | retune |
|  | justhodl-fleet-freshness-monitor | rate(3 hours) | rate(30 minutes) | fleet-freshness-monitor-sched | retune |
| None | justhodl-stock-analyzer |  |  |  | snapstart |
| None | justhodl-stock-screener |  |  |  | snapstart |
| None | justhodl-investor-agents |  |  |  | snapstart |
| None | justhodl-reports-builder |  |  |  | snapstart |
| None | cftc-futures-positioning-agent |  |  |  | snapstart |
| None | justhodl-morning-intelligence |  |  |  | snapstart |
| None | justhodl-edge-engine |  |  |  | snapstart |

## Log
## 1. Deploy census v1.11.0 — break the AWS recursion alarm

- `05:27:53` pre-fix RecursiveInvocationsDropped(14d) = 5
- `05:27:54` code uploaded, settling by marker…
- `05:27:54` ✅ census v1.11.0 SETTLED (marker verified inside zip)
- `05:42:15` ⚠ census probe: Read timeout on endpoint URL: "https://lambda.us-east-1.amazonaws.com/2015-03-31/functions/justhodl-fundamental-census/invocations"
## 2. Retune CloudWatch pollers (the ~$66/mo line)

- `05:42:40`   justhodl-fleet-error-monitor           events    justhodl-fleet-error-6h        cur=cron(13 13 * * ? *) state=ENABLED
- `05:42:40` ✅      cron(13 13 * * ? *)  ->  rate(1 hour)
- `05:42:40`   justhodl-fleet-error-monitor           scheduler fleet-error-monitor-sched      cur=rate(15 minutes) state=ENABLED
- `05:42:40` ✅      rate(15 minutes)  ->  rate(1 hour)
- `05:42:40`   justhodl-fleet-monitor                 events    fleet-monitor-3h               cur=cron(20 */3 * * ? *) state=ENABLED
- `05:42:40` ✅      cron(20 */3 * * ? *)  ->  rate(6 hours)
- `05:42:40`   justhodl-fleet-monitor                 events    justhodl-fleet-monitor-6h      cur=cron(36 19 * * ? *) state=ENABLED
- `05:42:40` ✅      cron(36 19 * * ? *)  ->  rate(6 hours)
- `05:42:40`   justhodl-fleet-monitor                 events    justhodl-fleet-monitor-sched   cur=cron(38 14 * * ? *) state=ENABLED
- `05:42:40` ✅      cron(38 14 * * ? *)  ->  rate(6 hours)
- `05:42:40`   justhodl-fleet-monitor                 scheduler fleet-monitor-sched            cur=rate(3 hours) state=ENABLED
- `05:42:40` ✅      rate(3 hours)  ->  rate(6 hours)
- `05:42:40` ⚠   justhodl-health-monitor                no schedule found — skipped
- `05:42:40`   justhodl-event-flow-monitor            events    event-flow-monitor-hourly      cur=cron(53 12 * * ? *) state=ENABLED
- `05:42:40` ✅      cron(53 12 * * ? *)  ->  rate(6 hours)
- `05:42:40`   justhodl-event-flow-monitor            events    event-flow-monitor-hourly      cur=cron(53 12 * * ? *) state=ENABLED
- `05:42:40` ✅      cron(53 12 * * ? *)  ->  rate(6 hours)
- `05:42:40`   justhodl-fleet-freshness-monitor       events    fleet-freshness-monitor-30min  cur=cron(12 12 * * ? *) state=ENABLED
- `05:42:40` ✅      cron(12 12 * * ? *)  ->  rate(3 hours)
- `05:42:40`   justhodl-fleet-freshness-monitor       events    justhodl-fleet-freshness-hourl cur=cron(52 19 * * ? *) state=ENABLED
- `05:42:40` ✅      cron(52 19 * * ? *)  ->  rate(3 hours)
- `05:42:40`   justhodl-fleet-freshness-monitor       events    justhodl-fleet-freshness-monit cur=cron(10 20 * * ? *) state=ENABLED
- `05:42:40` ✅      cron(10 20 * * ? *)  ->  rate(3 hours)
- `05:42:40`   justhodl-fleet-freshness-monitor       events    justhodl-freshness-30m         cur=cron(56 17 * * ? *) state=ENABLED
- `05:42:40` ✅      cron(56 17 * * ? *)  ->  rate(3 hours)
- `05:42:40`   justhodl-fleet-freshness-monitor       scheduler fleet-freshness-monitor-sched  cur=rate(30 minutes) state=ENABLED
- `05:42:40` ✅      rate(30 minutes)  ->  rate(3 hours)
## 3. Disable SnapStart on batch functions (~$16/mo)

- `05:42:47` ✅   justhodl-stock-analyzer                  SnapStart OFF
- `05:42:47`   justhodl-ai-chat                         KEPT (interactive path)
- `05:42:48` ✅   justhodl-stock-screener                  SnapStart OFF
- `05:42:48` ✅   justhodl-investor-agents                 SnapStart OFF
- `05:42:48` ✅   justhodl-reports-builder                 SnapStart OFF
- `05:42:49` ✅   cftc-futures-positioning-agent           SnapStart OFF
- `05:42:49` ✅   justhodl-morning-intelligence            SnapStart OFF
- `05:42:50` ✅   justhodl-edge-engine                     SnapStart OFF
## 4. Log retention 14d on never-expiring groups

- `05:46:12` ✅ retention=14d applied to 974 groups (0 skipped, 1070 total)
## 5. Budget alarms on ACTUAL (not only FORECASTED)

- `05:46:13` ⚠ budgets not in IAM scope (An error occurred (AccessDeniedException) when calling the DescribeBudgets operation: User) — set via console/CLI
## 6. Verify — recount CW poll volume after retune

- `05:46:13` expected CW metric requests/day AFTER retune:
- `05:46:13`   fleet-error-monitor  765 fn x 3 metrics x 24 runs = 55,080/day  (~$0.55/day, was ~$2.25/day)
- `05:46:13`   -> CloudWatch line should fall ~$66/mo -> ~$17/mo
- `05:46:13`   (a further 3x is available by moving check_lambda to a single Metrics Insights GROUP BY query — wave 2)
## RESULT

- `05:46:13` actions completed: 21
- `05:46:13`    + census-v1.11.0
- `05:46:13`    + justhodl-fleet-error-monitor:cron(13 13 * * ? *)->rate(1 hour)
- `05:46:13`    + justhodl-fleet-error-monitor:rate(15 minutes)->rate(1 hour)
- `05:46:13`    + justhodl-fleet-monitor:cron(20 */3 * * ? *)->rate(6 hours)
- `05:46:13`    + justhodl-fleet-monitor:cron(36 19 * * ? *)->rate(6 hours)
- `05:46:13`    + justhodl-fleet-monitor:cron(38 14 * * ? *)->rate(6 hours)
- `05:46:13`    + justhodl-fleet-monitor:rate(3 hours)->rate(6 hours)
- `05:46:13`    + justhodl-event-flow-monitor:cron(53 12 * * ? *)->rate(6 hours)
- `05:46:13`    + justhodl-event-flow-monitor:cron(53 12 * * ? *)->rate(6 hours)
- `05:46:13`    + justhodl-fleet-freshness-monitor:cron(12 12 * * ? *)->rate(3 hours)
- `05:46:13`    + justhodl-fleet-freshness-monitor:cron(52 19 * * ? *)->rate(3 hours)
- `05:46:13`    + justhodl-fleet-freshness-monitor:cron(10 20 * * ? *)->rate(3 hours)
- `05:46:13`    + justhodl-fleet-freshness-monitor:cron(56 17 * * ? *)->rate(3 hours)
- `05:46:13`    + justhodl-fleet-freshness-monitor:rate(30 minutes)->rate(3 hours)
- `05:46:13`    + snapstart-off:justhodl-stock-analyzer
- `05:46:13`    + snapstart-off:justhodl-stock-screener
- `05:46:13`    + snapstart-off:justhodl-investor-agents
- `05:46:13`    + snapstart-off:justhodl-reports-builder
- `05:46:13`    + snapstart-off:cftc-futures-positioning-agent
- `05:46:13`    + snapstart-off:justhodl-morning-intelligence
- `05:46:13`    + snapstart-off:justhodl-edge-engine
- `05:46:13` ✅ wrote 4229_cost_remediation.json
