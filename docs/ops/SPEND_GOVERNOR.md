# Spend governor - the enforced monthly AWS cap

`justhodl-spend-governor` (ops 6519-6524, 2026-10-16) enforces the AWS Budget
**JustHodl-Monthly-Hard-Cap-500**. The budget amount is the single source of truth: change the amount in
the AWS Budgets console and the governor follows it on its next tick.

AWS has no native hard cap. Budget Actions can only attach IAM/SCP policies or stop EC2/RDS: they cannot stop
scheduled or event-driven Lambda invocations (the bulk of this account's spend), and they fire after the
money is spent. Billing data also lags up to a day, and storage, site traffic and alarms keep billing when
everything is stopped. So the governor acts on lines computed **below** the cap.

## How it decides

```
reserve   = residual/day x days_left + baseline/day x 1 day (billing lag) + $10
freeze_at = cap - reserve                (never below 50% of the cap)
brake_at  = freeze_at - max($40, 2 days of baseline)
```

* **MTD** = max(Budget ActualSpend, Cost Explorer month-to-date). Cost Explorer is read every 6 h
  ($0.01 a request), usage types once a day.
* **baseline** = median of the last 14 complete days (a multi-day spike cannot drag it).
* **residual** = min over the last 7 days of the usage types that keep billing when everything is stopped
  (storage, data transfer for the site, hosted zones, alarms, keys, secrets).

| Stage | Trigger | What it does |
|---|---|---|
| NORMAL | MTD below brake_at | nothing; a daily email when the month is on pace to exceed the cap |
| BRAKE | MTD >= brake_at | throttles the Lambdas that make up 80% of the last 72 h of Lambda cost + `brake_always` (justhodl-ai, student-rsi, factory-grader); suspends SageMaker async endpoints to zero; deletes real-time endpoints (config kept); stops SageMaker jobs, notebooks, Studio apps; stops EC2, RDS; pauses App Runner; scales ECS to 0 |
| FREEZE | MTD >= freeze_at | reserved concurrency 0 on every non-exempt function, event source mappings disabled, everything in BRAKE |
| FUSE | any time | a single function burning >= $20/day over the last hour, still invoking in the last 20 min, with the fleet far above normal, at two consecutive checks 0.8-3 h apart, is throttled |

BRAKE/FREEZE hold until **the 1st of the month (UTC)**, **a budget raise**, or **an operator release** -
never because the moving lines drift. FUSE holds survive the 1st and are only lifted by an operator, because
a new month does not fix a runaway.

Every change is written to a ledger with its BEFORE value and restored exactly. If something a human set
during a freeze differs from what the governor left (e.g. concurrency changed), the human change wins and is
left alone. While FROZEN the hourly tick re-applies drift (a new deploy, RDS auto-restart).

Exempt by default: `justhodl-spend-governor`, `justhodl-guardrail-notify`, `justhodl-supabase-keepalive`
(Supabase pauses after 7 idle days and login breaks), `justhodl-dr-snapshot`.

## Where things live

| What | Where |
|---|---|
| Engine | `aws/lambdas/justhodl-spend-governor/` (role `justhodl-spend-governor-role`, reserved concurrency 1) |
| Triggers | Scheduler `justhodl-spend-governor-tick` `cron(7 * * * ? *)`; SNS `justhodl-spend-governor` (only AWS Budgets may publish) |
| State | `s3://justhodl-ops-private-857687956942/spend-governor/state.json` (private, versioned) |
| Control | `s3://justhodl-ops-private-857687956942/spend-governor/control.json` - `mode` (monitor/enforce), `cap_overrides` per month, `exempt_functions`, `exempt_prefixes`, `brake_always`, `thresholds` |
| Audit | `spend-governor/events/<ts>-<kind>.json` (every escalation, restore, fuse trip) |
| Alerts | SNS `justhodl-fleet-alerts` (email + Telegram) and the Budget's own emails at 50/80/90/100% and forecast 100% |
| Dead-man alarms | `justhodl-spend-governor-heartbeat` (no tick for 3 h), `justhodl-spend-governor-errors` |
| Kill-switch canary | `justhodl-spend-governor-canary` - the only function `{"action":"selftest"}` may throttle |

## Operating it

Raise or lower the cap: AWS console -> Billing -> Budgets -> JustHodl-Monthly-Hard-Cap-500 -> edit amount.

Direct invokes (from an ops script, or the AWS CLI with account credentials):

```bash
# status (read-only)
aws lambda invoke --function-name justhodl-spend-governor --cli-binary-format raw-in-base64-out \
  --payload '{"action":"status"}' /dev/stdout
# what a FREEZE would touch (read-only)
aws lambda invoke --function-name justhodl-spend-governor --cli-binary-format raw-in-base64-out \
  --payload '{"action":"plan","stage":"FREEZE"}' /dev/stdout
# release everything the cap holds and stay released for 24 h
aws lambda invoke --function-name justhodl-spend-governor --cli-binary-format raw-in-base64-out \
  --payload '{"action":"restore","suppress_hours":24}' /dev/stdout
# release one runaway-fuse hold after the cause is fixed
aws lambda invoke --function-name justhodl-spend-governor --cli-binary-format raw-in-base64-out \
  --payload '{"action":"restore","include_fuse":true,"only_ids":["justhodl-xyz"]}' /dev/stdout
```

To keep an engine running through a freeze, add it to `exempt_functions` in `control.json`
(a one-off release during a FREEZE is re-applied by the next tick).

## Do not

* Do not complete the `JustHodl-Budget-Exceeded-Deny` Budget Action that another lane started on 2026-10-16
  (it failed AccessDenied; the policy exists unattached). Attached to `lambda-execution-role` it would deny
  S3 writes while EventBridge keeps invoking every engine: the fleet would run, fail and bill. The governor
  supersedes it.
* Do not set reserved concurrency on `justhodl-spend-governor` other than 1, and do not put it on a hitched
  EventBridge rule.

## History

* 2026-10-12..16: miss-calibrator <-> event-coordinator loop, ~$450/day Lambda + S3 0.9 -> 106 TB of
  noncurrent audit copies; October MTD $2,351 when found. Fixed by ops 6520/6521 (see docs/EVENT_BUS.md).
* October 2026 runs with `cap_overrides["2026-10"] = $3,500` (runaway-only guard); $500 from 2026-11-01.
* Normal run-rate before the loop was ~$24/day (~$730/month) - above $500. Until that comes down, expect
  BRAKE around the 15th-18th and FREEZE around the 18th-21st of each month.
