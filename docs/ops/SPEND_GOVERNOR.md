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
| FAST FUSE (v1.1.0) | a sustained-runaway alarm goes to ALARM | within seconds: every function burning >= $20/day over the alarm's 15 minutes, carrying >= 10% of the fleet's burn, and at that rate in each 5-minute bucket (a burst that ended never qualifies) is throttled - top 5 per alarm, held as FUSE |

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
| Triggers | Scheduler `justhodl-spend-governor-tick` `cron(7 * * * ? *)`; SNS `justhodl-spend-governor` (AWS Budgets notices -> tick; CloudWatch alarms `justhodl-runaway-*`, `justhodl-event-breaker-open`, `justhodl-s3-live-bucket-size` -> fast fuse) |
| Runaway alarms (ops 6541) | `justhodl-runaway-concurrency` (account avg > 100 for 3x5 min), `justhodl-runaway-invocations` (> 8,000 per 5 min, 3x), `justhodl-runaway-duration` (> 20M ms per 5 min, 3x) - normal Sep 15-Oct 11 never held above 17.7 / 5,303 / 4.8M for 15 min; the Oct 12 storm held 760 / 35,302 / 227M |
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
# run the fast fuse now, exactly as a runaway alarm would (it only throttles sustained burners)
aws lambda invoke --function-name justhodl-spend-governor --cli-binary-format raw-in-base64-out \
  --payload '{"action":"fast_fuse"}' /dev/stdout
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

* 2026-10-12..16: miss-calibrator <-> event-coordinator loop, ~$450/day Lambda + S3 0.9 -> 145 TB of
  noncurrent audit copies; Oct 12-16 cost $2,369 vs ~$21/day normal. Fixed by ops 6520/6521 (see
  docs/EVENT_BUS.md; forensics in ops 6530/6531 reports).
* v1.0.3 (2026-10-17): a failed lever keeps the service's error message in the ledger (`error_msg`), and the
  SageMaker async scale-down is decided on the endpoint's current state (ops 6525 left
  `smasync:endpoint/jh-owned-coder-async/variant/owned` failed with a bare ValidationException). The message
  was "User is missing the following permissions: cloudwatch:PutMetricAlarm, cloudwatch:DeleteAlarms" -
  Application Auto Scaling checks the CALLER for the scaling policy's alarms; ops 6542 granted them.
* S3 egress (~$4.70/day of the normal ~$21/day; ops 6543, 2026-10-17): website data traffic, not engines. Oct 10: ~15 GB/day of internet GETs (Cloudflare 10.9, direct-to-S3 browsers 3.4, the Actions runner 1.1); top files data/backlog.json, earnings-quality, estimate-revisions, share-flows, best-setups. On Oct 15 one Mac browser on Apple's relay network pulled data/best-setups.json straight from S3 at ~10.7 GB/day. Access logs deliver fine: EventTime-partitioned names carry no hour, so sample whole days.
* v1.1.0 (2026-10-17): fast fuse - runaway alarms reach the governor through SNS and sustained burners are
  throttled in seconds (`docs/ops/RUNAWAY_GUARDRAILS.md`). v1.1.1: every tick checks that the runaway alarms
  still exist, are enabled and still notify the governor; problems are e-mailed once a day.
* October 2026 runs with `cap_overrides["2026-10"] = $3,500`; **$600 from 2026-11-01** (budget
  `JustHodl-Monthly-Hard-Cap-600`, ops 6555; the -500 budget is retired by ops 6556).
* v1.2.0 (2026-10-20, Khalid: "never goes past $600 ... every engine once every 24 hours"):
  - **Diffuse-storm fuse.** The Oct 12-16 loop was one hot pair, but a storm spread over many functions each
    under `runaway_fn_usd_day` ($20/day) was invisible to both fuses. When the fleet burns >= `diffuse_fleet_usd_day`
    ($75/day, ~18x normal) and >= 3x its baseline, the hottest functions covering 60% of the burn (each >= $2/day,
    at most 30) are throttled: two strikes in the hourly path (now checked every 2 h, `fuse_interval_h`), and in
    the fast path when every 5-minute bucket of the alarm window stayed hot. Alerts say "diffuse storm".
  - **Cadence sentinel.** Once a day the governor lists every enabled scheduled rule and Scheduler schedule and
    checks it with `cadence_core` (the same rules as `tests/deployment/test_cadence_floor.py`). Anything that can
    fire more than once on a day is retimed to a daily expression (enforce mode, `cadence_enforce`), recorded under
    `state.cadence.fixed`, and e-mailed. Undo one: `{"action":"cadence_restore","names":["<name>"]}` and add
    `"<kind>:<name>"` to `control.json cadence_exempt`. The governor's own hourly tick is exempt by construction.
    Manual run: `{"action":"cadence_check"}`. Rights: ops 6555 (events/scheduler list+retime, PassRole to the
    Scheduler role only).
  - **Egress alert.** The last complete day's data-transfer-out above `egress_alert_usd_day` ($5 = ~55 GB) is
    e-mailed once a day (the October site leak was ~118 GB/day, invisible under the Lambda storm).
  - The once-a-day rollout itself: ops 6552-6554 (341 triggers retimed/disabled, 8,081 -> 759 scheduled
    firings a day, plan in `config/cadence-plan.json`, backups in `backups/cadence-floor/`).
* v1.3.0 (2026-10-20, Khalid: "any engine or events that cause $50 in a month shuts down automatically and
  inform me and won't turn on again unless I manually turn it on and address the issue"):
  - **Meter.** Every hourly tick (not while frozen) reads each function's Invocations since the last read, then
    Duration for the ones that ran, and adds duration x memory x list price + requests to `state.meter.mtd`;
    the first read of each day re-reads the whole month so late metric points cannot drift it. ~$0.01 a read
    (~$8/month). The fuse reuses the same read, so its own hourly metric reads are gone.
  - **Engine cap (kill switch).** A function whose month-to-date Lambda cost reaches `engine_cap_usd_month`
    ($50; per-function overrides in `control.json` `"engine_caps": {"justhodl-x": 120}`, 0 = uncapped) is held
    at concurrency 0 under ledger stage **CAP**. CAP outranks BRAKE / FREEZE / FUSE (an entry already held is
    upgraded, `capped_from` kept) and **no automatic path lifts it**: not the 1st of the month, not a budget
    raise, not `{"action":"restore"}` (even with `include_fuse`), not a stage change; a hand change of the
    concurrency is re-held on the next tick. Khalid is e-mailed "ENGINE SHUT DOWN: <engine>" with the amount
    and the exact release command. Release, after the cause is fixed:
    `{"action":"release_cap","function":"<name>","confirm":"<name>","reason":"..."}` - restores the exact
    previous concurrency and the counter starts again from zero for that engine (`mtd_at_release`).
    `{"action":"caps"}` lists held engines, overrides, releases and the month-to-date top 25;
    `{"action":"meter"}` forces a read; `{"action":"cap_selftest"}` round-trips the switch on the canary.
  - What it does NOT see: S3 request / storage charges an engine causes (the Oct 12-16 S3 balloon is bounded
    by the lifecycle rule Khalid disabled on Oct 19 and watched by the bucket-size alarm -> fast fuse), and
    the exempt control plane (the governor itself, guardrail-notify, supabase-keepalive, dr-snapshot).
* 2026-10-16 22:53 UTC: Khalid chose to FREEZE the rest of October (ops 6525, `{"action":"force","stage":"FREEZE"}`):
  914 Lambdas + the owned SageMaker async endpoint held; the 2026-11-01 00:07 UTC tick restores them.
* 2026-10-19: ops 6545 proved the freeze live (922 of 926 Lambdas at 0, only the 4 exempt ran in 24 h; 19 lane
  changes during the freeze were re-held by drift, the slowest after 28 min). On Khalid's instruction the 11
  scheduled GitHub workflows that touch AWS were disabled too; `.github/workflows/freeze-resume.yml` re-enables
  them 2026-11-01 (00:23, retries 03:23/06:23 UTC) only once this governor reports a stage other than FREEZE,
  then disables itself. One-command check from Git Bash: `scripts/justhodl-engines-off-check.sh`.
* Normal run-rate before the loop was ~$24/day (~$730/month) - above $500. Cuts shipped 2026-10-16:
  justhodl-ai caches Cost Explorer 6 h (~$1.6/day), fleet-error-monitor hourly v1.2.0 (~$5.5/day).
  If the run-rate still exceeds ~$16/day the governor will BRAKE late in the month.
