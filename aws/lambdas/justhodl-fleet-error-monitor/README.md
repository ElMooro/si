# justhodl-fleet-error-monitor

Deployed: 2026-05-22 as part of AUDIT_2026-05-22.md institutional foundation.

**Schedule:** once a day - EventBridge Scheduler `fleet-error-monitor-sched` `rate(24 hours)` (02:29 UTC). The classic
rules `fleet-error-monitor-5min` and `justhodl-fleet-error-6h` are DISABLED (cadence floor, 2026-10-20).

## Purpose
This Lambda is part of the JustHodl.AI institutional observability foundation.
It runs autonomously and alerts via SNS + Telegram when conditions are met.

## Environment variables required
- `SNS_ARN`: arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts
- `TELEGRAM_BOT_TOKEN`: Khalid's bot token
- `TELEGRAM_CHAT_ID`: Khalid's chat ID
- `ERROR_RATE_THRESHOLD` (default 5.0 %)
- `MIN_INVOCATIONS` (default 5 — filter noise)
- `LOOKBACK_MINUTES` (default 15 min)

## Dead letters (v1.3.0, 2026-11-20, ops 6856 / 6857)

Every Lambda sends the async events it gives up on to the shared queue `justhodl-dlq-default` (14-day retention). The
queue's messages do not name the function and its depth is a stock, so "depth >= 1" was a standing CRITICAL page: on
2026-11-20 it read 3,255, of which 2,845 were bus events refused by `justhodl-event-coordinator` (held at reserved
concurrency 0 by the spend governor since Oct 20) and ~400 were ~25 engines failing their daily run, none named.

Now each run reads CloudWatch `AsyncEventsDropped` per function over the last `DLQ_LOOKBACK_HOURS` (25) - one
GetMetricData call per 500 functions, about $0.01 a run - and splits the result:

| Who | Meaning | What happens |
|---|---|---|
| running engine | its run failed every attempt (or the event aged out) | named in one `DLQ:` line; WARNING, or CRITICAL from `DLQ_CRITICAL_COUNT` (100) in the window |
| engine at reserved concurrency 0 | the hold refuses every event it is sent | counted in the same line as "Not failures"; never an alert on its own |

An engine not reported in the last `DLQ_REALERT_HOURS` (168) is reported on the next run; a standing set is repeated
weekly, not daily; a storm follows the ordinary 60-minute window. The alert history keeps `<DLQ>:engines`
{function: last reported}. If CloudWatch cannot be read the old depth rule applies, marked "per-engine arrivals
unavailable". The public report (`data/_fleet-monitor.json`) still carries the queue's depth; the invoke response
carries the per-engine breakdown (`dead_letters.failing` / `.held`).

Forensics of the queue itself (received, never deleted): `aws/ops/STAGED/ops_6856_dlq_forensics.py`.
