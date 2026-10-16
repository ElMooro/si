# Event-Bus Coordination Architecture

The system uses an event-driven coordination layer to break the polling
pattern that engines previously relied on. Engines that need to react to
state changes elsewhere in the system no longer wait for their own cron
schedule — they react to events on the bus.

---

## High-level diagram

```
   ┌─────────────────┐    publish     ┌─────────────────────────┐
   │  Producer       │  ──────────→   │   EventBridge custom    │
   │  Lambda         │                │   bus:                  │
   │  (7 today)      │                │   justhodl-system-events│
   └─────────────────┘                └─────────────────────────┘
                                                  │
                                                  │  pattern: source=justhodl.*
                                                  ↓
                                      ┌─────────────────────────┐
                                      │  Rule:                  │
                                      │  justhodl-events-to-    │
                                      │  coordinator            │
                                      └─────────────────────────┘
                                                  │
                                                  ↓
                                      ┌─────────────────────────┐
                                      │  Lambda:                │
                                      │  justhodl-event-        │
                                      │  coordinator            │
                                      └─────────────────────────┘
                                                  │
                       ┌──────────────────────────┼──────────────────────────┐
                       ↓                          ↓                          ↓
              ┌──────────────────┐    ┌──────────────────┐         ┌──────────────────┐
              │ Async invoke 1+  │    │ Telegram alert   │         │ Append to        │
              │ downstream       │    │ (if event in     │         │ S3 audit log     │
              │ Lambda(s)        │    │ NOTIFY routes)   │         │ NDJSON daily     │
              └──────────────────┘    └──────────────────┘         └──────────────────┘
```

---

## Producers (engines that publish events)

| Engine | Events Emitted | Trigger Condition |
|---|---|---|
| `outcome-checker` | `outcome.resolved` | After processing > 0 signals |
| `miss-calibrator` | `calibrator.proposal_high_confidence` | Per HIGH proposal, weekly scheduled run only (it no longer emits `near_miss.extreme` - see Loop safety) |
| `signal-scorecard` | `signal.promoted`, `signal.deprecated` | When a signal newly enters/exits promoted/deprecated status |
| `cross-asset-regime` | `regime.changed` | When `regime_change` variable is non-empty |
| `calibrator` | `calibrator.weights_updated` | After SSM weights write |
| `crisis-plumbing` | `engine.error` | On any uncaught handler exception |
| `liquidity-credit-engine` | `engine.error` | On any uncaught handler exception |

---

## Coordinator routes (consumers)

When an event lands on the bus, the coordinator looks it up in the ROUTES
table and dispatches:

```python
ROUTES = {
    "outcome.resolved":                    {invoke: [calibrator, alpha-calibrator],         notify: False},
    "regime.changed":                      {invoke: [master-ranker, alpha-compass, signal-board], notify: True},
    "regime.flashing_bucket":              {invoke: [alpha-compass],                         notify: True},
    "near_miss.extreme":                   {invoke: [miss-calibrator],                       notify: True},
    "calibrator.proposal_high_confidence": {invoke: [],                                       notify: True},
    "calibrator.weights_updated":          {invoke: [alpha-compass],                          notify: False},
    "signal.deprecated":                   {invoke: [engine-signal-map],                      notify: True},
    "signal.promoted":                     {invoke: [engine-signal-map],                      notify: False},
    "miss.detected":                       {invoke: [],                                       notify: False},
    "engine.error":                        {invoke: [],                                       notify: conditional},
    "signal.fired":                        {invoke: [],                                       notify: False},
}
```

For `engine.error`, Telegram alert fires only if the source engine is in
`CRITICAL_ENGINES` (14 engines including conviction-engine, signal-board,
outcome-checker, calibrator, master-ranker, alpha-compass, crisis-plumbing,
liquidity-credit-engine, crypto-opportunities, crisis-composite,
eurodollar-stress, signal-scorecard, magnitude-distributions, miss-detector,
miss-calibrator).

---

## How to wire a new engine to emit events

```python
# At top of lambda_function.py
from system_events import publish, EVT_REGIME_CHANGED  # or whatever event

# Wherever the transition happens
publish(EVT_REGIME_CHANGED, {
    "previous": "EXPANSION",
    "current":  "CONTRACTION",
    "khalid_score": 38.2,
})
```

That's it. The helper auto-derives `source_engine` from
`AWS_LAMBDA_FUNCTION_NAME`. publish() is fire-and-forget — never raises,
never blocks.

You also need to copy `aws/shared/system_events.py` into the engine's
`source/` directory (the system uses copy-shared-files pattern, not Lambda
layers).

---

## How to add a new route (consumer)

Edit `aws/lambdas/justhodl-event-coordinator/source/lambda_function.py`,
add an entry to `ROUTES`:

```python
ROUTES = {
    ...
    "your.new.event": {
        "invoke": ["justhodl-some-downstream-lambda"],
        "notify": True,   # Telegram alert?
        "audit":  True,   # write to S3 audit log?
    },
}
```

Then redeploy the coordinator (`ops/1022_event_bus_deploy.py` pattern).
No bus / rule / permission changes needed — the rule catches all
`justhodl.*` sources via prefix pattern.

---

## Audit log

Every event the coordinator processes is appended to:

```
s3://justhodl-dashboard-live/system-events/audit/<YYYY-MM-DD>.jsonl
```

Each line is one JSON record:

```json
{
  "ts": "2026-05-31T12:16:42.123456+00:00",
  "event": "outcome.resolved",
  "detail": {"n_resolved": 47, "_source_engine": "outcome-checker"},
  "route": {
    "invokes": [
      {"fn": "justhodl-calibrator",       "ok": true, "status": 202},
      {"fn": "justhodl-alpha-calibrator", "ok": true, "status": 202}
    ],
    "notify": false
  }
}
```

The `system-health.html` dashboard reads today's file and renders it
live alongside the audit findings.

The day-log is rewritten on every append and the bucket is versioned, so it is capped at 5 MB per
day (appends beyond that are skipped and logged); see Loop safety.

---

## Dedupe

The coordinator maintains an in-memory dedupe cache keyed by
`hash(event_name + sorted_detail_keys)`, with a 60-second window. This
prevents duplicate trigger storms when EventBridge delivers the same event
multiple times (which it can do under at-least-once semantics).

The cache is scoped to a warm Lambda container, so cold starts reset it.
That's fine — duplicates are only a problem within the same execution
window.

---

## Loop safety (ops 6520, 2026-10-16)

**Incident.** `miss-calibrator` both consumed `near_miss.extreme` (routed to it by the coordinator) and
re-emitted it on every run, so each run re-invoked itself through the bus: ~33M invocations in 7 days,
~$450/day of Lambda from Oct 12 to 16. EventBridge hops reset Lambda's recursion lineage, so AWS's
16-hop recursive-loop breaker never fired. Every routed event also rewrote the whole day audit log in the
versioned bucket, taking it from 0.9 TB to 106 TB of noncurrent copies.

**Rules, enforced in code:**

1. An engine never emits an event it consumes. `miss-calibrator` runs triggered by the coordinator are
   debounced 6 h and publish nothing.
2. The coordinator refuses to route an event back to the engine that produced it
   (`is_self_loop()` compares `detail._source_engine` / `source` with the target); the invoke is recorded
   as `skipped: self_loop_guard`. Tests: `tests/test_event_coordinator_loop_guard.py`.
3. The audit day-log stops growing at 5 MB (`AUDIT_MAX_BYTES`); `system-events/` has a 1-day
   noncurrent-version expiry (`ops6520-purge-dead-versions-system-events`).
4. Longer cycles (A -> coordinator -> B -> coordinator -> A) are not caught by rule 2; the spend governor's
   runaway fuse (`docs/ops/SPEND_GOVERNOR.md`) throttles any function that burns far above normal for two
   consecutive hourly checks.

---

## Failure modes

- **Bus down**: `publish()` catches the boto3 error, logs, returns False.
  Producer continues.
- **Coordinator down**: events accumulate on bus, will be delivered when
  coordinator recovers (EventBridge holds them).
- **Audit write fails**: coordinator logs error, continues with other
  downstream actions (invokes + notify still happen).
- **Invoke fails**: per-target result includes error. Other targets in
  the same event still get invoked.
- **Telegram fails**: logged, doesn't block other actions.

---

## Operational commands

```bash
# Check bus state
aws events describe-event-bus --name justhodl-system-events

# Read today's audit log
aws s3 cp s3://justhodl-dashboard-live/system-events/audit/$(date -u +%Y-%m-%d).jsonl -

# Manually trigger a test event
aws events put-events --entries '[{
  "Source": "justhodl.test",
  "DetailType": "outcome.resolved",
  "Detail": "{\"n_resolved\": 1}",
  "EventBusName": "justhodl-system-events"
}]'

# Check coordinator's recent invocations
aws logs tail /aws/lambda/justhodl-event-coordinator --follow
```

---

## Naming conventions

- **Event names** use lowercase dot.namespace.format (e.g., `regime.changed`,
  not `regimeChanged` or `RegimeChanged`)
- **Source values** use `justhodl.<engine-name-without-prefix>` (e.g.,
  `justhodl.outcome-checker`)
- **Detail fields** prefixed with `_` are metadata added by the helper
  (e.g., `_emitted_at`, `_source_engine`) and should not be considered
  business data
