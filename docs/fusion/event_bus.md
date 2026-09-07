# Intelligence bus

Reuses the existing EventBridge custom bus `justhodl-system-events` and `justhodl-event-coordinator`
(see `docs/EVENT_BUS.md`). No new bus, no new rule.

## Events (DetailType), all with the loop-protection envelope
| Event | Producer | Coordinator route |
|---|---|---|
| `jhsignal.published` / `jhsignal.revised` / `jhsignal.expired` | bridge, per material change (flag `FUSION_PER_SIGNAL_EVENTS`: changes_only default, all, off; cap 250/run) | no invoke, no audit (the archive is the record) |
| `jhsignal.batch_published` | bridge, once per run | **invoke justhodl-jh-fusion (async)**, audit |
| `jhsignal.hard_veto` | bridge (per hard-veto signal) and fusion (per newly blocked entity/horizon) | Telegram, audit |
| `jhsignal.soft_veto` | reserved | audit |
| `jhsignal.fusion_changed` | fusion, when a horizon's fusion score moves >= 0.15 or capital_decision flips | audit |
| `jhsignal.critical_dependency_failed` | fusion, on a new CRITICAL engine failure | Telegram, audit |
| `jhsignal.regime_changed` | fusion, when the regime label changes | Telegram, audit |

Envelope (`jhsignal.envelope`): `event_id`, `parent_event_id`, `root_event_id`, `propagation_depth`, `origin_engine`,
`emitted_at` on top of the payload. Fusion events carry the triggering batch event as parent; a publish beyond
`FUSION_MAX_PROPAGATION_DEPTH` (3) is refused and counted (`Bus.suppressed`). Fusion also refuses to recompute a
snapshot it already fused (idempotent trigger), so a batch event can never fan into a loop.

The coordinator payload delivered to fusion is `{trigger_event, trigger_detail, triggered_by, triggered_at}`; the
fusion Lambda also accepts `{"mode": "scheduled"}` (Scheduler) and `{"force": true}`.
