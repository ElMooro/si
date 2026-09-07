# Observability

## CloudWatch namespace `JustHodl/Fusion`
Bridge: SignalsPublished, SignalsRejected, SourceStale, SourceMissing, AdapterFailures (per engine dimension),
StaleSignals, ExpiredSignals, SignalsTotal, EntitiesWithSignals, HardVetoSignals, StateWriteFailures,
ArchiveFailures, EventsPublished, EventPublishFailures, BridgeLatencyMs.
Fusion: FusionLatencyMs, FusionRecomputations, FusionSkipped (reason dimension), EntitiesFused, HardVetoCount,
SoftVetoCount, CoverageMean, ConfidenceMean, CriticalDependencyFailures, FusionChangedEvents, EventPublishFailures.

## Structured logs
One JSON object per line with `trace_id` (= run id), `component`, and where relevant `engine_id`, `entity_id`,
`signal_id`, `fusion_result_id`. Run reports: `data/jhsignal/bridge-run.json` (per-engine status, rejections,
changes, bus counters, state-store result) and `data/jh-fusion.json.stats` + the ledger.

## Alerts
Telegram through the coordinator on `jhsignal.hard_veto`, `jhsignal.critical_dependency_failed`,
`jhsignal.regime_changed`. Engine reliability drift, data freshness and coverage/confidence distributions are
visible as the metrics above; dashboards/alarms are Release 2.
