# JustHodl Intelligence Network -- fusion layer

Turns ~880 independent engines into one coordinated investment organisation without rewriting any engine:
engines keep publishing what they publish; a bridge translates their outputs into one canonical signal
(JHSIGNAL-1.0), puts the facts on the existing event bus, keeps a current-state store, archives the stream,
and a deterministic fusion engine combines the evidence per entity and horizon -- explainably.

```
existing engine artifacts (data/*.json)            unchanged
        |  justhodl-jhsignal-bridge (hourly)        aws/lambdas/justhodl-jhsignal-bridge
        v
SignalAdapter x17  -> JHSIGNAL-1.0 (strict)         aws/shared/jh_adapters.py, jhsignal.py
        |-> DynamoDB justhodl-jhsignal-state        current state (entity x engine#type#horizon)
        |-> data/jhsignal/state/latest.json        read model (pages + fusion)
        |-> data/jhsignal/archive/YYYY/MM/DD/*.jsonl.gz   append-only stream
        |-> EventBridge justhodl-system-events     jhsignal.published / revised / expired / hard_veto / batch_published
        v  (coordinator route: batch_published -> async invoke)
justhodl-jh-fusion (shadow mode)                    aws/shared/jh_fusion_core.py
        |-> data/jh-fusion.json                    per entity x horizon: fusion, conviction, confidence, coverage,
        |                                          contradiction, family/cluster scores, vetoes, what changed
        |-> data/jh-fusion/ledger/YYYY/MM/DD/*.json.gz   reproducible ledger
        |-> events: fusion_changed / hard_veto / critical_dependency_failed / regime_changed
```

Documents: [architecture](architecture.md) - [signal schema](signal_schema.md) - [engine registry](engine_registry.md) -
[event bus](event_bus.md) - [entity graph](entity_graph.md) - [hypothesis engine](hypothesis_engine.md) -
[fusion scoring](fusion_scoring.md) - [regime engine](regime_engine.md) - [veto system](veto_system.md) -
[historical replay](historical_replay.md) - [observability](observability.md) - [deployment](deployment.md) - [api](api.md) -
[release 1 plan](release_1_plan.md) - [audit](current_architecture_audit.md) - [gap analysis](gap_analysis.md)

## Release status
| Release | Scope | Status |
|---|---|---|
| 0 | audit, gap analysis, plan | done (this folder) |
| 1 | schema, registry, adapters, bus routes, state store, archive, decay, fusion v1 shadow, tests, metrics | **shipped, ops 5212** |
| 2 | read API v1, Fusion Desk page (`fusion.html`), shadow comparison + graded ledger (`jh_fusion` in justhodl-signals) | **shipped, ops 5215** |
| 3 | entity graph, propagation, trigger router, hypotheses | planned |
| 4 | regime vector, conditional reliability | planned (engines exist) |
| 5 | performance DB views, alpha decay feed, confluence research | planned (engines exist) |
| 6 | analogs, acceleration, asymmetry, opportunity ranking | planned |
| 7 | portfolio fusion, stress, sizing integration | planned |
| 8 | AI CIO | planned |

## Operating rules baked into the code
- Real data only: adapters never fabricate; a missing input yields no signal and a diagnostic.
- Stale is never fresh: exponential decay + FRESH/STALE/EXPIRED; STALE counts half toward coverage.
- Contradictions are preserved: bullish and bearish evidence are separate outputs.
- No double counting: evidence clusters + correlation matrix.
- One stale NONCRITICAL engine lowers coverage; only CRITICAL engines (risk-gate, crisis-composite) missing
  block a capital decision (`CAPITAL_DECISION_BLOCKED`).
- Engines never call each other; everything goes through the bus and the state store.
- Shadow mode: `data/jh-fusion.json` is advisory until the phase-51 comparison window is complete.
