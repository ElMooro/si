# Release 1 -- communication foundation (shipped 2026-09-07, ops 5212)

## Delivered
| Item | Files |
|---|---|
| JHSIGNAL-1.0 schema + native model, strict validator, decay, entity ids, envelopes | `schemas/jhsignal-1.0.json`, `aws/shared/jhsignal.py` |
| Engine registry (17 engines, 6 families, criticality, clusters) + loader/validator, flags, universe | `config/engine-registry.v1.json`, `config/jh-fusion-flags.json`, `config/jh-fusion-universe.json`, `aws/shared/jh_registry.py` |
| 17 adapters behind one `SignalAdapter` interface | `aws/shared/jh_adapters.py` |
| Current-state store (DDB) + S3 read model + archive + change detection + bus + metrics | `aws/shared/jh_state_store.py` |
| Fusion v1 core (independence, contradiction, coverage, confidence, vetoes, critical deps, what-changed, velocity) | `aws/shared/jh_fusion_core.py` |
| Bridge Lambda (hourly) | `aws/lambdas/justhodl-jhsignal-bridge/` |
| Fusion Lambda (event-driven + daily fallback, shadow mode) | `aws/lambdas/justhodl-jh-fusion/` |
| Coordinator routes for `jhsignal.*` | `aws/lambdas/justhodl-event-coordinator/source/lambda_function.py` |
| 59 tests (schema x2 validators, adapters, decay, entities, envelope/loop cap, state/diff, fusion math, vetoes, critical deps, replay, end-to-end handlers) | `aws/lambdas/justhodl-jhsignal-bridge/tests/`, `aws/lambdas/justhodl-jh-fusion/tests/` |
| Launch + gate op | `aws/ops/pending/ops_5212_fusion_release1.py` |
| Docs | `docs/fusion/*.md` |

## Pilot universe
market:US_EQUITY (macro subject), etf:SPY QQQ IWM, equity:NVDA TSM ASML AAPL MSFT AMZN GOOGL META, crypto:BTC ETH.
Coverage will be uneven on day one: most FLOW/FUNDAMENTAL/MARKET engines publish bounded top lists, so a mega-cap
appears only when it makes the list -- that is measured as coverage, not hidden.

## Architectural decisions
1. Extend, do not replace: bus, coordinator, DynamoDB, S3 layout, Scheduler, engine-trust, scorecard,
   orthogonality, risk-gate thresholds all reused; conviction-engine and sizing-engine untouched.
2. Adapters own engine shapes; fusion never reads engine JSON directly.
3. Strict schema, never coerce; no fabricated numbers (missing input -> no signal + diagnostic).
4. Shadow mode default; fusion outputs are read models and ledgers, not capital decisions.
5. Append-only S3 keys for streams/ledgers; one small read model per run.
6. Event loop protection in the envelope + idempotent fusion trigger.

## Risks / open items
- Reliability names: engine-trust / scorecard keys are matched by `trust_key` variants (`eng:fortress`,
  `justhodl-fortress`...); engines with no match run at 1.0 (published in `reliability_basis`).
- 13F flows are ~45 days lagged -- INTERMEDIATE horizon and long half-life reflect that.
- The regime axis v1 is a heuristic until Release 4 wires conditional reliability.
- Fusion currently inherits market context for every entity at weight 0.7; entity-specific macro sensitivity
  (betas from impact_mapper) is Release 3/7 work.
- `data/jh-fusion.json` grows with the universe; emergent entities are flagged off until a paged read model exists.

## Release 2 (shipped 2026-09-07, ops 5215)
Read API v1 on the data-proxy worker (see api.md), `fusion.html` Fusion Desk (regime banner, screener with filters,
evidence panel with per-horizon why-bullish / why-bearish / what-changed / contributions / vetoes, opportunities, shadow
comparison), and the phase-51 ledger: every directional best-horizon read is logged as `jh_fusion` into justhodl-signals so
outcome-checker + signal-scorecard grade fusion like any other engine; `data/jh-fusion/shadow.json` shows agreement with
the existing engines. Promotion out of shadow mode = a deliberate `FUSION_SHADOW_MODE` flip once that record matures.
