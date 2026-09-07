# Architecture (Release 1)

## Components
| Component | Where | Trigger | Writes |
|---|---|---|---|
| Signal core | `aws/shared/jhsignal.py`, `schemas/jhsignal-1.0.json` | library | -- |
| Registry / flags / universe | `config/engine-registry.v1.json`, `config/jh-fusion-flags.json`, `config/jh-fusion-universe.json`, `aws/shared/jh_registry.py` | library (copies bundled into both Lambda zips; tests assert no drift) | `data/jhsignal/state/registry.json` |
| Adapters | `aws/shared/jh_adapters.py` | library | -- |
| Bridge | `aws/lambdas/justhodl-jhsignal-bridge` | Scheduler `justhodl-jhsignal-bridge-hourly` rate(1 hour); `{"mode":"validate_only"}` for dry runs | DDB `justhodl-jhsignal-state`, `data/jhsignal/state/latest.json`, `data/jhsignal/archive/...`, `data/jhsignal/bridge-run.json`, bus events |
| Fusion | `aws/lambdas/justhodl-jh-fusion` + `aws/shared/jh_fusion_core.py` | coordinator route on `jhsignal.batch_published` (async) + Scheduler `justhodl-jh-fusion-daily` 05:20 UTC fallback | `data/jh-fusion.json`, `data/jh-fusion/ledger/...`, bus events |
| Coordinator routes | `aws/lambdas/justhodl-event-coordinator` ROUTES | existing bus rule | audit NDJSON, Telegram on hard veto / critical dependency / regime change |

## Data flow per bridge run
1. Load registry (validated), flags (file + SSM overlay), universe.
2. For each active engine: `get_object(artifact)` -> adapter -> signals (validated twice: builder + bridge).
3. Snapshot: one live signal per entity x `engine#type#horizon` (newest `data_asof` wins), freshness state and
   weight computed, EXPIRED dropped. Diff against the previous snapshot -> new / revised / expired.
4. Write DDB items (ACTIVE/STALE; expired swept to EXPIRED, TTL purges a week later), archive, read model, run report.
5. Publish per-signal facts for changes (cap 250/run), `jhsignal.hard_veto` per hard-veto signal, one
   `jhsignal.batch_published`. Flush metrics.

## Data flow per fusion run
1. Read `data/jhsignal/state/latest.json`, prior `data/jh-fusion.json`, `data/engine-trust.json`,
   `data/signal-scorecard.json`, `data/signal-orthogonality.json` (all optional except the snapshot).
2. Idempotency: a `batch_published` trigger whose snapshot run_id is already fused is skipped.
3. `run_fusion` (pure): regime axis from the market subject; critical dependency check; per pilot entity and horizon
   the scoring in [fusion_scoring.md](fusion_scoring.md); what-changed vs prior; velocity from history.
4. Ledger (gz) + read model; events for material changes; metrics.

## Identity
Entity ids are `type:SYMBOL` (`equity:NVDA`, `etf:SPY`, `crypto:BTC`, `market:US_EQUITY`). Symbols are canonicalised
(`BRK.B` -> `BRK-B`, `X:BTCUSD` -> `BTC`); the pilot universe file supplies aliases and entity types; unknown symbols
default to what the engine declares (ETF board, asset_class) else `equity`. A full security master (ISIN/CUSIP-first)
is Release 3 work on top of symdir.

## Why no new infrastructure
Bus, coordinator, DynamoDB, S3 layout, Scheduler, metrics namespace and the ops runner all existed. Release 1 adds two
Lambdas, one table, five shared modules, three config files and routes. The classic EventBridge rule cap is saturated,
which is exactly why fusion is triggered through the coordinator rather than a new rule.
