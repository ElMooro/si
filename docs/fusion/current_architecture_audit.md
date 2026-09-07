# JustHodl Intelligence Network -- Release 0: current architecture audit

Audited from the repository at HEAD 082964a/6756785 (2026-09-07) by reading engine
sources, `config/*.json`, `aws/shared/*.py`, the workflows and `docs/EVENT_BUS.md`.
Nothing below is inferred from memory; every claim names the file it comes from.

## Execution model
- ~881 Lambda functions under `aws/lambdas/<fn>/source/lambda_function.py` (+ `config.json`), python3.12,
  role `arn:aws:iam::857687956942:role/lambda-execution-role`, region us-east-1.
- Shared code is **copied into every zip** from `aws/shared/*.py` (no Lambda layers) -- `deploy-lambdas.yml`
  and `aws/ops/_lambda_deploy_helpers.build_zip` both bundle it; a local file of the same name wins.
- Deploy = git push: `pages.yml` (site), `deploy-lambdas.yml` (existing functions; does **not** reliably create
  new ones), `deploy-workers.yml` (Cloudflare), `run-ops.yml` (runs `aws/ops/pending/ops_NNNN_*.py` on the runner,
  moves them to `aws/ops/ran/`, writes `aws/ops/reports/latest/*.md`, regenerates `STATE.md`).
- Scheduling: classic EventBridge rules are **at the account cap**; new cadences use EventBridge Scheduler
  (`justhodl-scheduler-role`), see `AUTONOMY.md` and `config/schedule-manifest.json`. A fan-out router
  (`justhodl-scheduler`, `config/fanout-manifest.json`) invokes members that have no rule of their own.
- Sandbox (Claude) has no AWS egress; all AWS calls run in ops scripts on the GitHub Actions runner.

## Existing EventBridge usage (the intelligence bus already exists)
- Custom bus `justhodl-system-events`, rule `justhodl-events-to-coordinator` (source prefix `justhodl.*`) ->
  Lambda `justhodl-event-coordinator` (`aws/lambdas/justhodl-event-coordinator`, ROUTES table: async invoke /
  Telegram / S3 audit NDJSON `system-events/audit/YYYY-MM-DD.jsonl`).
- Helper `aws/shared/system_events.py`: `publish(event_name, detail, source_engine)` -> `Source=justhodl.<engine>`,
  `DetailType=<event_name>`; never raises. Producers today: outcome-checker, miss-calibrator, signal-scorecard,
  cross-asset-regime, calibrator, crisis-plumbing, liquidity-credit-engine, plus buzz/ticker-trends/sec/ark/patent
  producers. Unknown detail types are logged and dropped by the coordinator (cheap).
- Coordinator downstream payload: `{trigger_event, trigger_detail, triggered_by, triggered_at}` (async).

## Queues / topics
- None in the intelligence path. SQS/SNS are not used for engine coordination; Telegram (SSM `/justhodl/telegram/*`)
  is the human notification channel via the coordinator and `justhodl-alert-router`.

## S3 conventions
- Bucket `justhodl-dashboard-live` (versioned; Deny-Delete policies on data prefixes; lifecycle purges noncurrent
  versions after 1 day under `data/`). Site reads only `data/*` (Cloudflare route `justhodl.ai/data/*`);
  anything outside `data/` is unreachable from pages.
- Engines write `data/<engine>.json` (+ `data/<engine>-history.json`, `data/<engine>/history/...`, warehouse lanes
  under `data/warm/<provider>/`). `config/engine-contracts.json` holds 866 **learned** contracts keyed by artifact
  (rows_path, required_keys, max_age_hours). `engine-manifest.json` maps 858 engines -> keys they write.
- Rewriting a fixed key set on a schedule bills forever on a versioned bucket (Aug-2026 cost anomaly): the fusion
  layer therefore writes **append-only** archive/ledger objects and one small read model per run.

## Databases
- DynamoDB: `justhodl-signals` (graded point-in-time picks, written by `aws/shared/signals_emit.py`, the
  harvester and ~40 direct emitters) and `justhodl-outcomes` (outcome-checker). Both are the learning ledger,
  not a current-state store. `justhodl-history-snapshotter` creates its own table at runtime (the role can).
- SSM: `/justhodl/calibration/weights`, `/justhodl/calibration/accuracy` (calibrator), Telegram secrets.
- No relational store; no graph database; Supabase only for auth.

## Conviction / sizing / risk / LCE
- `justhodl-conviction-engine` (`data/conviction.json`): ingests 15 engines (FEEDS table) as 5-state signals,
  skill-weights by `data/signal-scorecard.json` multipliers, decorrelates within engine family, groups by
  **subject** ("Broad risk / equity beta", "US equity -- value tilt", "Crypto"...), scores 0-100, publishes an
  INVALIDATION text per subject. It is not per-ticker.
- `justhodl-sizing-engine` v1.0.1 (`data/sizing.json`): fractional Kelly from graded outcomes, calibrated
  confidence, 30% vol targeting, cluster haircut vs accepted recs; `justhodl-position-sizer-v2`
  (`data/position-sizing.json`), `justhodl-risk-sizer`, `justhodl-portfolio-sizer`, proven-portfolio COMPOSER.
- `justhodl-risk-gate` v2.3 (`data/risk-gate.json`): six brain-cited legs -> posture RISK_ON/NEUTRAL/RISK_OFF/SEVERE,
  SIZING 1.0/.75/.45/.20, composite -10..10; `justhodl-crisis-composite` (master_crisis_score, DEFCON);
  `justhodl-tail-risk` (p_drop_10 per index); `justhodl-credit-composite`; katlin's war room fuses 16 of these
  into a thermometer with hard vetoes.
- LCE = `justhodl-liquidity-credit-engine` (`data/liquidity-credit-engine.json`): composite is a STRESS score
  0-100 (NORMAL/WATCH/ELEVATED/CRISIS ranks, worst-of 70% + mean 30%) with a regime label.

## Existing fusion logic (macro level)
- `justhodl-engine-fusion` (`fusion_engine.py`, `config/fusion-schema.v1.json`, `fusion-registry.v1.json`,
  `fusion-policy.v1.json`, `fusion-subscriptions.v1.json` -> `data/engine-fusion.json`): evidence packets with
  evidence_id/source_id/subject/domain/direction RISK_ON|NEUTRAL|RISK_OFF/score/confidence/freshness/provenance/
  ancestry/source_family/evidence_level/independence_eligible; dedupe by subject+domain+source_family; thresholds
  risk_off 65 / hard_veto 80 / risk_on 35; `risk_effect: tighten_or_veto_only`. Subject-level (market), ~10 sources.
- `justhodl-apex-fusion` (pump conviction across five engines, scorecard-weighted), `justhodl-wl-fusion`,
  `justhodl-sector-capital-fusion`, best-setups/master-ranker priors -- each a bespoke per-purpose fusion.

## Calibration / weights / learning loop
- `signals_emit.log_signal` -> DDB -> `justhodl-outcome-checker` -> `justhodl-signal-scorecard` (Wilson LB,
  promotion/deprecation, multipliers) -> `justhodl-calibrator` (SSM weights) / `alpha-calibrator` /
  `miss-calibrator`; `aws/shared/calibration.py` (weight(signal_type), blend_score) and
  `aws/shared/evidence_weights.py` (Wilson-shrunk component weights) are the consumer helpers.
- `justhodl-signal-halflife` (per-engine decay curves, half-life), `justhodl-alpha-decay` (OOS watchdog),
  `justhodl-regime-conditional-trust` (per-regime engine factor), `justhodl-engine-trust` +
  `aws/shared/engine_trust.py` (fleet trust gate, `data/engine-trust.json`), `justhodl-signal-orthogonality`
  (pairwise engine correlation matrix + clusters from `data/calibration-fleet-history.json`),
  `justhodl-signal-harvester` (logs every engine's top_picks as `eng:<engine>` so the loop sees the whole fleet).

## Signal schemas and identifiers
- Three coexisting schemas: DDB `justhodl-signals` rows (signal_id `type#TICKER#date`, predicted_dir, check_windows,
  baseline_price...), fusion-v1 packets (macro), and each engine's own JSON (rows keyed by `ticker`).
- Identity is ticker-only everywhere (Finviz hyphen form `BRK-B` vs Polygon dot form `BRK.B`; crypto `X:BTCUSD`).
  No canonical entity id, no ISIN/CUSIP-first master (13F resolves CUSIP->ticker inside its own engine).
- Entity master, partial: `justhodl-symdir` (1.37M searchable docs, `/search`, `/series`), finviz-universe,
  fundamental-census (~215 cols). `aws/shared/impact_mapper.py` (impact-map/1.0) carries a small beneficiary graph
  with betas; fortress holds an industry->ETF map; `etf-flows/stock-exposure-lookup.json` gives ETF constituent pressure.

## Historical analogs / backtesting / replay
- `justhodl-historical-analogs` (6-dim FRED state vector, 252d z-scores, Euclidean top-15, SPY forward returns),
  `justhodl-positioning-analog`, `justhodl-episode-compass`. Macro-level only; no asset-specific analogs.
- Backtests: `justhodl-research-backtest`, `justhodl-signal-backtest`, fortress/katlin walk-forward modes,
  `data/calibration-fleet-history.json` snapshots. No fleet-wide point-in-time replay of a fused decision.

## Stale-data logic
- Per-engine `max_age_hours` in `config/engine-contracts.json` + `justhodl-contract-gate`; `justhodl-feed-registry`
  freshness ledger; fusion-v1 `stale_evidence_action: emit_health_trace_but_no_active_packet`; import-sentinel
  dead-lane chip (state doc age > 48h). No exponential decay anywhere -- freshness is binary.

## Observability
- `aws/shared/_sentry_lite.track_errors` (self-hosted error capture), `justhodl-fleet-error-monitor`,
  `justhodl-event-flow-monitor`, `justhodl-cost-anomaly` (put_metric_data users), S3 access logs, Storage Lens.
  No custom CloudWatch namespace for signal/fusion quality; logs are print-based, not structured JSON.

## Frontend / API contracts
- Static pages read `data/*.json` through the Cloudflare `justhodl-data-proxy` worker (edge cache, Range
  passthrough). `home.js` renders any feed through learned `engine-contracts` rows_path; section numbering
  (`jh-sections.js`) addresses blocks as `page#n`. No versioned JSON API beyond `/data/*`, symdir routes and the
  page-AI/ai-chat endpoints.

## AI / LLM integrations
- `aws/shared/llm_router.py`, `anthropic_shim.py`, `claude_compat.py`; page-AI (chart-pro), justhodl-ai-chat,
  auction-desk grounded note (Haiku, from computed facts only, cached per day), brain regime-read. All consume
  computed numbers; none produce scores.

## Where the proposed fusion architecture overlaps existing code
| Proposed | Existing | Verdict |
|---|---|---|
| Intelligence bus | `justhodl-system-events` + coordinator + `system_events.publish` | REUSE (add routes) |
| Engine registry | fusion-registry.v1 (10 macro sources), engine-contracts (866 learned), engine-manifest | EXTEND with a persisted `config/engine-registry.v1.json` |
| Signal schema | fusion-schema.v1 packets, DDB signals rows | ADD JHSIGNAL-1.0 alongside, adapters translate |
| Current-state store | none (DDB tables are ledgers) | ADD `justhodl-jhsignal-state` |
| Decay | binary staleness only | ADD exponential half-life decay |
| Conviction (per entity) | subject-level conviction-engine; per-ticker rankers in silos | ADD per-entity fusion v1, keep conviction-engine authoritative |
| Reliability | engine-trust, scorecard multipliers, regime-conditional-trust | REUSE as fusion inputs |
| Correlation | signal-orthogonality matrix | REUSE as the correlation adjustment |
| Vetoes | risk-gate posture/sizing, fusion-policy thresholds, katlin hard vetoes | REUSE thresholds, formalise HARD/SOFT on the signal |
| Analogs | historical-analogs (macro) | REUSE; asset-specific = Release 6 |
| Sizing | sizing-engine, position-sizer-v2, risk-gate multiplier | UNCHANGED (shadow mode) |
