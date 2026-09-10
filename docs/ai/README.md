# JustHodl.AI Cornerstone AI

`ai.html` is the owner control desk for the `justhodl-ai` service. The current implementation inventories SageMaker, exposes bounded training and deployment actions, organizes Brain notes for semantic retrieval, reads every registered engine feed, produces a governed cross-asset narrative, and grades eligible calls at 5/21/63 trading-day horizons.

## Honest capability boundary

- The Brain classifier predicts the operator-assigned note category. It is a semantic organizer and retrieval aid, not an investment-return model.
- The market read is advisory. It may explain eligible engine evidence, but it cannot allocate capital or override the independent risk authority.
- A market call is suppressed when fusion, risk-gate, or khalid-risk is not fresh, when registry v2 has fewer than 150 unique feeds, or when eligible feed coverage is below 80%.
- Every production promotion remains blocked until the P0 exit criteria in `CORNERSTONE_ARCHITECTURE_V2.md` are met.

## Data boundary

| Artifact | Boundary | Content |
|---|---|---|
| `data/ai.json` | public read model | statuses, counts, bounded fleet coverage, catalog metadata, safe market-read projection |
| `data/ai/control.json` | public bridge pointer | Function URL pointer and version only |
| `ai/*` | private AI bucket | policy, datasets, embeddings, model artifacts, jobs, complete reads, call ledger |
| Brain note prose | private | may be embedded and retrieved for owner use; never included in the market-read LLM prompt |

The canonical Brain source must move behind the private boundary before production approval. Existing public source compatibility is migration-only and is a release blocker.

## Current pipeline

1. Build a private Brain dataset from note categories.
2. Deploy a compatible financial text-embedding model.
3. Embed the Brain and build a private retrieval index.
4. Keep Brain note-category training disabled: classifier and fine-tune launchers
   now require verified market-outcome labels plus completed purged/embargoed
   walk-forward and CPCV evidence, which note categories do not satisfy.
5. Optionally serve the classifier with bounded serverless memory and concurrency.
6. Read the governed engine registry and inspect every unique feed.
7. Admit only fresh, timestamped, non-private evidence into the bounded fleet digest.
8. Ask the governed LLM router for a strict structured interpretation.
9. Validate the output and log only eligible calls.

## Governed ingestion and feature routes

The owner-only proxy exposes these bounded JSON routes under `/ai`:

- `POST /governance/signals/ingest` validates a clean `SignalEnvelope/v1`.
  `dry_run` defaults to `true` and performs no writes. With
  `"dry_run": false`, the Lambda first writes a durable `PENDING` outbox record
  under `ai/signals/outbox/v1/<fingerprint>.json` in `AI_PRIVATE_BUCKET`, then
  verifies the immutable compatibility archive, claims a bounded delivery
  lease, publishes to `AI_EVENT_BUS`, and conditionally records `PUBLISHED`.
  Retrying the exact same envelope is the redrive operation: a published
  fingerprint is a no-op, while a pending or lease-expired fingerprint is
  retried. Delivery is at-least-once, so downstream consumers must deduplicate
  on the envelope fingerprint.
- `POST /governance/features/assemble` accepts `entity_id`, timezone-aware
  `as_of`, and up to 200 `feature_names`. Without `observations`, it reads the
  configured signal Feature Group's offline Glue table through bounded Athena
  queries, selecting one latest eligible row per entity and feature. Both SQL
  and the adapter enforce `event_time <= as_of`,
  `available_time <= as_of`, exact entity/feature scope, deleted/tainted-row
  exclusion, and a 1,000-row result ceiling. `FEATURE_STORE_ATHENA_WORKGROUP`
  defaults to `primary`; `FEATURE_STORE_QUERY_OUTPUT` defaults to the private
  `ai/feature-store-query-results/` prefix.
- Supplying `observations` to the feature route keeps the dependency-free
  diagnostic path. It is reported as `SUPPLIED_OBSERVATIONS_DRY_RUN`, makes no
  Feature Store or Athena call, and still applies the point-in-time assembler.
  This path is capped at 10,000 supplied rows. `allow_tainted` is accepted only
  for this explicit diagnostic mode and is rejected for live Feature Store
  retrieval.

## Safety controls implemented in the review branch

- Owner and service-token action gate.
- Strict policy types, numeric bounds, and rejection of unknown fields.
- Live pricing and allow-list enforcement for instance-backed resources.
- Serverless memory and concurrency ceilings.
- Managed-resource tag checks before deleting, stopping, or replacing endpoints.
- HyperPod node-count bound and whole-cluster cost projection.
- Mandatory endpoint TTL and idle reaping only for engine-managed resources; new pinned endpoints are rejected.
- AutoML is disabled until worst-case candidate and infrastructure cost can be estimated before launch.
- No direct LLM fallback around router budgets or operating modes.
- No raw Brain prose in the narrator prompt.
- Strict stance, side, horizon, confidence, and candidate-symbol validation.
- Deduplicated, successfully logged prediction rows only.
- Private result clearing on sign-out plus epoch checks that discard in-flight owner responses.
- Mandatory `TrainingEligibility` for every classifier and fine-tune launch,
  resolved from digest-checked, exact-version private S3 receipts and dataset
  and evaluation artifacts. Each receipt must also identify the exact
  versioned train and validation objects corresponding to its source URIs.
  The resolver fetches and hashes those bytes, conditionally copies them to
  `ai/governance/training-inputs/v1/sha256/<digest>/<role>.data`, verifies the
  copied bytes, and supplies only those exact content-addressed object URIs to
  SageMaker. Missing, mismatched, or mutated objects fail before job creation.
  Caller-authored samples, metrics, fold status, or dataset digests are
  rejected. The gate then validates outcome labels,
  positive purge/embargo gaps, recomputed walk-forward and CPCV split digests,
  complete fold results, and stamps lineage digests into each SageMaker
  training request. Learning-curve jobs require a separate receipt whose
  `training_uri` exactly matches each materialized fraction.
- Deployable Signal Feature Store, archive-first prediction ledger, and model
  governance SQS Lambda consumers. Each has its own exact-source-queue
  execution role, bounded data-plane permissions, per-record batch failures,
  and stable idempotency receipts; all event source mappings remain disabled
  in `consumer-functions.template.json` until a reviewed canary.
- Model Registry registration remains `PendingManualApproval`; model-card
  creation remains `Draft`; MLflow lineage records both split-family digests.
  The HTTP handlers compose concrete SageMaker and Managed MLflow clients only
  for an explicit live request. Controlled live calls require a runtime-secret
  approval token (never checked in), matching owner, and hard API-call/cost
  budgets; the Lambda package must install `requirements-governance.txt`.
- Blue-green live execution requires model/endpoint ownership matches and
  captured rollback-plan evidence before green deployment. The live HTTP
  composition binds SageMaker control, SageMaker Runtime, and CloudWatch
  clients to concrete bounded deploy, wait, probe/metric, promote, rollback,
  and rollback-verification callbacks. Rollback is accepted only with proof of
  restored blue configuration, traffic, and at least three health checks.
- Direct `/deploy` and `/deploy-trained` calls fail closed in production; only
  the approval/readiness/canary path may perform a production promotion.
- Both active HTTP prediction write and grade routes use the immutable S3
  archive before the conditional DynamoDB index write. An archive failure
  therefore cannot leave a DynamoDB row.

## Remaining production blockers

- Deployment-time review of the dedicated Lambda, training, inference,
  consumer, and separate human-approval roles.
- Private canonical Brain migration.
- Deployment and canary verification of the disabled queue consumers, including
  DLQ alarms and replay drills.
- Independent source-system provenance verification that training labels,
  transaction costs, liquidity, and market-data snapshots match the immutable
  evaluation-pipeline artifacts.
- Calibration, uncertainty, regime slices, portfolio exposure, and risk limits.
- Separate human approval authority for Model Registry candidates and a
  successful blue-green rollback drill with retained evidence.
- Reproducible market-data snapshots and explicit prediction void reasons.
- Prompt-injection, poisoning, licensing, and data-retention controls.
- Successful clean-account canary from dataset through inference and rollback.

See `CORNERSTONE_ARCHITECTURE_V2.md` and the schemas in this directory.
