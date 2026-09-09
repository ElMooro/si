# JustHodl.AI Cornerstone AI

`ai.html` is the owner control desk for the `justhodl-ai` service. The current implementation inventories SageMaker, exposes bounded training and deployment actions, organizes Brain notes for semantic retrieval, reads every registered engine feed, produces a governed cross-asset narrative, and grades eligible calls at 5/21/63 trading-day horizons.

## Honest capability boundary

- The Brain classifier predicts the operator-assigned note category. It is a semantic organizer and retrieval aid, not an investment-return model.
- The market read is advisory. It may explain eligible engine evidence, but it cannot allocate capital or override the independent risk authority.
- A market call is suppressed when fusion, risk-gate, or khalid-risk is not fresh, when the fleet registry is unavailable, or when production-scale eligible feed coverage is below 80%.
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
4. Train an XGBoost note-category classifier with a runtime cap and managed spot.
5. Optionally serve the classifier with bounded serverless memory and concurrency.
6. Read the governed engine registry and inspect every unique feed.
7. Admit only fresh, timestamped, non-private evidence into the bounded fleet digest.
8. Ask the governed LLM router for a strict structured interpretation.
9. Validate the output and log only eligible calls.

## Safety controls implemented in the review branch

- Owner and service-token action gate.
- Strict policy types, numeric bounds, and rejection of unknown fields.
- Live pricing and allow-list enforcement for instance-backed resources.
- Serverless memory and concurrency ceilings.
- Managed-resource tag checks before deleting, stopping, or replacing endpoints.
- HyperPod node-count bound and whole-cluster cost projection.
- Endpoint TTL and idle reaping only for engine-managed resources.
- No direct LLM fallback around router budgets or operating modes.
- No raw Brain prose in the narrator prompt.
- Strict stance, side, horizon, confidence, and candidate-symbol validation.
- Deduplicated, successfully logged prediction rows only.
- Private result clearing on sign-out.

## Remaining production blockers

- Dedicated least-privilege Lambda, training, inference, and approval roles.
- Private canonical Brain migration.
- Signal Envelope v1 enforcement and point-in-time feature materialization.
- Outcome labels, transaction costs, liquidity, portfolio exposure, and risk limits.
- Purged/embargoed walk-forward evaluation, calibration, uncertainty, and regime slices.
- Model Registry manual approval, Model Cards, MLflow lineage, and blue/green rollback.
- Append-only prediction ledger with reproducible market data and void reasons.
- Prompt-injection, poisoning, licensing, and data-retention controls.
- Successful clean-account canary from dataset through inference and rollback.

See `CORNERSTONE_ARCHITECTURE_V2.md` and the schemas in this directory.
