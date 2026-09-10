# Cornerstone AI Architecture v2

The target is a governed interpretation and forecasting layer, not one giant model. It consumes immutable outputs from every engine, builds point-in-time features, runs specialist models, records disagreement and vetoes, produces a cited advisory verdict, and grades every prediction out of sample.

## Design principles

- **Quant computes, AI explains.** Deterministic engines and approved statistical models produce measurements. The LLM narrates their evidence and cannot create executable capital permission.
- **Every input is attributable.** Every signal carries event time, availability time, schema version, source URI, content hash, lineage, quality, and taint.
- **No silent filling.** Missing, stale, untimestamped, malformed, unlicensed, or private inputs remain visible but are ineligible evidence.
- **No recursive learning.** Cornerstone narratives and model-derived outputs are tainted and cannot enter an upstream engine training set. Recursive model-generated training data can cause model collapse ([Nature](https://www.nature.com/articles/s41586-024-07566-y)).
- **Promotion is a separate act.** Trained artifacts enter SageMaker Model Registry as `PendingManualApproval`; no deployment route serves an unapproved package ([AWS Model Registry](https://docs.aws.amazon.com/sagemaker/latest/dg/model-registry.html), [AWS approval workflow](https://docs.aws.amazon.com/sagemaker/latest/dg/model-registry-approve.html)).
- **Fail closed.** Missing provenance, freshness, grounding, risk approval, or cost information suppresses calls rather than silently degrading.

## Target data flow

1. Each engine emits `SignalEnvelope/v1` to an EventBridge custom bus.
2. Validation rejects malformed envelopes to a dead-letter queue.
3. Accepted envelopes are appended to an Iceberg raw ledger and projected into SageMaker Feature Store online and offline groups. Feature Store supports online serving plus an offline historical store ([AWS Feature Store](https://docs.aws.amazon.com/sagemaker/latest/dg/feature-store.html)).
4. Point-in-time datasets enforce `feature.available_ts <= prediction.as_of_ts`; AWS documents point-in-time queries specifically to reduce leakage ([AWS point-in-time datasets](https://aws.amazon.com/blogs/machine-learning/build-accurate-ml-training-datasets-using-point-in-time-queries-with-amazon-sagemaker-feature-store-and-apache-spark/)).
5. SageMaker Pipelines process, train, evaluate, calibrate, and register candidate models.
6. An independent approver reviews the evaluation bundle and Model Card.
7. Approved challengers run in shadow mode before blue/green promotion. SageMaker deployment guardrails support controlled traffic shifting and rollback ([AWS deployment guardrails](https://docs.aws.amazon.com/sagemaker/latest/dg/deployment-guardrails.html)).
8. The decision service combines specialist outputs, correlated-family deduplication, disagreements, risk vetoes, and portfolio constraints into `Verdict/v2`.
9. A grounded narrator converts the verdict into a human-readable cross-asset read with citations.
10. Predictions enter an append-only ledger and are graded at 5/21/63 trading days, gross and net of costs.

## Model portfolio

- **Time series:** Chronos-2 for probabilistic 5/21/63-day forecasts with covariates; compare against seasonal-naive and existing engine baselines ([Chronos-2 model card](https://huggingface.co/amazon/chronos-2)).
- **Tabular fusion:** XGBoost meta-model over point-in-time engine features, with documented monotonic constraints where economically justified ([XGBoost monotonic constraints](https://xgboost.readthedocs.io/en/stable/tutorials/monotonic.html)).
- **Financial text:** FinBERT-tone and SEC-focused encoders for tone and filing features. Evaluate retrieval on finance-specific tasks rather than assuming general embedding rankings transfer ([FinMTEB research](https://arxiv.org/abs/2409.18511)).
- **Retrieval:** hybrid lexical and vector retrieval with reranking, source entitlements, temporal filters, and citation spans.
- **Narration:** a model-pluggable Bedrock path behind versioned prompts, Guardrails, contextual-grounding thresholds, and full invocation logging ([AWS Guardrails](https://docs.aws.amazon.com/bedrock/latest/userguide/guardrails.html), [contextual grounding](https://docs.aws.amazon.com/bedrock/latest/userguide/guardrails-contextual-grounding-check.html)).

Do not couple the system to one Bedrock model. AWS now documents explicit Active, Legacy, and End-of-Life lifecycle states, so model substitution and regression tests are mandatory ([AWS Bedrock model lifecycle](https://docs.aws.amazon.com/bedrock/latest/userguide/model-lifecycle.html)).

## Evaluation and approval

Every candidate must produce:

- purged and embargoed walk-forward results with a 63-trading-day contamination boundary;
- performance by regime, asset class, liquidity bucket, and confidence decile;
- calibration curve, Brier score, log loss, expected calibration error, and coverage of prediction intervals;
- directional accuracy, information coefficient, turnover, drawdown, and gross/net returns;
- transaction-cost and slippage assumptions;
- baseline and incumbent comparisons with confidence intervals;
- leakage, duplication, taint, feature-freshness, and licensing checks;
- data snapshot ID, code commit, image digest, hyperparameters, feature manifest, MLflow run ID, and Model Card version.

Managed MLflow is the experiment and GenAI trace spine ([AWS Managed MLflow](https://docs.aws.amazon.com/sagemaker/latest/dg/mlflow.html)). Model Monitor and Clarify should not be treated as the future default because AWS says they are no longer open to new customers; use CloudWatch, MLflow, Evidently, and explicit batch evaluation instead ([AWS Model Monitor availability](https://docs.aws.amazon.com/sagemaker/latest/dg/model-monitor-availability-change.html)).

## Release gates

The desk remains `ADVISORY_ONLY` until all of these are true:

- every critical engine is fresh and the registered eligible-feed coverage threshold passes;
- the current production model package is manually approved;
- point-in-time and anti-contamination assertions pass;
- the evaluation minimum sample size and regime coverage pass;
- probabilities are calibrated and uncertainty intervals are present;
- the independent risk authority returns permission;
- grounding and citation thresholds pass;
- expected transaction costs and liquidity constraints are present;
- no unresolved security, privacy, licensing, or severity-1 data incident exists;
- rollback has been exercised in the current release.

Even after those gates pass, `permission_to_invest` and `trade_entry_ready` remain separate fields.

## Security and operations

- Split the current shared Lambda role into least-privilege control, training, inference, ingestion, and approval roles, following AWS IAM least-privilege guidance ([AWS IAM best practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)).
- Require immutable CloudTrail evidence that trainer and approver are different identities.
- Encrypt private buckets with customer-managed KMS keys, apply retention policies, and deny public access.
- Put quotas and authenticated subject checks at the proxy, not `Origin` checks.
- Treat retrieved text as untrusted input and apply prompt-attack filtering ([AWS prompt-injection filtering](https://docs.aws.amazon.com/bedrock/latest/userguide/prompt-injection.html)).
- Use AgentCore, not Agents Classic, for any later investigate-disagreement workflow ([AWS AgentCore](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/what-is-bedrock-agentcore.html)).
- Respect source terms. FRED restricts automated extraction and third-party series use, and SEC scripted access requires a declared user agent and rate discipline ([FRED terms](https://fred.stlouisfed.org/legal), [SEC access guidance](https://www.sec.gov/os/webmaster-faq)).

## Phased implementation

### Trustworthy core

Land Signal Envelope v1, point-in-time storage, prediction ledger, dedicated IAM roles, Model Registry approval, MLflow tracking, leakage tests, risk gates, and a clean canary. This phase makes the system defensible before adding model power.

### Learn from every engine

Add Chronos-2, the tabular fusion meta-model, finance-specific text features, hybrid retrieval, uncertainty, dissent and family-deduplication logic. Train only on outcomes available at the decision timestamp.

### Institutional autonomy

Add AgentCore investigation workflows, challenger scheduling, conformal intervals, automated reasoning for prohibited claim classes, and independent challenge with block authority. None of these expands execution authority without a separate human-approved mandate.
