# JustHodl AI production infrastructure gates

Status: **review artifacts only — not deployed**

These artifacts define the infrastructure and policy boundary required before
the JustHodl AI engine can be considered for production. They do not call AWS,
modify the existing Lambda, alter the live dashboard bucket policy, migrate
data, or enable event consumers.

## Artifacts

| Artifact | Purpose |
|---|---|
| `aws/lambdas/justhodl-ai/iam/production-gates.template.json` | CloudFormation definition for dedicated roles, KMS, private buckets, EventBridge, Feature Store, Model Registry, Managed MLflow, queues, and the prediction ledger |
| `aws/lambdas/justhodl-ai/iam/lambda-execution-role-trust.json` | Trust policy for the dedicated Lambda role |
| `aws/lambdas/justhodl-ai/iam/lambda-control-policy.json` | Lambda core guardrails, KMS, observability, and approved parameter access |
| `aws/lambdas/justhodl-ai/iam/lambda-training-deployment-policy.json` | SageMaker inventory, tagged training/deployment control, and the single PassRole boundary |
| `aws/lambdas/justhodl-ai/iam/lambda-feature-store-policy.json` | Feature Store online/offline query access |
| `aws/lambdas/justhodl-ai/iam/lambda-runtime-data-policy.json` | Explicit private runtime input/output prefixes and public AI read-model publication |
| `aws/lambdas/justhodl-ai/iam/lambda-brain-signal-policy.json` | Read-only canonical Brain plus governed signal archive/outbox prefixes |
| `aws/lambdas/justhodl-ai/iam/lambda-ledger-archive-policy.json` | Append-only prediction index and immutable archive access |
| `aws/lambdas/justhodl-ai/iam/sagemaker-execution-role-trust.json` | Account-bound SageMaker service trust |
| `aws/lambdas/justhodl-ai/iam/sagemaker-execution-role-inline.json` | Least-privilege SageMaker data-plane policy |
| `aws/lambdas/justhodl-ai/iam/dashboard-brain-deny-policy.json` | Statements to merge into the existing public dashboard bucket policy |
| `aws/lambdas/justhodl-ai/iam/validate_production_gates.py` | Offline static gate validator and deterministic change-plan generator |
| `aws/lambdas/justhodl-ai/iam/test_production_gates.py` | Standard-library regression tests |
| `aws/lambdas/justhodl-ai/consumer-functions.template.json` | Three deployable queue-consumer Lambdas with every event source mapping staged disabled |
| `aws/ops/pending/justhodl_ai_infrastructure_dry_run.py` | Operator-facing wrapper that emits the local plan without AWS access |

## Identity boundary

The template creates:

- `justhodl-ai-lambda-prod`, dedicated to the existing `justhodl-ai` function;
- `justhodl-ai-sagemaker-prod`, assumed only by SageMaker and pinned to the
  target account and region;
- `justhodl-ai-feature-store-prod`, assumed only for governed
  `justhodl-ai-*` feature groups and limited to the Feature Store `offline/`
  prefix, its Glue tables, and the stack KMS key;
- `justhodl-ai-mlflow-prod`, assumed only by the named Managed MLflow
  tracking server and limited to its artifact prefix and KMS key;
- no attachment of `AmazonSageMakerFullAccess`;
- no `sagemaker:*` or global `Action: "*"` grant.

SageMaker creation is limited to `jh-ai-*` resource ARNs and requires both
`justhodl=ai` and `justhodl-ai-managed=true` request tags. Mutation is limited
to matching prefixes with those resource tags. `iam:PassRole` names exactly
the dedicated SageMaker role and requires
`iam:PassedToService=sagemaker.amazonaws.com`.

The Lambda retains account-wide access only for APIs that do not support useful
resource scoping, such as selected list, pricing, Cost Explorer, and CloudWatch
metric reads. Every such action is enumerated; wildcard service actions are
forbidden by the validator.

The Lambda role is not one monolithic permission document. Its CloudFormation
definition attaches six independently validated customer-managed policies for
core guardrails, training/deployment control, Feature Store, runtime data,
Brain/signals, and ledger/archive. Keeping the domains in separate managed
policies also avoids the IAM role's 10,240-character aggregate inline-policy
quota; the validator enforces the ten-attachment role quota, the 6,144-character
per-managed-policy quota, and the remaining inline-policy aggregate quota.
Private runtime reads and writes enumerate the existing datasets, jobs, models,
market-read, control, pricing, and query-result paths; there is no `ai/*`
object grant.

The SageMaker execution role can write experiments and runs only to
`arn:aws:sagemaker:us-east-1:857687956942:mlflow-tracking-server/justhodl-ai-prod`.
It has no MLflow delete actions and no `sagemaker-mlflow:*` grant. The tracking
server's own service role has no SageMaker or Model Registry API permissions
because automatic model registration is disabled.

The CloudFormation stack deliberately does **not** contain an
`AWS::Lambda::Function`. CloudFormation cannot safely adopt the existing
unmanaged function through this review without an explicit resource-import
operation. The generated change plan therefore treats the role/environment
cutover as a separate, reversible, manually approved action.

## Private data boundary

The stack defines the following account- and region-pinned buckets:

- `justhodl-ai-brain-857687956942-us-east-1` — canonical Brain source;
- `justhodl-ai-feature-store-857687956942-us-east-1` — offline point-in-time
  feature history;
- `justhodl-ai-mlflow-857687956942-us-east-1` — Managed MLflow run artifacts;
- `justhodl-ai-prediction-ledger-857687956942-us-east-1` — immutable
  prediction archive.

Each bucket has:

- SSE-KMS default encryption with bucket keys;
- all four S3 public-access-block controls;
- bucket-owner-enforced ownership;
- versioning;
- lifecycle and incomplete-multipart cleanup;
- explicit insecure-transport and cross-account denies;
- `Retain` deletion and replacement policies.

The Brain, Feature Store, and prediction-ledger buckets also enable
S3-to-EventBridge eventing. The MLflow artifact bucket does not, avoiding a
high-volume stream of internal run-file events that has no reviewed consumer.
Its retained current objects are not lifecycle-expired; older noncurrent
versions transition to Glacier Instant Retrieval and expire after seven years.

The prediction archive additionally enables S3 Object Lock at creation and
uses a seven-year **COMPLIANCE** default retention. Its lifecycle does not
expire current objects until ten years.

`dashboard-brain-deny-policy.json` is intentionally separate from the stack.
An `AWS::S3::BucketPolicy` resource would replace the existing unmodeled live
policy. Operators must merge the two deny statements instead:

1. deny all reads of `brain`, `brain/*`, `data/brain.json`,
   `data/brain/*`, and the corresponding Brain-history paths;
2. deny all new writes to those dashboard prefixes.

The Lambda identity policy independently denies reads and writes on those
public Brain paths, even if a broader dashboard allow is later introduced.
SageMaker has no dashboard-bucket access.

On the private canonical Brain bucket, the Lambda can read only
`data/brain.json`. Its only writeable canonical prefix is `signals/v1/*`,
which is used for immutable governed signal envelopes. The mutable delivery
state is isolated to
`s3://justhodl-ai-857687956942/ai/signals/outbox/v1/*`. The canonical Brain
bucket policy adds a second, explicit deny preventing the Lambda role from
putting or deleting any canonical object outside `signals/v1/*`, even if a
broader identity allow is accidentally attached later.

## Event boundary

The template defines:

- custom bus `justhodl-ai-prod`;
- `SignalEnvelope/v1` ingress to a validation queue;
- `Prediction/v2` ingress to a ledger queue;
- SageMaker model-package approval-state changes to a governance queue;
- encrypted queues, retry policies, and a dead-letter queue.

All EventBridge rules are created **disabled**. They must not be enabled until
their consumers, replay/idempotency behavior, alarms, and DLQ runbook have
passed a separate review and canary.

The companion consumer template now wires real handlers for SignalEnvelope
Feature Store materialization, archive-first Prediction/v2 ingestion/grading,
and immutable model-package governance events. The mappings use
`ReportBatchItemFailures`, and handlers retain stable S3 receipts so poison
records reach the queue DLQ without replaying successful siblings. Every
mapping is still `Enabled: false`; this review did not deploy or enable them.

## Feature Store

Two `AWS::SageMaker::FeatureGroup` resources define encrypted online and
offline stores:

- signal features include record ID, event time, availability time, asset,
  value, confidence, source URI, content hash, schema version, lineage,
  quality, and taint;
- prediction features include prediction ID, event/as-of times, asset,
  horizon, side, confidence, model package, feature snapshot, market-data
  hash, verdict version, and risk status.

Both groups use `justhodl-ai-feature-store-prod`, not the combined
training/deployment role. The dedicated role can materialize only below
`offline/*`; it has no canonical Brain, model, job, endpoint, or Model Registry
access. The training/deployment role may read `offline/*` as a training input
but cannot write that prefix or alter the Feature Store Glue catalog.

Consumers must enforce:

```text
feature.available_time <= prediction.as_of_time
```

The infrastructure stores both values but cannot by itself prove the
point-in-time join. The governed feature route now performs a bounded offline
Athena query and independently revalidates entity scope, requested feature
names, `event_time`, `available_time`, deletion state, taint, and result count
before assembly. Supplied observations remain an explicit no-AWS diagnostic
path rather than a substitute for the production offline store.

## Governed signal outbox

Live signal ingestion stages the full clean envelope in the versioned private
AI bucket before writing the compatibility archive or calling EventBridge. The
state object uses conditional S3 writes and the states `PENDING`,
`DELIVERING`, and `PUBLISHED`, with a bounded delivery lease and attempt count.
Submitting the same fingerprint again redrives pending or expired delivery and
is a no-op after a published receipt. This closes the former archive-success /
EventBridge-failure loss window. Delivery remains at-least-once if a process
stops after EventBridge accepts the event but before the receipt is committed;
consumers therefore deduplicate on the stable envelope fingerprint.

## Model Registry and Managed MLflow

The stack defines the retained SageMaker Model Package Group
`justhodl-ai-prod`. Candidate-registration request builders remain
`PendingManualApproval`; none of the automated roles has
`sagemaker:UpdateModelPackage`, so approval remains a separate operator action.
The group itself is retained on stack deletion or replacement.

The stack also defines a retained, `Small` Managed MLflow tracking server named
`justhodl-ai-prod`. It uses
`s3://justhodl-ai-mlflow-857687956942-us-east-1/artifacts/`, a private,
versioned bucket encrypted by `alias/justhodl-ai-prod`. The dedicated tracking
server role can list only that prefix, manage only objects below `artifacts/`,
and use only the stack KMS key through S3 in `us-east-1`. The role, bucket,
bucket policy, and server are all retained together so a stack deletion cannot
leave the retained server without its artifact-store identity.

`AutomaticModelRegistration` is explicitly `false`. This keeps MLflow
experiment logging separate from the Model Registry approval path and prevents
an MLflow-side registration path from bypassing governance. The application
registrar can now be called in controlled live mode, but only through injected
clients and a reviewed token/owner/budget control; it creates a candidate as
`PendingManualApproval` and immediately describes it to verify the persisted
status. Model cards use the same control and must persist as `Draft`. Managed
MLflow publication uses an injected writer and requires receipts tied to the
same run and S3 artifact URI. Approval authority remains separate.

CloudFormation outputs expose the package-group name/ARN, tracking-server
name/ARN, artifact-bucket name, and tracking-server role ARN. The
`RequiredLambdaEnvironment` output includes the repository's complete
stack-managed bindings, including:

- `MODEL_PACKAGE_GROUP`;
- `MLFLOW_TRACKING_SERVER_NAME`;
- `MLFLOW_TRACKING_SERVER_ARN`;
- `MLFLOW_EXPERIMENT_NAME`.

The checked-in `config.json` binds those values to `justhodl-ai-prod` and its
account- and region-pinned ARN. The stack still does not modify the Lambda.

## Prediction ledger

The authoritative prediction record is a unique object under
`predictions/` in the Object-Locked archive. The DynamoDB table
`justhodl-ai-prediction-ledger-prod` is a query index with:

- composite `prediction_id` / `created_at` key;
- on-demand billing;
- SSE-KMS;
- point-in-time recovery;
- deletion protection;
- a `NEW_IMAGE` stream.

The dedicated Lambda ledger policy grants `PutItem`, reads, and queries on the new
table but not `UpdateItem`, `DeleteItem`, or `BatchWriteItem`. Writers must use
a conditional put (`attribute_not_exists`).

`ArchiveFirstPredictionLedger` in `source/prediction_ledger.py` enforces the
ordering rather than relying on callers to remember it:

1. conditionally create the deterministic `predictions/records/...` or
   `predictions/grades/...` archive object with `If-None-Match: *`;
2. on a duplicate key, read and digest-verify the existing immutable object;
3. only after archive confirmation, conditionally create the DynamoDB index
   row;
4. if the archive call fails, propagate the failure without touching
   DynamoDB;
5. if the index call fails, retain the authoritative archive and safely retry
   by verifying it before another conditional index put.

Thus archive failure leaves no partial index state, while a crash between
archive and index is recoverable and idempotent. A mismatched existing archive
fails closed and never creates an index row. Adversarial unit tests cover all
three paths for prediction records and the same archive-first boundary for
grade events. The current legacy `justhodl-signals` permissions are
kept in a separately named compatibility statement and are not the new
production ledger.

### Immutable training inputs and executable canary controls

Training receipts bind their declared train and validation URIs to exact
private-bucket object references containing non-null S3 version IDs and
SHA-256 digests. The Lambda reads those exact versions, verifies the bytes,
copies each object with `If-None-Match: *` to the dedicated
`justhodl-ai-training-inputs-857687956942-us-east-1` bucket at
`sha256/<digest>/<role>/payload.data`, then verifies the destination version,
digest, and exact-one-object prefix. That bucket is versioned, seven-year
COMPLIANCE Object Locked, deletion-denied, and policy-restricted so only the
Lambda execution role can create the two exact payload shapes and only with
the write-once conditional header. SageMaker has read-only access to the exact
payload patterns and receives only those non-expandable, unique prefixes.

The canary HTTP handler constructs SageMaker, SageMaker Runtime, and CloudWatch
clients only for an explicitly approved live request. Its concrete callbacks
verify the Approved model-package and live endpoint owner tags, deploy and
wait for green, send bounded paired requests to blue and green until the
configured minimum sample count is reached, and compute prediction match and
drift from the paired responses. Promotion additionally waits, with a timeout,
for fresh CloudWatch invocation/error/latency evidence generated by that
traffic. Promotion or rollback deletes the separately billed green endpoint;
rollback deletes its config, while promotion ownership-checks and retires the
now-active config. Cleanup records terminated hourly cost and a digest, and a
cleanup failure is fail closed. Approval token, exact owner, API-call ceiling,
and estimated-cost ceiling remain enforced by `ControlledExecution`.

## Review and cutover order

The offline plan uses the following order:

1. Validate JSON, CloudFormation shape, IAM invariants, and tests.
2. Create and review a CloudFormation change set; do not execute it as part of
   this repository task.
3. Copy the canonical Brain object to
   `s3://justhodl-ai-brain-857687956942-us-east-1/data/brain.json` (the key the
   existing dataset builder expects) and verify checksum, SSE-KMS, and
   non-public access.
4. Update the existing Lambda to the dedicated role and merge these environment
   values while preserving all others:
   - `AI_BRAIN_SOURCE_BUCKET`
   - `SIGNAL_FEATURE_GROUP`
   - `PREDICTION_FEATURE_GROUP`
   - `PREDICTION_LEDGER_TABLE`
   - `PREDICTION_LEDGER_ARCHIVE_BUCKET`
   - `AI_EVENT_BUS`
   - `MODEL_PACKAGE_GROUP`
   - `MLFLOW_TRACKING_SERVER_NAME`
   - `MLFLOW_TRACKING_SERVER_ARN`
   - `MLFLOW_EXPERIMENT_NAME`
   - `SAGEMAKER_ROLE_ARN`
5. Verify that a training job can create and update an MLflow run only on the
   named server, that its artifacts are SSE-KMS encrypted in the private
   artifact bucket, and that it cannot delete runs.
6. Register a candidate as `PendingManualApproval`; verify that automated
   identities cannot call `UpdateModelPackage`.
7. Run negative IAM tests and an owner dataset-build canary.
8. Verify rollback to the previous role/environment.
9. Merge, rather than replace, the dashboard Brain deny statements only after
   the private-read canary passes.
10. Remove the now-unreadable public Brain source.
11. Enable event rules only after reviewed consumers and alarms exist.

The checked-in `aws/lambdas/justhodl-ai/config.json` records the intended role
and environment values for a later deployment workflow. Editing that repository
configuration does not mutate AWS. The dry-run plan reports whether each
binding matches the reviewed resources and always retains the live-diff
blocker.

## Offline verification

Run from the repository root:

```bash
python aws/lambdas/justhodl-ai/iam/validate_production_gates.py
python -m unittest aws/lambdas/justhodl-ai/iam/test_production_gates.py -v
python aws/ops/pending/justhodl_ai_infrastructure_dry_run.py
```

If `cfn-lint` is installed, also run:

```bash
cfn-lint aws/lambdas/justhodl-ai/iam/production-gates.template.json
```

The first three commands use only local files and the Python standard library.
The dry run reports `aws_api_calls: 0` and must continue to flag
`NO_LIVE_AWS_DIFF`; a later operator must inspect an actual CloudFormation
change set before any execution.
