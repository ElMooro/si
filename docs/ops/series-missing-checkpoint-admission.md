# Series-extractor missing-checkpoint admission draft

This draft changes only admission after the checkpoint GET raises a typed
botocore ClientError with exact code `NoSuchKey`. Installed PR75 at
`e5a4c11cd44fc103ae64a0ca43a7161c31ec7808` remains the predecessor. Its separate
strict RuntimeVersionConfig acceptance is still **HOLD**: this draft changes
neither that probe, criterion nor `series-admission-baseline.json` (SHA-256
`4f4937b34ce424e80adfde5c674ee88ae10652db6e96eb6e4a6f087f4dad0e09`). No release
is authorized. Historical PR75 evidence remains in
[series-checkpoint-admission.md](series-checkpoint-admission.md).

Current main was initially revalidated at `6fe20c0484d8a2e08359a0df9558838c59420c0e`:
producer bytes still equal the installed PR75 handler SHA-256
`bcd80ce358aa433d9de33c45b9fb2900987c63046d343462b3c359b7c3724867`.
DEPLOY_LANE.md, AUTONOMY.md, STATE.md and SESSION_CLAIMS.md were read; no
AGENTS.md or local .agents/skills instructions were available. Prior standalone
work is not acceptance evidence. Claim `S-codex#semissing1002x4` is draft-only;
no overlapping extractor source claim was active. Actual provider-catalog
`_series_list` and SymDir `_page_rows`/`_page_row` were inspected and exercised.
The final draft integration includes current main `1b4c0a4ec7e57a077107ac0d3d4acb0e890a7f52`. Its additional
Compound work is retained without edits; it changes no extractor/consumer
source, deployment workflow or PR75 acceptance baseline. Final-head gates and
independent review are recorded in the draft PR description.

## Admission contract

Before returning the unchanged bootstrap state, the helper sequentially requests:

| Order | Prefix | Delimiter | MaxKeys |
|---|---|---|---|
| 1 | `data/providers/{provider}/series/` | `/` | 1 |
| 2 | `data/providers/{provider}/series-manifest.json` | `/` | 1 |

Every new admission LIST has Delimiter. No continuation, StartAfter, paginator,
parent/provider-wide prefix or archive/version scan is used. The user clarified
that this requirement applies to both new admission LISTs in this scoped draft.
Pre-existing discovery and legacy counter-seeding listings omit Delimiter and
retain their exact request shapes to avoid a separate behavior change. Their
existing delimiter-rule discrepancy is explicitly out of scope; this is neither
a waiver of that rule nor a claim that the entire engine conforms. Application
code compares SDK-returned keys literally, without URL decoding or
filesystem-path normalization.

Each response must be a known dictionary shape, with matching bucket, prefix,
delimiter and integer MaxKeys=1; HTTP status integer 200; IsTruncated exactly
False; integer KeyCount between zero and one; and list-shaped object/group
collections if present. Absent collections are allowed only with this affirmative
complete response and consistent count. Unknown top-level/object fields,
continuation/StartAfter fields, groups, malformed admission fields, errors,
timeouts and denied requests refuse initialization. Known ancillary object
metadata is not used to establish namespace emptiness.

The exact zero-byte `series/` root marker can be the sole complete ungrouped
result. A nonzero root marker, every other direct object (including zero-byte
or high-index pages), and every nested marker/group refuse. At the manifest
prefix, the exact key refuses at any size; a distinct direct suffix neighbor is
allowed only when it is the sole complete ungrouped result. A child name/group,
or a cap that hides further results, refuses. This intentionally sacrifices
availability for conservative bounded evidence.

[AWS's ListObjectsV2 API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
describes delimiter groups as one result against MaxKeys. Its example with one
direct object and one group has KeyCount=2; its groups-only example has
KeyCount=2 without Contents. Accordingly, groups are rejected *before* the
ungrouped count is compared to Contents length. Empty Contents alone never
establishes emptiness. [The official SDK output model](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/list_objects_v2.html)
represents KeyCount as an integer and CommonPrefixes separately. Offline
transport tests exercise both using the installed official botocore parser.
Botocore's [existing transport handler](https://github.com/boto/botocore/blob/develop/botocore/handlers.py)
automatically requests URL encoding and decodes once. Admission accepts its
returned `EncodingType=url` and performs no additional decode; a literal `%2F`
suffix remains distinct from a slash child.

Refusal is the fixed RuntimeError `missing checkpoint namespace admission
refused`, with exception chaining suppressed and no key/provider/AWS payload
logging. It occurs before discovery, engine-index reads, parsing, writes or
publication. An ordinary scheduled retry can recover after a transient listing
failure clears in an otherwise empty namespace. A populated namespace keeps
refusing without inventing progress.

## Deliberate recovery change and limits

A first initialization whose page landed but whose first checkpoint never
committed now refuses on retry, just like an initialized namespace whose
checkpoint was lost. Current-object listings cannot distinguish those cases.
The previous handler could overwrite page zero in both; this change does not
reconstruct progress or repair retained data. A lost checkpoint PUT response
*after server commit* remains recoverable from the valid checkpoint, with the
same replay behavior and bytes as PR75. A valid earlier checkpoint with an
uncommitted tail continues its normal rewrite/correction behavior.

Two reads are observations, not a transaction. An external writer can create a
page or manifest after an observation; reserved concurrency=1 does not lock
external writers. The suite demonstrates a racing writer's page can still be
overwritten by the unchanged writer. Complete empty current listings also omit
noncurrent versions and deleted historical objects; a separate fake-history
case demonstrates admission despite that hidden history. No historical absence
or namespace integrity guarantee is made, and no archive/version reads are
performed.

The new manifest-prefix ListBucket permission is **unproven** in production.
Denial refuses initialization; no IAM expansion or broader-prefix fallback is
included. There is no create-only PUT, bootstrap event flag, new state field,
journal/schema change, reconciliation, schedule/concurrency/retention/storage
change, invoke, deletion, security-setting change or paid service activation.

## Call budget and evidence

Valid-checkpoint runs add **zero** LISTs or other API calls. A missing-checkpoint
admission attempt adds at most **two logical LISTs**, stopping after the first
refusal. A blocked invocation performs only the checkpoint GET and one or two
LISTs. Once admitted, ordinary discovery and legacy counter seeding continue
unchanged; the bound is for admission, not the entire successful handler. SDK
adaptive retries (`max_attempts=5`) remain unchanged, so logical calls can cause
multiple HTTP attempts. Repeated blocked invocations repeat one or two logical
LISTs each; no cross-invocation cache or permanent failure flag is introduced.
After a valid checkpoint is committed, the additional checks disappear.

The retained `pr75-installed.py.txt` is byte-pinned. Removing the new helper and
replacing its one call with the installed comment reconstructs that entire
source byte-for-byte. The original PR75 predecessor fixture and original AST
scope assertions also remain enforced. All ordinary handler comparisons include
return/error behavior, full ordered calls/options, output object bytes, state,
provenance, stdout and time-budget checks. Missing-bootstrap comparisons remove
only the two asserted admission calls from the trace. Actual consumers run on
both valid-resume and first-initialization output. Synthetic executors do not
simulate asynchronous completion ordering.

Run:

```sh
python3 aws/lambdas/justhodl-series-extractor/tests/run_tests.py
python3 aws/lambdas/justhodl-series-extractor/tests/test_sdk_listing_contract.py
python3 tests/deployment/run_tests.py
```

The first runner is dependency-free; when the deploy workflow's boto3 dependency
is installed, it also runs the SDK contract tests. All input data, objects,
checkpoint failures and HTTP responses are invented. No AWS is accessed. The
required deployment suite and selected source/config/preflight, compilation,
secrets/stub, public-boundary and wiring gates must pass before publishing the
draft. Independent review must bind its result to the exact final head and
challenge the partial-bootstrap refusal contract, listing completeness and
ordinary byte equivalence.

## Required decision before any release

This is an independently reviewable draft, not a release. Before release,
explicitly accept the changed partial-bootstrap recovery contract and resolve
production permission qualification within an authorized read-only plan without
expanding IAM. Preserve the separate PR75 RuntimeVersionConfig HOLD and its
strict acceptance gate; do not replace its baseline or waive that criterion.
Require current-main integration gates, independent exact-head review and all
existing deploy-lane baseline/package/receipt/control acceptance. No merge,
deploy, runner dispatch, producer invoke, archive read or stored-data operation
is part of this task.

A rollback would be a separately reviewed source-only commit removing the new
helper/call, adapting these tests, and retaining both fixtures and evidence.
It would restore the demonstrated populated-namespace bootstrap vulnerability;
it cannot recover lost progress or historical data and does not resolve PR75's
runtime acceptance hold. No rollback has been performed.
