# Series-extractor checkpoint admission

This incremental repair admits extraction only after a checkpoint GET, body
read and JSON decode succeed, or the GET raises a typed botocore `ClientError`
with exact S3 error code `NoSuchKey`. AccessDenied, timeout, InternalError,
NoSuchBucket, generic HTTP 404 and all other retrieval/body/JSON failures raise
a fixed RuntimeError before engine-index reads, discovery, writes or manifest
publication. Exception chaining is suppressed; checkpoint content, provider
keys and AWS error messages do not enter rejection logs. The SDK's existing
bounded retry policy is unchanged; a normal scheduled retry resumes once the
valid checkpoint is readable.

The exact full predecessor fixture has SHA-256
`9c82d0046498e7de75d5f24a8346047e0a59635972d438e65d1613331250a312`.
No AGENTS.md or local `.agents/skills` instructions were available. Current
DEPLOY_LANE.md, session claims, current producer, original checkpoint writers,
and actual provider-catalog/SymDir consumers were revalidated. Held lookup claim
`S-codex#selookup1002h9` was released by documentation commit
`5eccfcd995e160a550edf704b2a888ad67490123`; this patch does not include that
optimization. Ops 6448 and 6449 belong to SymDir. The admission claim is
`S-codex#seadmit1002r8`.

## Counter compatibility

The existing page formatter uses `:04d`, requiring a nonnegative integer
`n_pages`; bool and integral floats cannot be admitted as page indices.
`series_count` retains nonnegative integers and finite integral floats without
coercion. Optional `pages_objects` and `pages_bytes` also preserve integral
floats and existing int-compatible integer strings (whitespace, plus sign and
leading zero included). Bool, negative, fractional and nonfinite numeric
values are rejected. Missing required allocation fields cannot initialize zero.
Optional missing, None and empty-string totals retain the old idle manifest
defaults. Their existing append behavior is not repaired in this change.
No page/series/object counter relationship is imposed: legacy buffers, hashes,
recorded errors and an uncommitted tail make such assumptions unsafe.

Healthy state and output representations remain byte-identical. The source AST
is identical outside the added validator/import and replaced checkpoint
admission block. Parsers, flow progress, buffers, allocation and PUT options,
hash pruning, tail replay, write-error handling, updates/corrections, Tier1 and
consumer contracts remain unchanged.

## Offline evidence

Run `python3 aws/lambdas/justhodl-series-extractor/tests/run_tests.py`.
The dependency-free test runner uses invented warm files/checkpoints/pages,
fake AWS modules and deterministic executor completion. It compares complete
handler returns, all object bytes, complete ordered read/LIST/PUT traces and
options, clock-budget checks, stdout and error type/message against the full
predecessor. Real asynchronous completion races are not simulated.

The suite includes 66 healthy/legacy/bootstrap handler differentials, 166
rejected/recovery cases, 10 replay/corruption scenarios and four actual
provider-catalog/SymDir consumer cases. Eurostat and grouped ECB are covered,
with list pagination, empty/partial/exact-page/multi-page input, legacy missing
fields, buffered rows, progress, budget stop, normal writes and existing write
failure behavior. Body-raised NoSuchKey and an untyped exception spoofing that
code both reject. Rejected admission asserts one checkpoint GET only, no
discovery, zero writes/publication, unchanged objects and sanitized traceback.
The same fake store survives rejection and then allocates from the readable
checkpoint's original page index on the next invocation.

The synthetic corruption reproduction intentionally loses the old checkpoint
GET response while pages and checkpoint still exist. The old handler publishes
success after replacing page 0 and resetting progress. Reading the newly
replaced checkpoint on retry cannot restore the overwritten page. This is an
invented-data reproduction, not evidence of live corruption. Checkpoint PUT
response loss, an uncommitted page tail, identical-hash replay and changed-input
tail correction are separately compared with the predecessor.

## Release conditions and rollback

The existing deploy lane reapplies configured memory=10240, timeout=900,
description and declared environment overrides, and forces X-Ray Active and
the standard DLQ after publishing the code receipt. `eventbridge_rules` is
legacy metadata; the current config has no managed schedule-write block.
A narrow source change therefore still requires a reviewed Actions-only
technical probe proving those settings already match the live predecessor,
reserved concurrency remains 1, actual temporary storage is preserved, and
projected existing schedule controls (including the five intentional monitor
bindings) are unchanged. Never retime a rule based on its name.

Before release require independent exact-head review, all repository release
gates, and the read-only predecessor/control baseline. After release require
exact live package/CodeSha256, receipt bound to the released commit and handler,
Active/Successful readiness and unchanged projected controls. A green workflow
or a code receipt written before config reconciliation is insufficient.
Natural manifest publication may be observed; it cannot establish that the
rejected failure path executed or that all retained data is intact.

Rollback is a reviewed commit restoring only the predecessor checkpoint
admission and removing its unused validator/import, through the same Actions
lane with package/receipt/control verification. Preserve every stored object,
checkpoint, schedule and control. No reset, deletion or producer invocation is
part of rollback. Rolling back also restores the old admission vulnerability.

## Explicit unresolved risks

Genuine NoSuchKey still bootstraps even if the namespace already contains
pages. A missing checkpoint in a previously populated namespace is **not
fixed**. The suite demonstrates that compatibility deliberately; no namespace
scan, bootstrap flag, new object/state schema or extra recurring call is added.
Storage-write errors, checkpoint transactions/reconciliation, page collision
guards, missing-page repairs, ECB slicing, Tier1 indexing, lookup optimization
and other known defects remain separate. This repair neither establishes
historical integrity nor restores already overwritten progress/data.
