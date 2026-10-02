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

The suite includes 72 healthy/legacy/bootstrap handler differentials, 166
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

Rollback is a reviewed commit restoring the predecessor checkpoint admission
and removing its unused validator/import. Retain the original fixtures and
repair/release evidence. In the same commit, adapt the offline runner's source
scope and rejected-admission expectations to the deliberately restored
predecessor behavior; tests must not keep expecting the removed repair. Rerun
complete healthy/legacy and tail-replay/correction differentials and reproduce
the invented-data failure to record that the vulnerability returns. Require
exact-head independent review, every release gate and Actions package/receipt/
control verification. Preserve every stored object, checkpoint, schedule and
control; no reset, deletion, data correction or producer invocation is part of
rollback. No rollback has been performed.

## Explicit unresolved risks

Genuine NoSuchKey still bootstraps even if the namespace already contains
pages. A missing checkpoint in a previously populated namespace is **not
fixed**. The suite demonstrates that compatibility deliberately; no namespace
scan, bootstrap flag, new object/state schema or extra recurring call is added.
Storage-write errors, checkpoint transactions/reconciliation, page collision
guards, missing-page repairs, ECB slicing, Tier1 indexing, lookup optimization
and other known defects remain separate. This repair neither establishes
historical integrity nor restores already overwritten progress/data.

## Reviewed draft and release qualification

[Producer draft PR #75](https://github.com/ElMooro/si/pull/75) changes only
checkpoint admission, its byte-pinned predecessor/test runner and supporting
evidence. Independent review accepted source head
`5aef1abf8acda0993602afebdc225288eefae310`, handler SHA-256
`bcd80ce358aa433d9de33c45b9fb2900987c63046d343462b3c359b7c3724867`.
The final draft head and its review are recorded in the PR. The reviewer reran
72 healthy/legacy comparisons, 166 rejection/recovery cases, 10 replay/corruption
scenarios and four actual consumer cases, and independently challenged SDK
exception subclasses, legacy representations and same-store recovery.

[Probe PR #76](https://github.com/ElMooro/si/pull/76) received independent exact-head
review at `054a64052bad2d12495fae5074d99c07f49464ba`; all 18 dependency-free probe
tests passed. Its merged source is byte-identical. Read-only
[baseline run 36976934336](https://github.com/ElMooro/si/actions/runs/36976934336)
succeeded at `912cdcf97eb0cc33703aff3ded79ec69ee87ccc7` with 14 allowlisted AWS
reads, one signed package GET, zero AWS writes and zero producer invokes.

`series-admission-baseline.json` is the exact sanitized probe result. The live
predecessor handler matches the fixture; ZIP bytes=111270 and CodeSha256=
`Gq4ZfrkuxI3Oak3NxaR3HOsySdn87EL64epCdgy9bVY=`. The previous release has no
receipt (exact typed NoSuchKey); a new commit-bound receipt remains mandatory.
The independently recomputed operating fingerprint is
`ae119eb01335b5d1dcec8897bce47ad66e79c3f8579301bf9ef8c80a9c47b199`.
Concurrency=1, timeout=900, memory=10240, temporary storage=512 MB, Active tracing
and standard DLQ already match the existing deployment projection. The extractor
rule is enabled at **rate(1 hour)** with four targets; retain that actual cadence.
The five intentional monitor bindings are enabled. Payload/environment/settings
are compared as digests; arbitrary additional bindings and runtime failure paths
are outside this probe's qualification.

**No producer merge or deployment has occurred.** The required deployment suite
passed 1,041 static tests plus 15 shell gates on the original candidate base,
then correctly held the draft when newer main failed the unrelated Ticker360
preservation boundary. The original transition at `bfd28d614` pinned shared
source `815435a5384d860dc19bb32a2b9b83d8acd72d70b5969623aa5ce5d029615387`
and reverses eleven exact guarded-row/source-clock changes to the prior full
source bytes. This is a deliberate review boundary, not an ordinary snapshot.

Owner ops commit `c4f56e8cb` added eight producer-list aliases (`big_buys`,
`big_sells`, `clusters`, `upcoming_14d`, `upcoming`, `earnings`, `positions`,
`holdings`) after that boundary. They extend matching-row lookup and universe
enumeration while retaining old key precedence; guarded-row/fallback and clock
logic remained byte-identical. Commits `cc5129fda`, `4765db130` and `cfbc9967c`
subsequently changed only the addition's comment. The final shared source has
SHA-256 `34cd1f717e0bf5e7aa2e58598ce735d7ff1d256280175b1901d62c5f7448b39c`.
The ticker producer's separate pinned source still matched throughout. Clean
main `a69a5d523` reproduced one failure out of 23 focused cases, and owner
[run 36977445049](https://github.com/ElMooro/si/actions/runs/36977445049)
failed preflight at that exact check. This admission repair changed none of
those source, test or fixture paths and bypassed no gate.

Owner correction `13e5e1e9813b5d34d7fec319275474d33e7e28aa` now adds a separate
`peer-list-keys.json` exact inverse comparison: verify current full source hash,
reverse only the eight-alias delta once, verify the reconstructed original
transition hash, then apply every original guarded-transition assertion.
Two new cases exercise all eight aliases and prove context-withheld rows stay
withheld. The original boundary remains enforced. Focused clean-main checks
pass 25 cases. Owner evidence explicitly leaves the alias delta's native
delivery unverified; a passing static gate does not qualify that other lane's
production behavior. Ownership remains with `S-codex#symdir1001a`; this patch
contains no other-lane repair. Independent review accepted this exact owner correction after rerunning all
25 focused tests and reconstructing both full source boundaries. The first owner-corrected
admission candidate passed all 1,043 required deployment static tests and 15
shell gates, its complete 72/166/10/four differential suite, all 18 probe tests,
selected source/config validators, source compilation, secrets/stub checks,
15 public-boundary checks and engine wiring. The source, fixture, runner and
committed baseline are unchanged from the earlier independently approved draft.

Read-only [refresh run 36979510081](https://github.com/ElMooro/si/actions/runs/36979510081)
at exact owner-corrected main `13e5e1e9813b5d34d7fec319275474d33e7e28aa`
succeeded. Its complete sanitized result is identical to the retained baseline:
predecessor package/handler, operating controls and fingerprint all match;
14 AWS reads, one signed package GET, zero AWS writes/invokes. Report commit
`599fa623c04b7a2137380ea8155f91f87363ce77` changes only that read-only report;
no tested code or deployment dependency changed on the final rebase. Refresh
again if controls or deployment dependencies change before release. The owner subsequently landed reader commit
`df5ad4aefb4af8bac6a310a373ee63da8b316e38`. Its independent integration review
passed 25 guard and 19 reader cases and verified a second exact inverse: current
reader source to the full retained eight-alias source, then the peer inverse
and every original guarded-transition assertion. Unexpected source mutations
still reject. The extractor imports no bundled shared modules and has no dynamic
import/eval/exec path; the changed Ticker reader is unused by this producer.
The owner's normal release selected only Ticker360. No production contract from
that other lane is added by this admission patch.

After rebase onto that reader commit, all **1,044 required deployment static
tests and 15 shell gates** pass, along with all 72/166/10/four extractor cases,
18 probe tests, selected source/config validators, secrets/stub, 15 public-boundary
checks and wiring. Subsequent main commits through
`62cc80106027d3ef80d524adb9821ef4ba1cf4f9` add owner technical probes, their
reports and supporting evidence only; selected source/tests/deployment scripts
and operating controls remain unchanged. A further nonoverlapping owner Ranker release at
`5b91df2b3c6f57c490caf4f1cf6ecfa711c7fdce` adds two required boundary tests.
The complete suite was rerun on this integration: **1,046 deployment static
tests and 15 shell gates pass**. Independent review again reran all extractor
and probe cases and selected validators, with unchanged source/tests/baseline
and no extractor findings. The final secrets scan covers 15,761 files with no
findings. No other-owner production contract is qualified by this repair.
Main `a5aa60de960c6931f5845dca3cac9f5ff8221555` adds only that owner's read-only
acceptance report, leaving every tested input and deployment dependency unchanged.
Exact-head release review, producer deployment and post-release package/receipt/
control proof remain pending. Natural publication cannot prove the failure path
executed.
