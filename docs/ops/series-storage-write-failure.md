# Series-extractor storage-write failure draft

This is an isolated DRAFT repair of extraction page/checkpoint write-failure
handling. It was first based on main `39f7b521fff3a1a43509de23590d7d8be649e82c`
and is now rebased on owner main `86f8aeb755e301b55ea2a253e7237e3fadc8ba75`.
**Release HOLD.** The owner correction passes the unchanged compound gate and
all required tests. Independent review qualifies its three actual candidate
mappings; two broader detector limitations are retained for active-owner follow-up
and do not materially block this isolated storage draft. Parent coordination
confirms PR78 is a completed draft with no live editing turn and authorizes this
separate storage claim/branch. Draft publication requires final exact-head review;
the draft PR records that verdict and exact head. No merge or deployment is authorized.

No merge, deployment, runner dispatch, real AWS/data-provider access, producer
invocation or retained-data operation is authorized or performed by this task.
Offline calls to the handler use invented objects and fake SDK modules only.

PR75 remains installed, with its strict runtime-version acceptance HOLD. Its
checkpoint admission, full predecessor fixture, allocation validation, missing-key
bootstrap, acceptance probe, control baseline and release criterion are unchanged.
PR78 remains a separate unmerged draft awaiting its user decision. This repair
imports none of its namespace admission logic and adds no bootstrap observations.

## Confirmed failure and bounded repair

The installed PR75 source is preserved byte-for-byte in
`aws/lambdas/justhodl-series-extractor/tests/fixtures/storage-predecessor.py.txt`,
SHA256 `bcd80ce358aa433d9de33c45b9fb2900987c63046d343462b3c359b7c3724867`.
The original pre-PR75 fixture remains unchanged, SHA256
`9c82d0046498e7de75d5f24a8346047e0a59635972d438e65d1613331250a312`.
Full-handler invented fixtures reproduce the same storage defect on both sources:
page PUT failures become `missing_pages`, but the flow becomes done and the new
checkpoint and manifest publish success. An unchanged next invocation skips the
flow instead of repairing the missing page. A one-shot checkpoint PUT failure
inside either provider's broad per-flow catch becomes a parsing error; a later
checkpoint/manifest can publish success. Repeated uncertainty can contaminate
parser retry/retirement state. Before editing, the primary and an independent
reviewer separately reproduced these paths using the installed handler.

The repair introduces one distinct `StorageWriteError` type for failed or
uncertain page/checkpoint work. A collection examines every selected result,
retains still-pending futures, then refuses if any writer failed. The failure
remains sticky for the invocation. Submission rejection is also fatal because
allocated work cannot safely be claimed as durable. Both per-flow generic catches
explicitly rethrow the storage error before normal parser retry/retirement logic.
Checkpoint PUT failures, including the final idle checkpoint, raise the distinct
error. Serialization remains outside the new PUT catch.

A `finally` block harvests every outstanding writer result and waits for executor
shutdown. It performs no checkpoint or manifest write. It can surface a late
writer failure while unwinding another exception; the storage failure has priority
and the original exception context is suppressed. Failure messages are fixed and
include no object key, body, SDK message or exception chain. Normal parser-error
records retain their original format, retries and retirement behavior.

No newer checkpoint is attempted after an observed failed page result. A failed
checkpoint is not followed by another checkpoint or a new manifest. There is no
successful handler return after either failure. Successful siblings may already
have written their pages; their in-memory counters/hashes are discarded when the
invocation exits. The handler does not undo any accepted page or checkpoint,
rewrite the previous checkpoint, or claim an uncertain PUT was definitely absent.

The collector's existing broad catch also includes counter/hash bookkeeping after
a successful PUT. For example, PR75 admits an optional `pages_objects=None` idle
state, but an append cannot convert that value to int. The old handler mislabeled
this as a missing page and published success. The new path refuses conservatively
with the same storage/accounting error. This is an additional blocked false-success
case; it is not evidence that the acknowledged PUT failed. Admission and legacy
idle defaults are unchanged. Stored legacy errors/missing-page records are preserved;
this repair does not reconcile them.

## Recovery from actual durable state

On a page precommit failure, successful siblings may exist beyond the unchanged
checkpoint. On an accepted page PUT with a lost response, that page may exist too.
A normal later scheduled invocation reads the actual checkpoint and repeats the
ordinary uncommitted-tail allocation and replacement behavior. Identical recorded
hashes still suppress identical writes; changed input still corrects replayable
tail bytes. Pages are not made create-only. No tail reconstruction is introduced.

If checkpoint PUT fails before committing, the last earlier committed checkpoint
remains byte-identical and recovery uses its original buffer/progress/page indices.
If checkpoint PUT commits but its response is lost, the durable object is newer;
recovery reads that actual state. The failing invocation cannot know which outcome
occurred, and does not write a guessed rollback. Once a flow is done in that durable
checkpoint, the ordinary subsequent invocation skips it. A changed warm input for
that completed flow remains undetected, as it did before this repair.

More than one checkpoint, carried buffers spanning flows, progress budget stops,
partial ECB slice progress, repeated ordinary invocation and warm-input changes
between attempts are exercised in full-handler tests. Recovery is compared against
running the installed full handler on the exact durable post-failure object store,
not against a guessed absent page or pre-invocation snapshot. Existing completed
pages are preserved; only the ordinary replayable tail may be replaced.

## Offline validation

Run:

```sh
python3 aws/lambdas/justhodl-series-extractor/tests/run_tests.py
```

The runner preserves the original PR75 admission assertions and predecessor pin.
Its scope check reverses only the reviewed storage exception/cleanup footprint,
then enforces PR75's original complete AST scope proof. Four original page-failure
comparisons and the original lost-checkpoint-response comparison now run against
the retained installed baseline because candidate failure behavior intentionally
changes. They are not claimed as candidate-equivalent healthy runs.

Current full-handler coverage:

- 68 original healthy/legacy comparisons, four retained original page-failure
  comparisons, 166 admission rejection/recovery cases, ten original replay/
  corruption cases and four actual SymDir/provider-catalog consumer assertions.
- Eight explicit baseline false-success reproductions on both full predecessor
  sources; 48 additional healthy comparisons including real local executor use.
- 56 storage-fault/recovery scenarios: page precommit and accepted/lost response,
  mixed parallel page outcomes, checkpoint precommit and accepted/lost response,
  first/middle/final checkpoint, buffers, budget resume, changed warm input and
  repeated invocation. Every failed candidate forbids new manifest publication.
- 12 worker-drain/submission scenarios, including nonblocking collection, late
  completion after another writer fails and real local worker synchronization.
- Eight ordinary parser-error cases and 12 exception-boundary cases: parser error
  after yielding a page, storage failure inside parser-error handling, stall
  checkpoint failure and idle final-checkpoint failure.
- Ten explicit remaining-risk/bookkeeping scenarios, including both manifest PUT
  outcomes, completed-flow input changes and unchanged missing-key bootstrap.

Required checks also include the repository deployment static/shell suites,
selected source/config validators, engine preflight/compilation, stub guard,
secrets scan, brain public-boundary suite, wiring and unchanged PR75 acceptance
probe tests. Local evidence and the final task response record the exact-head
results and independent publication disposition.
All engineering fixture execution is offline. The local test venv installs only
the existing deployment-test dependencies; it does not change repository or runtime
settings. An external socket guard is used for required Python suites. No tests
read retained archives or live provider objects.

Independent review separately generated its own baseline, failure, exception-path,
real-worker and recovery fixtures rather than merely rerunning the author's suite.
It challenges each changed exception boundary and the actual-durable-checkpoint
semantics. Draft publication requires its final exact-head verdict; release stays
held regardless of a draft-publication approval.

## Owner gate correction and integration qualification

The initial ordinary required command
`DEPLOY_TARGETS=justhodl-series-extractor python3 tests/deployment/run_tests.py`
failed on main `39f7b521f` at
`test_release_verifier.test_explicit_primary_outputs_are_source_bound_and_do_not_select_control_state`
in `tests/deployment/test_release_verifier.py:38`:
`AssertionError: ('justhodl-compound-aggregator', {'data/prime-convergence.json'})`.
Both primary and independent reviewer reproduced it on clean main. The complete
initial diagnostic reported 1,058 of 1,059 static tests passing plus all 15 shell
gates; that was correctly reported as a failed required suite and publication
was held. No check was bypassed or edited by this repair.

Active owner `S-codex#symdir1001a` landed correction
`86f8aeb755e301b55ea2a253e7237e3fadc8ba75`: conservative returned-write-bundle
candidate tracing restores Compound's three existing literal output candidates.
The original release test and PRIMARY declarations remain byte-identical. Each
current restored candidate matches the unchanged actual Compound source, with
`entrypoint_reachability=unproven` and `runtime_verified=false`. These three
mappings are independently qualified for this integration; this is not runtime
ownership or publication certification. The extractor imports none of the owner's
new helper/logger modules. No owner source, gate, baseline or claim is modified
by this storage branch.

After rebase, the ordinary complete required suite passes **1,071** static tests
and all **15** shell gates. Selected source/config validation, engine preflight/
compilation, stub guard, all **15** brain public-boundary tests, wiring (**36 pages /
143 wired**, no missing or stale entry), unchanged PR75 acceptance probe (**21**
tests) and complete engine/consumer suites pass. Final exact-head secrets and
staged inventory are rerun after final documentation/claim updates.

Independent review also challenges the new owner detector with two invented
full-handler cases: a producer's local KEY shadows its module KEY, and a consumer
changes the loop item's Key before PUT. The general detector currently retains
an unused original key in those cases. Its broader advertised conservative
handling is not qualified by this repair; the actual Compound has neither pattern.
Evidence `/tmp/series-storage-review/owner-correction-review.json` is retained
for parent handoff to the active owner. This branch does not fix or rebaseline it.
The independent reviewer explicitly treats these as separate known main risks,
not storage-draft blockers; only the three actual current mappings are qualified.
Draft publication awaits final independent exact-head integration review. Broader
owner workstreams and their separate runtime acceptance remain open.

Supporting local evidence:

- `/tmp/series-deployment-tests.log` and `/tmp/series-main-deployment-tests.log`:
  original failed gate on candidate and untouched main39f7.
- `/tmp/series-deployment-diagnostic.json`: complete initial diagnostic, not a
  replacement for the failed ordinary gate.
- `/tmp/series-integrated-deployment-tests.log`: complete passing ordinary suite
  after owner correction and rebase.
- `/tmp/series-storage-review/independent-baseline.json`,
  `independent-repair.json` and `independent-exception-paths.json`: separately
  generated full-handler recovery and exception cases.

## Local combination with held PR78

PR78 head `de486e8e16b80f3ff2997edf3c4ec0f9d8ff40c6` has handler SHA256
`3c0aeb7a3ea3454a9296c8d832044376c2283bac0b3885f21773c5ea7dfb2cfb`.
The independent storage handler remains SHA256
`d50f2c942d72a97af63cb7a581d10c4c8844a3aab7a3d7a64ccd5d319215f034`.
A detached local combination has SHA256
`2e5fd09a4dac47275812b8e8955916af6f913395a8a9918933d1dfe362cccf33`.
No combined source is published or added to this branch, and neither PR is edited.

The patches share the source file. A mechanical add/add conflict occurs where both
insert declarations immediately before `lambda_handler`. Local resolution keeps
PR78's complete namespace helper verbatim followed by the distinct StorageWriteError
class. PR78's actual admission helper and checkpoint-admission block are unchanged.
Reversing only the storage AST footprint reconstructs PR78's complete AST; PR78's
own exact installed-PR75 byte inverse and original predecessor scope proof also pass.

Combined local validation passes **728** full-handler/consumer scenarios and **14**
offline official SDK listing-contract cases: 48 healthy storage comparisons, 56
recovery cases, 12 workers, eight parser cases, 12 exception boundaries, 166 PR78
admission rejection/recovery cases, 414 namespace cases, eight actual consumers
and four explicit first-bootstrap checkpoint-uncertainty cases.

A first-bootstrap page plus precommit checkpoint failure leaves no checkpoint;
the next combined invocation retains PR78's deliberate partial-bootstrap refusal.
An accepted checkpoint with a lost response recovers from actual durable progress.
That difference remains PR78's pending user decision, not authorization to ship its
bootstrap policy. The independent storage branch retains installed PR75 bootstrap.
Executable local evidence and results: `/tmp/series-combined-validation.py` and
`/tmp/series-combined-validation.json`, with a detached `/tmp/series-pr78-combined`
worktree. Fixtures use invented objects and blocked networking only.

## Remaining baseline risks and transaction limits

Pages, checkpoint and manifest are separate PUT operations. Without a transaction,
this code cannot guarantee atomic visibility, exactly-once writing, complete crash
recovery, or that a completed request corresponds to every retained page. An abrupt
process termination can occur before failure is observed or before worker cleanup.
Accepted but uncheckpointed pages can be rewritten on retry, creating additional
versions/PUTs. This deliberate behavior is retained.

A committed checkpoint can be newer than the manifest if the invocation dies or
manifest PUT fails; a lost manifest response can mean a new manifest exists even
though the handler returns an error. This existing manifest behavior is unchanged
and tested. Readers may observe successful page writes ahead of the old checkpoint
or old manifest. No rollback, cross-object lock or conditional replacement is added.

The repair does not repair retained missing pages, wrong existing counters/hashes,
old `missing_pages`, parser-caused partial data, ignored read failures, ECB slice
aggregation limits, Tier1 behavior, external writers, or changed completed-flow
inputs. Genuine `NoSuchKey` retains PR75's current bootstrap, including its populated
namespace replacement risk. PR78 addresses a separate admission decision and is
not incorporated or preapproved here. The existing discovery/counter-seeding LIST
request shapes remain unchanged; this repair adds no LIST or other recurring call.

## Coordination, release hold and rollback

No AGENTS.md or `.agents/skills` instruction files were available in the checkout
or mounted instruction directories. DEPLOY_LANE, current main, original writer,
actual SymDir/provider-catalog consumers, STATE, `docs/SESSION_CLAIMS.md` and
accessible GitHub PRs/claim references were revalidated. The only accessible open PR overlapping this handler
was the held PR78 draft. PR75's recorded admission claim is separate; the earlier
lookup claim is documented as released. The SDMX order claim targets a different
producer/acceptance path. This task takes no ops number and edits no other batch.
The first delegated lookup missed the repository claim ledger; its earlier
unavailable-ledger statement was incorrect and is superseded here. Parent
coordination confirms PR78 has no live editing turn and permits this separate
storage claim `S-codex#sewrite1002k4`. The ledger has no competing storage-write
claim. Only our new row is added; PR75/PR78 and all owner rows remain unchanged.

Before any draft publication, require final independent exact-head/integration
approval and all unmodified required gates. Parent live-batch coordination is now
resolved for this separate draft. Before any release,
require explicit user approval of this distinct recovery contract, parent claim
coordination, exact reviewed head and all existing release gates. PR78's bootstrap
user decision and PR75's strict RuntimeVersionConfig acceptance hold remain
independent and unchanged. Require authorized predecessor-control qualification
and post-release exact package/source/CodeSha256, commit-bound receipt, readiness
and projected-control acceptance. No AWS probe or release verification is run by
this draft task. Fresh natural publication cannot prove the injected failure path
ran or establish integrity of retained data.

A reviewed rollback would restore the byte-pinned installed PR75 handler and adapt
only the new storage-refusal tests/scope proof to the intentional restored failure
behavior. Retain both predecessor fixtures and this evidence; rerun healthy,
consumer, admission and tail-replay tests plus the reproduced vulnerability, all
required gates and independent exact-head review. Rollback is a code release with
its own authorization/verification. It does not reset, repair, delete or reconstruct
any retained object or checkpoint. No rollback has been performed.
