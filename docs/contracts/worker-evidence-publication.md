# Exact Worker evidence publication

The runner verifies the complete deployed Worker source, pinned Wrangler build,
source inventory and unchanged configuration before preparing a receipt. The
complete before/after capture remains an Actions artifact even if Git retention
fails. No private packet, Worker route or native producer is used for acceptance.

The evidence publisher validates the complete local receipt and capture again,
then fetches current `main`. A temporary Git index starts from that exact tree.
Only the immutable capture and mutable release receipt are added to a new commit.
An ordinary fast-forward push either retains those exact bytes or loses the race.
A lost race retries from the newly fetched tree, at most five times; it never
rebases or text-merges an assembled receipt. The caller checkout, index and HEAD
are preserved. Every success confirms the complete bytes on remote `main`, even
if the push acknowledgement was lost. Repeating an already retained publication
is idempotent.

The source commit must belong to current main history and every Worker build
input must still match. An existing capture path with different bytes refuses.
A later source receipt, later capture time, or equal capture time with different
receipt bytes refuses. These guards protect release evidence; capture time is not
a sequence number for portfolio data and grants no investment authority.

The workflow publishes the public S3 receipt only after Git retention succeeds.
Git and S3 are separate stores: a crash or S3 failure can leave the older public
receipt after the new capture is retained. Exact native prerequisite checks then
fail closed until publication is repaired. This is not a two-store transaction,
proof of actual private publication, or a claim of an absolute network deadline.
Normal workflow concurrency remains serialized. No force push, Git user-config
change, AWS key request or producer invocation is required.

Tests execute current code on whole invented local repositories and a bare local
remote. They reproduce the previous mutable-receipt rebase conflict, competing
writers, lost acknowledgements, main moving during push, corrupt local evidence,
immutable-capture conflicts, stale releases, bounded retries, caller-state
preservation and the Git-before-S3 workflow ordering. The complete predecessor
workflow is retained as inert bytes under `tests/fixtures/pre-worker-evidence-publisher/`.
