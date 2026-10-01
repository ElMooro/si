# ETF desk execution-budget safeguard

Scope: defect repair for `justhodl-etf-global-desk`, not PR26 activation. `ETF_OWNERSHIP_SUMMARY_ENABLED` remains absent/disabled; explicit `false` still rolls new acquisition back to v1 while retained v2 recovery/replay remains readable. No schedules, Lambda memory/timeout, vendor collection requests, IAM, secrets, classification, retention or automatic recovery policy changed. No live manual invocation or new ops/provider probe.

## Admission and transport limits

The invocation-local budget checks compilation boundaries, cache reads, artifact queue/worker admission, every S3 call, each SDK send/retry and each bounded body chunk. Existing transport settings remain 5s connect/20s idle read, legacy retries with three total attempts; the handler now states `total_max_attempts=3` explicitly (equivalent to the prior Config `max_attempts=2`). A conservative 120s per-call allowance accounts for planned attempts/backoff/body progress; it is not a wall-clock guarantee. Slow trickles, DNS, socket writes and transport behavior can exceed that allowance.

Work admission reserves 120s for draining and 120s for a failure checkpoint. I/O admission additionally needs 120s; short invocations at or below 360s admit no request write. Queues are bounded (writer 12, warm reads 16); workers recheck before I/O. Any budget refusal latches stop across workers. Failure cancels queued futures and waits only within the drain/checkpoint reserve; shutdown never adds an unconditional wait. Already-running work cannot be canceled. A late immutable PUT can complete, but its subsequent readback/retry is refused and it cannot initiate public replacement. The SDK hook remains attached on failure so late retries cannot borrow the coordinator's thread-local checkpoint allowance. Quiescent successful invocations detach it.

Only after all compiler/writer/replay pools finish is the drain reserve released. Immediately before each conditional root or alias PUT, admission requires more than 360s (120 PUT + 120 readback + 120 checkpoint). Full replay and the durable candidate checkpoint precede root replacement. A fully consumed collection deadline at end-minus-360 leaves insufficient publication allowance once subsequent work takes time: the safe outcome is withholding. Full-workload evidence is required to quantify that operational tradeoff.

Provider acquisition retains its existing executor context-manager waits and vendor transport. An already-running collection/save can therefore overrun before returning to this guard. The compiler/writer/warm/replay checks prevent further unsafe work once control returns; they do not impose a hard acquisition deadline.

A root PUT accepted with response lost has an unknown commit outcome; no rollback is attempted. The durable candidate replay remains the evidence. Failure after root replacement can retain new replayable root with old aliases. If no safe checkpoint budget remains, the existing running request may remain; no new public status or recovery policy was added. Hard Lambda termination need not execute cleanup/finally.

## Compatibility and blast radius

Only `aws/shared/etf_desk_store.py` changes shared behavior, through optional/run-local guards. Ordinary reader/replay and budgetless writer callers retain their APIs and error propagation. `scripts/shared_dependents.py aws/shared/etf_desk_store.py` returns only `justhodl-etf-global-desk`. Shared holdings/flow modules and compiler mathematics are unchanged. No fleet deployment is warranted.

Reviewed immediate predecessor store SHA256: `6a8e288db962cd50455d69e04c5fed7da0e40df3b18b564799a59b4fc37262ac`. PR26 store hash `3fba26e742d01f587d3e67caab15f170beb93e4a515a0d7897c85a09a7818aa1` was already pinned. New synthetic v1/v2 predecessor fixtures retain the exact former compiler bytes; replay reconstructs every output/artifact byte using current code without executing retained source. Existing older fixtures and immutable/recovery readers remain.

## Evidence and release gate

The 48 desk tests pass; two-file preflight passes with zero warnings. Offline tests cover short budgets; synthetic collection and compilation overruns; blocked active writer plus queued cancellation; body consumption/close and deadline progress; actual SDK retry limit without network; pagination; failures before root and after accepted/lost-response root; complete candidate replay; explicit-false rollback and v1/v2 exact predecessor bytes. Existing desk qualification, identity, recovery, partial readback and supplementation regressions remain required. Deployment requires independent review of the exact commit and normal Actions receipt proof.

No AGENTS.md or .agents/skills exists in the supplied checkout or current origin/main tree. DEPLOY_LANE.md, AUTONOMY.md and claims were inspected. Remote desk history showed no matching code activity within six hours on takeover; PR26 remains draft. Completed S-shopiz implementation ownership and outstanding evening verification notes remain intact in SESSION_CLAIMS.md.

Cost: no new paid AI/vendor calls, resources or schedule frequency. Guard overhead is local clock checks and bounded body chunks; refusal may reduce work and writes, but immutable evidence already admitted remains stored. No quantified savings or full-capacity claim. Prior ~350/483MiB synthetic benchmarks are not Lambda capacity proof. Ops6410 established 4096MiB/900s and package agreement, without current memory REPORT samples. Optional supplement activation remains blocked pending full-workload evidence.

Natural-publication checkpoint remains the existing 23:05 UTC daily desk run, after canonical holdings. Observe its normal output and retained replay following release; do not manually invoke or change schedules. Receipt/CodeSha agreement is code deployment evidence, not proof of that future publication or capacity.
