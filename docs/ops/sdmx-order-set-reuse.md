# SDMX walker membership-set reuse

This candidate changes only `_order` in `justhodl-sdmx-walker`: the priority
membership set is constructed once when the second iteration yields its first
ID, then reused. Prefix processing, priority order, duplicates and the returned
list remain as before. The lazy construction matters: a one-shot iterator can
yield unhashable priority values on the first pass and yield nothing on the
second, which previously never attempted hashing. Eager construction would
introduce a new failure. Malformed reusable inputs continue to fail as before.

The exact full predecessor source is retained as a byte fixture from main
`ab0a502c047c1399815c9c081fe633d9dba3d608`. No other production function changes;
the engine test runner asserts AST identity outside this helper. The three
actual callers are the Eurostat, ECB and OECD catalog-ID list comprehensions.
Every agency-mode invocation evaluates all three catalog orderings, even when
another agency owns the requested walk or its state has no remaining work.

Ownership was checked against current main, open PRs/branches and
`docs/SESSION_CLAIMS.md`. Claim `S-codex#sdmxset1002m7` reserves only this helper and
read-only acceptance 6442; provider-catalog PR66's files and claim are untouched.
No AGENTS.md or local `.agents/skills` files were available in this environment.

## Offline equivalence and consumers

`python3 aws/lambdas/justhodl-sdmx-walker/tests/run_tests.py` compares the complete
predecessor and candidate: 8,626 helper cases across lists, tuples, iterators,
prefix iterators, empty/duplicate/mixed scalar IDs and malformed inputs;
90 complete handler fixtures with fixed wall clocks and gzip timestamps.
Catalog and state reads, ordered submissions, URLs/headers/caps/timeouts,
worker settings, all write options/metadata/bodies, final state, summaries,
return values, stdout and visible failures must match exactly. Fixtures include
fanout, all five agencies, empty/missing/malformed catalogs, existing done/lease
state, retry failures/truncation, reset_done and combined modes, negotiation,
triplet fallback/failure, tiny responses, provider/storage errors and budget cut.
Executor completion is deterministic; live concurrency races and real network
timing are not simulated. All fixtures are invented, without archive reads.

The actual series-extractor Eurostat parser and import-sentinel classifier also
consume the synthetic walker writes/state and must agree. Existing cycle-features
and SymDir consumer suites pass. Their source/data contracts, the ECB-deep shared
state, the provider catalog, all output locations and storage options are unchanged.

The local release gates passed: 1,033 deployment static tests, 15 validated
candidate shell checks, secret scan, stub guard, selected source/config validators,
ops preflight, 15 public-boundary tests and engine-wiring check. An initial shell
check failed because child Python processes did not use the prepared test venv;
the full suite passed with that venv on PATH. No gate or unrelated source was
modified to obtain a pass. Run those gates against the final release head again
if its dependencies change. Independent exact-head review and Actions acceptance
must precede the production merge/release.

## Local runtime evidence and limits

`docs/ops/sdmx-order-validation.json` contains reproducible Python 3.12.14 CPU
timings, traced peak allocation and full fixture fingerprints. The optional
`--benchmark --json PATH` mode uses five samples per size/priority combination.
Additional interleaved seven-sample measurements use synthetic scales approximating
public provider-catalog dataset counts (905/1,497/8,197); these counts are not
assumed to be the exact ordering inputs. No retained catalogs/data were fetched.

At 1,000 IDs/100 priorities, local median CPU falls from 2.69ms to 0.58ms (4.64x).
At the 14,000-ID stress scale/1,400 priorities it falls from 301.83ms to 5.57ms
(54.24x); higher priority shares amplify the predecessor's quadratic work.
Small and no-priority cases benefit little: some sub-micro/microsecond samples
regress or vary. Set construction count falls from N to one for a nonempty
second pass, and remains zero for an empty second pass. Peak memory is not
uniformly lower: the retained set can overlap the final concatenation; for the
14,000/1,400 case traced peak rises from 283,224 to 362,360 bytes. These are
local CPU/allocation observations, not measured AWS duration or dollar savings.

Catalog ordering runs outside `_walk_generic`'s elapsed download budget. Reusing
the set removes time before that budget starts; it does not change requested
PER, workers, budget, cadence, retries, leases or checkpoint behavior. Finishing
ordering earlier can shift the network window and wall-clock metadata under
real scheduling; boundary effects can change which in-flight work finishes.
Fixed-clock fixtures establish computation/handler equivalence for matching
completion outcomes, not byte identity across independent real timed executions.
Any financial effect depends on actual invocation duration and billing; network
work may dominate. No AWS bills, billing metrics or private financial output were
read or published.

## Release acceptance and rollback

Read-only `staged/ops_6442_sdmx_order_acceptance.py` runs through normal GitHub
Actions, after its own independent review. Before release it must verify the
exact predecessor package and existing technical controls, then its sanitized
baseline is retained as `docs/ops/sdmx-order-baseline.json`. After release it must
verify the exact candidate handler, ZIP CodeSha256, matching commit-bound receipt,
Active/Successful readiness, unchanged projected operating controls and ten
named schedule references: five walker references and the five intentional monitoring
schedules. Optional historical retry absence/state is captured and must remain
unchanged; mandatory monitoring and the declared walker binding stay enabled. It requires a baseline for candidate acceptance. Target payload
contents, actual role values, undeclared environment values, other package
members and arbitrary bindings are explicitly outside this projection.

The production diff contains no config/schedule/storage/IAM/security/retention/
concurrency/timeout/paid-service/trading-policy edits. This engine has no managed
schedule-write block in config; its existing bindings remain on the original lane.
The standard release's X-Ray/DLQ reconciliation must already match the baseline
before release, avoiding a control change. No manual producer invocation is allowed.
Use the normal pinned release workflow and exact public receipt verification;
observe the current public walker summary on its natural schedule. An unchanged
summary or new publication does not prove `_order` executed, and should never
be reported as full live equivalence or financial measurement.

Rollback is a reviewed commit restoring only the predecessor `_order` helper,
merged through the same Actions release lane. Preserve all state/data, historical
evidence, config and schedules. Verify the rollback package/receipt and the same
projected controls; keep normal producers scheduled. No deletions or reset/retry
invocations are part of rollback.

## Verification history

Probe PR #68 and diagnostic PR #69 were independently reviewed and merged without
producer changes. Premature run 36955156112 failed before the probe file was
landed. Baseline run 36955311522 held at a control mismatch; diagnostic
run 36955593528 narrowed it to temporary storage. Those red runs are retained,
not treated as passing acceptance. The verifier had incorrectly required the
unused legacy `ephemeral_mb` metadata to match the live size. The actual deploy
lane reads only canonical `ephemeral_storage`; this engine has no such field,
so the lane preserves the live temporary storage. The corrected read-only
probe captures the actual setting in its pre/post fingerprint, rejects drift
and never changes storage or config. Its correction requires its own exact-head
independent review and baseline acceptance before this candidate can release.

While release was held, a natural public summary at 2026-10-02T02:26:21+00:00
showed the existing OECD retry mode publishing with 65 visible HTTPError ledger
entries. That confirms existing scheduled output is continuing; no producer
was invoked by this task, no helper execution is inferred and no checkpoint or
failure-ledger correction is in scope.

The next baseline attempt, run 36955949609, passed function-control checks but
stopped at the Eurostat retry schedule read. Its original creation in ops 4911
was conditional. The verifier now records optional historical retry absence or
state in the pre/post fingerprint and rejects additions/removals/retiming, while
keeping the five monitoring schedules and declared walker binding mandatory.
No schedule is created, enabled or retimed to satisfy this check. This technical
correction also requires independent exact-head review before a new baseline.


Corrected probe PRs #70 and #71 passed independent exact-head review, then
baseline run [36956524665](https://github.com/ElMooro/si/actions/runs/36956524665)
passed with 19 AWS reads, one signed package download and zero writes/invokes.
The live handler matches the exact predecessor SHA-256, and its code SHA-256 is
`nKqoKqhpZRWO8mtoetlS0G9HYBgLw4xwNxb1dhvo2cI=`. The old deployment has no
release receipt; a verified commit-bound receipt remains mandatory after this
release. The projected operating fingerprint is
`dcf6a81a255eeb1b59254390dfaed6d300f168783fc30b25596825c090dfc4e7`.
Live temporary storage is 2,048 MB, reserved concurrency is unset, and the
historically conditional Eurostat retry reference is absent. All five intentional
monitoring schedules are enabled. Existing walker bindings retain their actual
cadences, including the hourly-named rule running every five minutes and the
hourly-named OECD retry running every fifteen minutes. None is repaired or retimed.
The complete sanitized baseline is retained in `sdmx-order-baseline.json`.

Independent source review accepted `8b4c14b484cd1a6cd937c5d77ff133f6d4a4584e`,
including 2,020 additional helper comparisons, the 8,626 committed comparisons,
90 complete handler fixtures, consumer suites and all local release gates. Its
independent interleaved measurement at 8,197 IDs/819 priorities was 80.42 ms to
3.11 ms (25.85x). Final rebased-head confirmation accepted `3553eb2dd8f43ef678d6e5db825ccec94fc5a9bf` before merge; its full deployment suite passed 1,034 tests plus 15 shell checks and six probe tests, with 15,457 files scanned and no secrets found.


## Verified release

Production [PR #72](https://github.com/ElMooro/si/pull/72) merged as
`c6d34a1706ad71d7853da439fc92160e29961fc8` after independent exact-head
acceptance and passing available Actions checks. CodeRabbit's green context was
an automatic-review skip; it is not counted as the independent review.
Normal push-triggered [deployment run 36957398475](https://github.com/ElMooro/si/actions/runs/36957398475)
passed every release step, including deployment preflight. The public receipt is
verified, names this exact merge/run, records deployment at
2026-10-02T02:52:53Z and matches handler SHA-256
`736138a65f926206688f94fdf08b9a17f5594193234327d16be6f985cd5c4214`.
Its ZIP hash matches CodeSha256
`qh4jtwQ0wtThYXmk1RHDd40/WcwXsJLU1pxPWMLi9Rc=`.

Post-release [read-only acceptance run 36957806995](https://github.com/ElMooro/si/actions/runs/36957806995)
passed with candidate phase, baseline comparison, the exact handler/ZIP/receipt
and Active/Successful readiness. Every projected control and ten named schedule
reference equals the baseline, fingerprint `dcf6a81a255eeb1b59254390dfaed6d300f168783fc30b25596825c090dfc4e7`.
All five intentional monitoring schedules remain enabled, actual walker/retry
cadences and optional retry absence remain unchanged, temporary storage remains
2,048 MB and reserved concurrency remains unset. The probe used 19 AWS reads,
one package download, zero writes and zero producer invokes. Source/config,
package-receipt and controller evidence are retained in `sdmx-order-release.json`.

The probe observed a naturally published current-summary S3 Last-Modified of
2026-10-02T02:54:44+00:00, after the release. The public site initially still
served its cached StatCan summary at 02:49:41 (five visible failures, zero new
pulls); these are distinct observations. At 02:59:43 the public GET naturally
refreshed to the ECB summary as of 02:59:41: COMPLETE, 214 total flows, zero
new pulls and seven visible failures. No helper execution or byte equivalence
of independently timed live runs is inferred from these publications. No producer was manually
invoked and no existing failure or checkpoint was repaired.

Actions log download returned HTTP403; successful run/job metadata, the exact
public receipt and the reviewed live acceptance establish the release evidence.
An alternate public proxy hostname also returned a network tunnel HTTP403; the
public site remained reachable. No retention, historical object, security,
schedule, cadence, timeout, concurrency or billing change was made to obtain
acceptance. Earlier verifier failures remain recorded above. AWS runtime and
financial savings remain unmeasured. The reviewed-helper rollback procedure
above restores the byte fixture without altering data, controls or schedules.
