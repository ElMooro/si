# Independent exact-head review: history newest-50 candidate

Reviewed SHA: `38cefa71de11de4b5aa37c30f143cd7d2d66f268`.
Base SHA: `31b50acda4d33a127cff0ffb59a588a18e4005c6`.
Conclusion: **HOLD production release; retain draft PR #74.** No ordinary DynamoDB/JSON-input behavior defect was found in the narrow production diff. The change is conditionally acceptable as an offline candidate, not approved for merge or deployment.

## Exact source and behavior
- The full predecessor fixture byte-matches the base source, SHA-256 `fe1f2ee892f23c8fc39c3d901a2bc908654da76798c04417757563b19680fe73`. Candidate working source byte-matches exact reviewed HEAD, SHA-256 `d94eb47d09b3747691f10e641cfebf288ee77fc279f3fb79aea679d9fae663b8`.
- Production diff adds only heapq and replaces timestamp append with a bounded min-heap for string values. Counts, first/last, latest-hash comparisons, final descending sort, stable feed-count ordering, pagination and 101-page cutoff remain unchanged.
- Duplicate strings retain multiplicity. Rejecting a value tied with the heap minimum cannot change serialized timestamp values; latest_hash still uses the first encountered maximum. Non-string malformed values use the original accumulation path; incompatible mixed values fail before heap selection in the same existing first/last comparisons. The 50-value memory bound applies only to valid string feeds, not all malformed inputs.
- Scans, writes, retention, consumers, engine communication and runtime/deployment configuration files are unchanged by the candidate. Resource failures or real completion timestamps near a Lambda timeout cannot be proven equivalent by fixed-clock fixtures.

## Independently rerun checks
- Native snapshotter script passed 303 index cases, 318 complete handler fixtures and 4 actual API/audit consumer output pairs. It checks return values, exception type/message, stdout, ordered fake AWS calls and complete write bytes/options.
- Native history API suite passed 12 private/public/archive route scenarios using offline fixtures; no actual archive read occurred.
- 26 additional reviewer fixtures passed: malformed non-string values preceding 1/51/101 string-boundary inputs, a duplicated selection boundary across pagination, and equal-count feeds across pages. Independently asserted newest-50 duplicate multiplicity, first tied latest hash and feed insertion order.
- No broad gates were rerun: the parent reports the full local deployment gate already passed. This independent review does not substitute for the required exact-head CI gates.
- Working tree stayed clean and HEAD stayed at the reviewed SHA. No AWS call, producer invocation, network request, tracked-file edit, branch change, merge or deployment was performed.

## Independent CPU/allocation spot reproduction
The rerun used seven interleaved process-CPU samples after warmup and one traced-allocation sample per version at each scale. It executes the whole index builder using existing zero-latency fake scan/S3 fixtures and fixed wall clocks. Modules are prepared outside timing. Inputs retain all original timestamp strings. Raw samples are in `docs/ops/history-newest50-independent-benchmark.json`.

| Rows / feeds / order | Old CPU median | Heap CPU median | CPU change | Old traced peak | Heap traced peak |
|---|---:|---:|---:|---:|---:|
| 5000 / 1 / ascending | 3.299 ms | 4.627 ms | +40.3% | 42,998 B | 5,064 B |
| 50000 / 1 / ascending | 35.905 ms | 43.396 ms | +20.9% | 445,494 B | 5,070 B |
| 50000 / 1 / descending | 33.701 ms | 34.988 ms | +3.8% | 445,494 B | 5,070 B |
| 50000 / 1 / shuffle | 85.680 ms | 78.757 ms | -8.1% | 644,982 B | 5,070 B |
| 45000 / 45 / ascending | 35.303 ms | 43.510 ms | +23.2% | 411,777 B | 161,149 B |

These results support the recorded shape: substantially smaller selection allocations and ordered-input CPU regressions; shuffled inputs may improve. CPU variability exists and no live distribution, Lambda RSS, total page-memory pressure, provisioned-memory reduction or billed-duration saving is established. The benchmark is synthetic and does not establish measured dollar savings.

## Release blockers and deployment limitations
1. Ordered scans regress in CPU; representative live feed counts/order and actual memory pressure are unknown. The helper shares the whole handler timeout. Shipping the candidate now would assume the favorable distribution and could consume additional timeout budget. The hold conclusion is justified.
2. Static deployment reconciliation independently confirms legacy memory_mb/timeout_s do not activate memory/timeout update flags; runtime/role/handler need absent explicit opt-ins and configured environment overrides are empty. However description is reapplied and scripts/deploy_lambdas.sh lines 278–288 unconditionally reconcile X-Ray Active and the default DLQ, swallowing errors. A small source diff/green run cannot prove these controls are unchanged. The existing config has no schedule declarations, so its binding checks do not establish all live bindings or the five excluded user schedules.
3. No exact predecessor live package/control baseline, release package/receipt, unchanged-control projection or natural post-release output exists because there has been no release. Before reconsideration require bounded privacy-preserving read-only runner evidence, all exact-head gates, independent review of the release head, exact package/receipt/control proof and natural observation without a producer invocation. Rollback described in the hold document preserves source-only predecessor restoration through normal Actions; it is prospective and no production rollback is needed now.

No additional source repair is requested. `docs/ops/history-newest50-hold.md` accurately keeps the candidate held and separates synthetic resource evidence from billing savings.
