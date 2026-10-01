# Canonical holdings qualification diagnostics — draft

This change explains excluded membership coverage; it does not increase the number
of eligible funds. The existing `summary_reason`, identity tuples, source parser,
completeness/date rules, instrument treatment, row pages and decision permissions
are unchanged. Public production metadata returned HTTP 403 during diagnosis;
no alternate route, protected originals, provider or AWS API was used. The live
281-fund missing/duplicate/field-error split remains unmeasured.

## Retained contract and scope

New canonical acquisitions with the existing exact-string
`ETF_OWNERSHIP_SUMMARY_ENABLED=true` retain an additional
`ownership_diagnostic_policy=qualification-diagnostics.v1`. Absent/false/malformed
summary settings remain off. Recovery and duplicate-request paths retain their
original policies, regardless of the current environment. Explicit null, unknown
or orphan diagnostic policies are rejected. No configuration, cadence, acquisition,
workflow, UI or deployment changes are included.

The existing `qualified-membership.v1` summary contract remains unchanged in
meaning. Opted-in manifests additionally reference one content-addressed
`etf-qualification-diagnostics.v1` object in the existing directories namespace.
It contains the complete fund inventory, compilation clock, policy and, per fund:

- Current and prior counters: returned rows, source row count, field-error rows,
  missing ticker rows, missing identity rows and duplicate identity rows. Missing
  counters stay null; zero and invalid metadata are never conflated.
- Independent overlapping exclusion reasons: incomplete pagination/status,
  source-clock failure or expiry, mixed/future/invalid dates, future/invalid
  processing date, missing identities, duplicate identities and field errors.
  Missing/noninteger/negative required counter metadata is separately unresolved.
- The existing native `comparable_snapshots` boolean and a separate date-order
  explanation. Partial snapshots are not mislabeled as date incompatibility;
  same-date revisions and reversed or unavailable date pairs remain explicit.

The original single `qualification_exclusion` and `comparison_exclusion` remain
unchanged. Current and prior reasons are separate so a deficient prior does not
imply current membership exclusion. Missing ticker alone is not a qualification
failure. Null numeric values, uncertified currency/weight units and asset/security
labels do not independently acquire new eligibility tests. A missing identity
counter cannot distinguish absent identifiers from malformed identity fields;
field-error counters do not identify which field failed. These diagnostics must
not be presented as that finer causal evidence, actual trades or current ownership.
The unchanged UI does not render the new object; this is inspectable metadata.

The desk supplement retains its default reducer behavior with no diagnostic
policy. Canonical references consumed by desk/lookthrough continue to replay;
no extra fund holdings shards or diagnostic copies are created by those consumers.

## Byte compatibility and importer radius

Absent diagnostic policy produces identical old canonical summary and row bytes.
An invented immediate-predecessor fixture was generated using the exact trusted
main source at `6233db36489885f91b1e53e7b83c641840f898ab`, before editing, with a
2-fund canonical universe and 1 desk extra. It contains a canonical v1 summary and
a desk v2 supplement plus all retained synthetic objects. Both replay byte-exactly
under this draft without executing retained source. Existing older predecessor
fixtures remain covered too. Exact additional predecessor SHA-256 pins are:

| Module | SHA-256 |
|---|---|
| etf_holdings_model | 13a3cc577eac2101ad303971d7b52d04e4574b14bdc5ddd947d5b4d588487cc9 |
| etf_holdings_store | 564e975a150bbee905ba39d129960323cf0a6dc8265008d756ee686ee162b81b |
| etf_desk_store | 3fba26e742d01f587d3e67caab15f170beb93e4a515a0d7897c85a09a7818aa1 |

Compiler pin checks still verify exact retained source bytes and full output
reconstruction, including the diagnostic object. Unknown hashes or altered
artifacts fail closed. Transitive deployment/importer inventory is exactly:
`justhodl-etf-constituents`, `justhodl-etf-global-desk`, `justhodl-flow-lookthrough`.
This is a draft; no deployment to any of these functions is authorized yet.

## Publication cost and limits

`python tests/benchmark_qualification_diagnostics.py` uses invented rows with all
300 actual configured names/tags and realistic reference lengths. The committed
JSON gives reproducible byte/object results (wall times are illustrative):

| Scenario | Additional bytes | Additional objects | Existing row pages |
|---|---:|---:|---|
| All qualified | 141,603 | 1 | Byte-identical |
| Overlapping quality/date failures | 267,303 | 1 | Byte-identical |

Inlining these fields exceeded the existing manifest limit, so diagnostics use
one separate object. Neither 512 KiB per metadata object, 48 MiB total summary,
768 row-page cap nor any acquisition limit is raised. All bytes are checked
before any summary emission; overflow leaves the summary unavailable, not partial.
The maximum summary object count is now 768 row pages + 2 metadata objects = 770.
Per successful new canonical compilation, the incremental publication is at most
one diagnostic PUT and readback before retries, plus a small reference in the
existing manifest. Its own payload is capped at 512 KiB. At one new publication
per day, 30 runs add at most 30 PUT/readback pairs and 15 MiB of diagnostic payload
plus reference bytes. Replay adds no PUTs; uncached verification can add one GET.
Canonical inputs/manifests change hashes without adding object types/counts beyond
this one diagnostic object. Changed source artifacts are retained through the
existing compiler mechanism, not a new per-run shard family. No duplicate row
pages, provider requests, producer invocations or schedule changes are introduced.
This benchmark is not live runtime/capacity proof or an enabled desk capacity test.

## Validation and rollback

Tests cover isolated/all combinations of required quality failures, missing or
malformed counters, valid null/zero observations, complete-but-unqualified rows,
independent current/prior reasons, date revisions, partial snapshots with compatible
dates, atomic overflow, unknown policies, tampered diagnostic bytes, new canonical
and lookthrough/desk replay, and immediate/older predecessor replay. Acquisition
brake tests also verify the new policy is retained only when enabled.

Before merge, rollback is closing the draft. After a separately reviewed future
release, stop only new diagnostic generation by removing the single acquisition
assignment of `ownership_diagnostic_policy`, while retaining diagnostic-policy
read/replay support and all compiler pins. That follow-up must pin the deployed
store hash through the normal reviewed compatibility process. Do not deploy the
old parser over new retained inputs, delete artifacts, change old input policies,
or turn off canonical membership summaries merely to suppress diagnostics.
PR26 desk enablement remains draft/unmerged and is unrelated to this rollback.

Validation on the reviewed implementation: 387 shared tests and 63 subtests passed;
two unrelated risk-regime authority tests fail because they expect the removed
signal-board `n_risk_regime` function. Both failures reproduce on unchanged base
code. The 9 dedicated diagnostic tests pass, including artifact tampering and
prior/current isolation. Existing ownership UI tests pass 17/17. Deployment checks
pass 1,062 tests and 10 subtests; preflight passes with zero warnings, and secret
scanning has no findings. These are offline checks, not production acceptance.
Current main advanced to `cd48b871ea04c141d7e82fd6e5241a350b56908c` with only
independent Katlin research/claim files; the merge is clean and preserves that work.

After the clean main merge, the focused suites pass 36 tests plus 27 subtests.
Native importer entrypoint suites pass canonical 53, desk 78 and lookthrough 53.
