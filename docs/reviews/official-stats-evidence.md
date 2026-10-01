# Official-stats warehouse context — draft, independent review pending

## Problem and scoped result

The required `data/fed-nowcast-join.json` is a one-time `ops_5410` output from
2026-09-12T00:31:21.701795Z. Its current writer is
`aws/ops/ran/ops_5410_cleveland_zip_members.py`; the corresponding committed
report matches that timestamp. Six historical operation writers exist; no
scheduled Lambda writes the join. Do not rerun the historical parsers or stamp
old values fresh. The Oct 1 natural official-stats publication correctly held.

This slice adds `evidence.schema = official-stats-evidence.v1` to the existing
`brief-1.0` official-stats output. It does **not** migrate the required gate or
replace decision fields. An old/failed/future required join remains HELD; an
otherwise qualifying legacy input retains its predecessor behavior. The new
context cannot promote, demote, score, size, or vote. All projected observations
are explicitly NOT_DECISION_QUALIFIED. The integration state explicitly remains
`legacy_required_input_unmigrated`. This is an evidence-only repair, not completion
of the dependency migration or a claim that official-stats is now LIVE.

## Evidence checked, without private originals or provider calls

At main `0be70ca09308b931174949bcd0ee5ad5eb226038`, no competing canary/join
work or matching source commits appeared in the last 72 hours. The related
PR12 brief freshness claim remains listed. Dedicated review claim:
`S-shopiz#ose1001c`, branch `agent/shopiz/official-stats-evidence`.

Public canary publication generated 2026-10-01T01:16:12.615387Z contains GDPNOW
3.7372 with economic period 2026-07-01 and receipt 2026-09-30T15:16:14.142954Z;
T10Y3M is 1.09, period 2026-09-30, receipt 2026-09-30T21:15:55.180703Z.
Legacy join values remain 4.4164 / July 1 and 0.89 / September 11. The canary's
existing policy labels GDPNOW stale (92 days against 21). That rule is unchanged;
new publication/receipt clocks cannot qualify the older economic period.

`data/ops/releases/justhodl-canary-macro.json` identifies deployed
`512b6ff78bd45d21bcec07f4833717716b0111cb` (September 18); both recorded source
hashes match main. Current source uses `atlanta_gdpnow`, not the old warm key's
`atlanta-gdpnow`. It explicitly has no verified Cleveland model. T10Y3M is a
Treasury spread, not a Cleveland model; RECPROUSM156N is also not that model.
No retained original was read or replayed. The evidence states replay unverified.
The canary config references `justhodl-canary-macro-daily`; its description's
11:10 time is not independent proof of the current live binding.

## Files and consumer contract

- `aws/shared/brief_compiler.py`: existing official-stats mode makes one extra
  warehouse GET, projects only GDPNOW/T10Y3M plus source metadata, and adds the
  versioned evidence object before its existing single output PUT. No new writer.
  Other modes have no new reads or output fields.
- `aws/lambdas/justhodl-brief-compiler/tests/run_tests.py` and
  `tests/test_official_stats_evidence.py` in that same Lambda: synthetic offline
  regressions, actual production staging block and isolated packaged imports.
  Shared dependency scan selects **only justhodl-brief-compiler**. The existing
  local `source/brief_contract.py` overlay stays byte-identical to the shared copy.
- `aws/shared/jh_brief_adapters.py:OfficialStatsBriefAdapter`: inspected, unchanged.
  LIVE + existing `fields.gdpnow` controls eligibility and its existing score;
  optional canary evidence is not consumed. Producer-to-adapter regressions added
  in the bridge's `tests/test_official_stats_brief.py` compare identical signals
  with/without evidence and preserve HELD abstention.
- `config/brief-engine-official-stats.json`, the canonical registry, and both
  fusion/bridge bundled registries: inspected, unchanged. Shadow, NONCRITICAL,
  thresholds, TTLs, authority, and capital behavior remain unchanged.
- `jh-data-feeds.js`: discovery listing only, unchanged.
- `jh-chart-pro-dock.js`: reads legacy `fields.gdpnow` and ignores HELD today.
  This pre-existing UI limitation is **not repaired** here and the new context is
  not rendered. A later UI repair needs separate page/browser acceptance.
- `activity_research_store.py` reads the legacy join directly; unchanged. This
  slice neither overwrites the join nor substitutes data beneath that consumer.

The evidence records warehouse publication and LastModified separately from
reported economic date/period, receipt, and source publication (null when absent).
Raw source-reported age policy and evidence references remain labelled as such;
references are not replay proof. Clock issues include missing/invalid and strict
future timestamps. Fresh source metadata is not a new eligibility grant.
Numerical zero survives; missing, boolean, string, nonfinite and unrepresentable
values project to null with a reason. Unknown identities/units/contracts remain
explicit. Optional source read failures or malformed shapes leave base output
and legacy gating intact.

## Tests and review status

- New synthetic producer cases cover held/live gate preservation, access failure
  through the actual loader, missing/malformed objects, zero/null/nonfinite/huge
  values, separate clocks, strict future/exact-now, expired publication, unknown
  identity/unit/contract, unchanged source policy, same-period revisions, repeat
  determinism and no input mutation.
- Packaged importer: 24 tests passed, including the real deployment copy order
  and prior PR12 future-date checks.
- Shared brief + focused adapter tests: 77 passed.
- Full bridge + fusion consumer suites: 78 passed.
- Predecessor reproduction: 9 of the 10 new producer cases fail against the
  pre-change compiler; all 10 pass with this patch.
- Deployment gates: 993 static tests and 15 validated-candidate shell tests pass.
  Public-boundary tests: 15 pass. Page syntax: 599 graphs, no errors. Engine wiring:
  36 pages / 143 bindings, no missing or stale entries. Preflight: four Python
  files, no warnings. Secret scan and exact staged inventory run before commit.
- No UI source changes or browser acceptance claimed. No current deployment or
  live acceptance is claimed.

## Cost, rollback, and remaining decision

No new invocations, provider requests, schedules, targets, resources, workflows,
permissions or paid services. At the existing daily official-stats cadence, the
addition is **30 S3 GETs per 30 days**, no additional PUT count, and more bytes in
the already published brief. The authorized public sample is 179,558 bytes read;
projection adds 2,891 JSON bytes and yields a 3,438-byte sample brief. At unchanged
sizes this is about 5.39 MB additional reads and 86.7 KB additional written bytes
per 30 days; public response transfer grows about 2.9 KB per brief fetch. Versioned
bucket retention, if enabled, can retain those extra bytes per existing version.
An offline 1,000-iteration projection of that public sample averaged 0.013 ms per
projection; this excludes S3 latency and is not a production benchmark. SDK retry
latency for a failed optional GET remains a runtime consideration.

Rollback: revert this additive source/test change through normal reviewed
deployment of the existing compiler. No retained compiler inputs are introduced,
no old contract is removed, and existing consumers ignore the extra object. No
settings, schedules or data backfill/overwrite are required for rollback.

The remaining design decision is whether and how GDPNOW's economic period can be
qualified separately from its release/vintage dates, and whether the product
requires a real Cleveland model or explicitly just the Treasury spread. Evidence
must establish that contract before migrating the required input/decision fields.
Do not widen the 21-day rule, infer a model from T10Y3M, or enable a signal merely
because the canary envelope was recently published.

## Independent-review correction: optional timestamp normalization

Review of b373436 found that UTC normalization could overflow for
`0001-01-01T00:00:00+01:00`, aborting the new optional projection before the
existing brief PUT. The local evidence clock adapter now catches normalization
errors and returns null with `missing_or_invalid`; freshness receives that null,
never the rejected original value. No shared brief contract or required-source
clock behavior changed.

The regression matrix tests lower/upper UTC overflow, malformed text, missing,
object and boolean clocks across publication, economic, receipt, source-publication
and warehouse LastModified, with both LIVE and HELD legacy inputs (60 cases).
Each requires unchanged base status/fields/inputs/why and exactly one publication.
Representable lower/upper normalization boundaries are also covered. Before the
fix: 20 subtest errors and 10 failures; after: all pass. The complete importer
suite is now 26 passing tests; combined shared/bridge/fusion remains 151 passing.
