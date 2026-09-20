# Manager disclosure overlap

`justhodl-smart-money-cluster` now publishes `holdings-disclosure-overlap.v1` at
`data/smart-money-clusters.json`. The historical transaction/conviction scorer
is retained as inactive source, but the configured entry point cannot call it.
`smart-money.html` is the native disclosure comparison desk.

## Measurement

The producer binds one immutable canonical 13F run and reads every manager's
complete current public amendment chain. A previous-quarter comparison is not
required. Each manager belongs to its own report-period cohort. Historical
cohorts remain inspectable; unavailable chains are excluded, never empty sets.

The exact identity is `(CUSIP, reported class, SH/PRN, PUT/CALL)`. Each positive
reported quantity enters that manager's set. Zero quantities remain in the
complete scope artifact but do not enter overlap sets. Negative/ambiguous
quantities, duplicate manager CIKs, forged bindings and incomplete artifacts
fail validation. No guessed tickers, prices, dollar sums or trade labels enter
the calculation. Class spelling differences may split an economic security.

For every manager pair in one period and instrument scope:

- shared = the exact intersection of positive-quantity identities;
- union = the exact union of those identities;
- Jaccard = shared / union;
- manager coverage = shared / that manager's set size.

Empty denominators are null. Integer counts and complete shared-identity lists
are retained. Percentages use six decimal places with half-even rounding. The
browser independently reconciles every security, quantity-membership count and
manager pair using exact integer arithmetic. Screen pagination does not truncate
the retained dataset. Search does not change pair metrics. Table manager counts
refer to the full cohort, not just the selected pair.

## Evidence and publication

Complete scope artifacts, calculation output, reviewed compiler bytes and run
manifest are content addressed under `data/holdings-overlap/`. The mutable
pointer is served without caching. Its immutable run binds the exact canonical
input, native research run and full fund/filing references. Original-record
links pin the native run, manager and CUSIP; they never silently switch to latest.

The runtime validates source hashes and full fund artifacts, calculates all
scopes, preserves the entire preceding public packet privately, writes immutable
artifacts, replays its complete derivative output and conditionally replaces the
current pointer. Failed validation performs no writes; conditional publication
refuses a concurrent writer. Immutable readbacks must match exactly. The first
cutover's whole legacy packet was also independently retained by ops 5886.

Independent acceptance and the command below go further: they reparse the retained
SEC originals with the matching reviewed native/canonical compilers before
reproducing the overlap output. Downloaded compiler code is never executed.

```sh
python scripts/replay_holdings_overlap.py --run <immutable-run-sha256>
```

The configured hourly `:55` schedule follows the native collection cadence.
Identical source and compiler bytes are a no-op and do not renew publication or
source timestamps. Passing the 48-hour source expiry produces a stale result
without changing the source clock. A future source is rejected on both new
publication and reuse paths. Collection freshness never means current ownership:
the report period and observation age remain visible separately.

## Authority and limits

This is descriptive public co-disclosure research. It does not establish purchases,
independent investors, conviction, live positions, exposure correlation,
diversification, forecast edge or sizing authority. Calls, sizing and execution
eligibility remain false, call is null, and additional independent votes is zero.
Public omissions, shorts, common discretion, manager relationships and the limited
roster remain explicit limitations. Options and principal amounts are separate
scopes, not interchangeable share counts.

Legacy `clusters: []` means the old inference is retired, not zero institutional
activity. `consumer_migration_complete` remains false while remaining consumers
and CapitalFlow are being migrated. Exact commit receipts, actual packaged-source
verification, public original-source replay and live page verification are needed
to accept a release. A configured schedule alone does not prove recurring execution.
