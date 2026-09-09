# Reviewed public archive indexes

This producer lists metadata only. It never calls `GetObject`, opens JSON payloads,
uses caller-supplied prefixes or infers ownership from basenames. Its fixed
`REGISTRY` is the source of truth for output membership. The scheduled run is every
15 minutes UTC; deployment and the protected target alias are release-team work.

Each family publishes `data/archive-indexes/<original-engine>.json` with schema
`public-engine-archive-index.v1`. `engine` names the original archive writer and
`publisher_engine` names `justhodl-public-archive-index`. The original exact family
pattern is preserved in `families`. `snapshots` contains only exact matching listed
keys, size bytes, current `LastModified` and conservative provenance labels.
`index_observed_at`/`listing_started_at` describe the metadata enumeration window.
`complete` means pagination completed; `listing_is_atomic=false` explicitly avoids
claiming a transactional point-in-time view of a changing prefix.

All rows use `immutable=false`, `point_in_time_certified=false`,
`availability_basis="listed metadata only"`, and `content_status="NOT_READ"`.
Listing proves neither historical availability nor payload validity/freshness.
Unknown metadata stays null; zero-byte size remains zero. No payload or response
body is printed. A failed or malformed continuation publishes an unavailable index
with `complete=false`, empty `snapshots`, and a bounded service-error code, never
partial keys disguised as complete coverage. The catalog at
`data/archive-indexes/catalog.json` lists each exact index and its completion state.
Individual publication failures propagate; catalog publication occurs last.

`{"mode":"validate_only"}` performs the real metadata listings and returns the
`public-engine-archive-index.v1` validation envelope without any writes. Other
request fields never modify the registry, bucket or prefixes. HTTP access is not
configured. The Lambda uses the repository's existing execution/Scheduler roles;
least-privilege policy and promoted `:live` binding remain deployment checks.

## Reviewed ownership

The candidate inventory contained 19 entries. Two writers share
`data/archive/auction-crisis/*.json` and are excluded. The distinct
`auction-crisis-ai` prefix is not that shared family. `pump-radar-brief` is also
excluded because its historical narrative includes synthesis from donors without
source-proven writer/public provenance; this is uncertainty, not a finding that
private text was disclosed. No private or redacted historical family is admitted.
That first batch contains 16 reviewed unique families. The extension below adds six source-reviewed result families, for 22 total.

| Original engine | Exact family | Actual writer in its source directory |
|---|---|---|
| justhodl-auction-crisis-ai | data/archive/auction-crisis-ai/*.json | lambda_function.py:435 |
| justhodl-catalyst-classifier | data/archive/catalysts/*.json | lambda_function.py:562 |
| justhodl-convergence-radar | data/archive/convergence-radar/*.json | lambda_function.py:1247 |
| justhodl-correlation-breaks | data/archive/correlation-breaks/*.json | lambda_function.py:508 |
| justhodl-crisis-plumbing | data/archive/crisis-plumbing/*.json | lambda_function.py:1105 |
| justhodl-dark-pool | data/archive/dark-pool/week-*.json | lambda_function.py:488 |
| justhodl-freight-pulse | data/archive/freight-pulse/*.json | lambda_function.py:222 |
| justhodl-grid-queue | data/archive/grid-queue/*.json | lambda_function.py:985 |
| justhodl-momentum-leaders | data/archive/momentum-leaders/*.json | lambda_function.py:503 |
| justhodl-pair-trades | data/archive/pair-trades/*.json | lambda_function.py:379 |
| justhodl-pump-earnings-nlp | data/archive/pump-earnings-nlp/*.json | lambda_function.py:331 |
| justhodl-pump-mechanics | data/archive/pump-mechanics/*.json | lambda_function.py:548 |
| justhodl-pump-positioning | data/archive/pump-positioning/*.json | lambda_function.py:986 |
| justhodl-signal-fabric | data/archive/feature-bus/*.json | lambda_function.py:484 |
| justhodl-ticker-deep-research | data/archive/ticker-research/*.json | lambda_function.py:442 |
| justhodl-velocity-acceleration | data/archive/velocity-acceleration/*.json | lambda_function.py:155, through archive helper |

## Offline verification

Run `python aws/lambdas/justhodl-public-archive-index/tests/run_tests.py`.
The actual handler is exercised with metadata-only fake S3. Tests forbid all payload
reads, traverse multiple pages, retain zero size, enforce exact family depth/names,
ignore attempted caller prefix/bucket overrides, reject foreign/private paths,
handle access denial after an already returned page, detect missing/repeating
continuation tokens, and prove validation-only performs zero writes. Source-bound
manifest scanning resolves every individual registry output plus the catalog with
no unresolved writes. No test invokes cloud services.


## Reviewed extension: daily public results

Six additional exact families are indexed. Each has a unique source writer;
none is an input cache. The prefix guard now accepts the anchored registry’s
single basename wildcard rather than requiring every reviewed history to live
under `data/archive/`. A caller still cannot introduce any prefix: exact
`(engine, pattern)` membership is checked before listing. Private-source checks
and all metadata-only / completeness guarantees remain in force.

| Original engine | Exact family | Writer and selected archive content |
|---|---|---|
| justhodl-activity-nowcast | data/activity-nowcast/snapshots/*.json | source/lambda_function.py:268–276: date, Activity Index/z-score and fixed regime/momentum labels; FRED-only calculated fields |
| justhodl-cb-injection | data/cb-injection/snapshots/*.json | source/lambda_function.py:366–373: date, numerical impulse/unwind risk and fixed labels; FRED/ECB numerical policy inputs |
| justhodl-consumer-pulse | data/consumer-pulse/snapshots/*.json | source/lambda_function.py:333–341: date, pulse and sub-index numbers, fixed regime/momentum; no cross-reference prose/errors |
| justhodl-conviction-engine | data/conviction/snapshots/*.json | source/lambda_function.py:530–538: coarse book-posture label and subject/direction/conviction/net-signal; drops source reads, theses, single names, personal details and donor payloads |
| justhodl-stock-screener | screener/snapshots/*.json | source/lambda_function.py:1357–1423: explicit slim public-company/financial/price/insider/political/M&A metric fields; no full provider response or private book |
| justhodl-theme-cascade | data/theme-cascade-history/*.json | source/lambda_function.py:690–716: ranked public ticker/theme/ETF-flow numerical research and deterministic sizing rules; selected source labels only, no private notes, account or provider diagnostic bodies |

For conviction, the source can consume the personal PM engine’s coarse posture.
Its archived projection contains only the already public aggregate research
posture and numeric conviction, not PM prose, orders, positions, cash or NAV.
Theme cascade’s names/categories come from the fixed public ETF catalogue in
`justhodl-theme-rotation-engine` (THEMES and actual S3_KEY writer), industry
classification from public FMP/momentum data, and public ETF constituent flows.
Its `macro_regime` selects only the macro classifier’s fixed regime label.
The stock screener snapshot excludes the newly attached complete donor objects.
These statements review source lineage; no stored historical body was read or
attested, and no original availability date or immutable version is invented.

Two examined families remain excluded: `etf-flows/history/*.json` and
`macro/history/*.json`. Their producers can include raw HTTP error bodies and
exception text inside failed metrics before persisting historical snapshots.
Current producer error projection plus separate historical sanitization must
complete before an index can make those bodies newly discoverable. The exclusion
is based on the executable error path, not a claim that a live secret was found.

The handler tests traverse both pages of each newly admitted prefix, reject nested
or neighbouring cache paths, preserve the exact source-family association, and
continue to prohibit all payload reads and caller-defined prefixes.
