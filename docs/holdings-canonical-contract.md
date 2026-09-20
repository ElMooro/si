# Canonical holdings disclosures

`data/13f-positions.json` is the authoritative commit pointer for the four
historical 13F products. Its `canonical_replay` identifies an immutable manifest
containing complete product references. Read those references when joining views;
S3 does not provide a four-object transaction. A partially updated mutable alias
cannot authorize a join with a different snapshot.

The native research manifest binds original SEC submissions, covers, complete
information tables, amendment chains, exact quantities and values, and the
reviewed compilers. The canonical manifest adds the canonical compiler and all
four outputs. Run `python scripts/replay_holdings_canonical.py` with the matching
reviewed release checkout to reproduce both layers. Archived Python is evidence,
not code to download and execute.

Every manager appears in `by_fund`. `positions_ref` holds the complete immutable
fund document: `periods[report_date].positions` contains all reported positions;
`comparison.rows` contains the full current/prior union. `security_indexes`
indexes every comparison by report-period cohort and native security identity.
Each entry links to its manager, periods and exact position in that fund document.
No top-list truncation substitutes for the complete dataset.

CUSIP, reported class, SH/PRN and PUT/CALL define identity. No reviewed security
master has qualified a ticker join here. Therefore the historical ticker maps
are empty with an explicit unavailable-mapping status. This does not mean zero
holdings. Purchase/sale lists are empty because executed transactions are not
observed; all transaction dollars, direction and scores are null. Missing
evidence must abstain and leave score denominators. Consumers must not translate
empty compatibility maps into bearish or neutral evidence.

The source preserves exact decimals, original row references and separate report,
filing, acceptance and acquisition clocks. Market-value change is distinct from
reported-quantity change; neither proves a trade. Report cohorts, outright
shares, principal amounts and option positions remain separate. No summation
across managers establishes new economic capital, because discretion can overlap.

The scheduled research action collects originals and publishes native plus
canonical views. The classic schedule replays the current source and collects
only when its source check is over two hours old. New-filing events collect
immediately. `holdings_canonical_refresh` replays/publishes only; it does not fetch
new SEC responses. `holdings_research_read` remains read-only. All event actions
enter the native boundary; the historical writer remains in source for audit and
is not reachable from the configured handler.

All four preceding products are retained whole under the protected audit prefix
before replacement. Immutable products/indexes and replay are prepared first;
conditional sidecar writes occur before the authoritative positions pointer.
An interrupted update preserves the old authoritative pointer; a retry repairs
the exact prepared views. A concurrently changed alias is never overwritten
without revalidation. The earlier whole-cache preservation remains separate.

The source cutover does not certify downstream heuristic models. Remaining
consumer and CapitalFlow migrations must prove unavailable evidence contributes
neither a score nor a denominator. All Calls, sizing and execution permissions
remain false until separate, appropriate validation exists.
