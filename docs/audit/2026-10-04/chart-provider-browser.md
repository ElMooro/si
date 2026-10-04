# Chart provider discovery and coverage

This batch adds a Providers control to chart.html symbol search. Dataset results
now open an in-chart browser instead of discarding the selected dataset at the
provider landing page. The exact mixed-case ID and the requested chart, compare
or watchlist destination travel together. A country/dimension choice must resolve
to an explicit series before it can be plotted. The existing Treasury par adapter
is included in chart classification; its data contract is otherwise unchanged.

The browser separates indexed series from stored-file metadata. Directory entries
are paged at 50; provider text search explicitly reports its native 200-result
ceiling. Dataset scan bounds and sampled facets stay visible. The stored-file view
follows every declared continuation page and labels its text filter as page-local.
An object name is never converted into candles or a source-equivalence claim.

Only explicitly chartable scalar rows with a native adapter can use the new
selection control. Other records remain inspectable with their full metadata.
Existing market search remains available. This capability check is not a claim of
complete history, matching economic definitions, entitlement or current observations.
The browser retains and downloads original successful metadata bytes with URL,
receipt clock, HTTP status, byte count and SHA-256. Denials have no alternate-route
retry. Closing or navigating supersedes a pending response. Native modal behavior
keeps background chart controls inactive; desktop and mobile use the same path.

All 73 public provider entries were investigated. Seventy published heads and all
1,588 declared continuation pages were captured (788,041 continuation entries).
Three derived heads returned403 and were not bypassed. Four first-page directory
queries timed out; 66 succeeded. Continuations lack a generation ID, so the result
is not an atomic snapshot. Full metadata receipts and individual hashes are in the
adjacent catalogue/directory receipt files. No listed measurement object was read.

The full public watchlist still has 10,745 unique IDs, with 4,567 lacking a handoff.
The concurrent 5169f1d85 Eurostat and 8b086c7d2 formula changes are preserved.
The full route audit was repeated after each reconciliation. Handoff counts are not history/identity qualification.

CFTC primary schemas identify exact fields for all 346 requested COT IDs across
Legacy, Disaggregated and Financial report families. The initial 329 matches were
expanded after checking TradingView's Legacy definition of the 17 LT concentration
codes against CFTC's LE column definitions; no numeric conversion is applied. These are discovery results only: no new CFTC adapter or
contract/history verification is claimed in this client batch. Official schema
identities, hashes, field names and unresolved requests are retained separately.

The earlier access candidate passed4,089 frontend,1,075 deployment and15 shell
tests, but did not ship: a concurrent main update stopped the push. It is included
here, with the complete combined candidate requiring fresh checks and served-source
acceptance. No AWS keys, producer invocation, paid AI API or schedule change is used.
The overall institutional upgrade remains open.
