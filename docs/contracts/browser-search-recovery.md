# Browser search recovery and visible cache status

The chart search reads five existing catalog sources independently. Successful
downloads are reused for at most five minutes of browser activity. A failed
source retries on the next search after thirty seconds; healthy sources keep
their own interval. Either wall or monotonic clock advancement expires a check;
clock regression also makes the check due. There is no background polling or
producer invocation. Opening the search or changing its query drives checks.

One in-flight job serves concurrent callers. Each source has a ten-second bound
covering fetch and JSON decoding; when supported, AbortController cancels the
request. A timed-out completion cannot publish later. The instrument source
retains its existing direct-path fallback inside the same time bound. A complete
validated source replaces its own previous population. A failed or malformed
replacement retains the complete old population and reports cached/unavailable
status. Missing populations have null counts; a valid empty population has zero.
Successful loading of one catalog does not certify any other source.

`JHChartCatalog.indexStatus()` exposes per-source download status and clocks.
These are browser receipt times, not source observation dates or independent
security-identity verification. The optional `JHCqFuse` module controls its own
cache, so a returned object is explicitly `enrichment_unverified`, without a
download clock. Its own refresh and source qualification remain separate work.
The chart's standalone CISS history cache is also outside this change.

The search UI displays missing/cached catalogs, retry timing, unavailable remote
search and the Lambda's directory/warehouse integrity fields. A generation check
is not a freshness claim about economic observations. Missing, overdue, future
or unavailable generation evidence cannot become a current check. All source
labels remain escaped text. Remote rows survive catalog-triggered repaints.

Every remote request belongs to one query sequence, including A → B → A and
the FRED fallback. Changing or closing the query invalidates older responses.
HTTP errors, invalid packets and timeouts remain visible while other search
sources can still provide results. Requests use the existing three search
providers and existing FRED fallback only. Explicit identifier choice, exact
ticker identity and keyboard selection retain their prior contracts.

Validation uses complete retained predecessors and invented sources. Whole
module preservation checks compare every unrelated existing function and outer
statement. Two offline Edge suites exercise the complete chart HTML/CSS and two
modules at 1440 and 390 pixels, intercepting every request. Other scripts are
removed and chart drawing is inert: these checks prove search behavior and
layout, not actual source quality, chart rendering, normal production capacity
or live account/consumer behavior. Release acceptance compares whole static
artifacts with the exact Pages build. No native operation is required.
