# Unavailable scalar history and calendar display

Stage 501 extends the existing observation and transport-cache contracts. It adds no provider acquisition, trading permission or new engine schedule.

## Empty charts retain evidence

An exact CQ/CISS parser result with no valid plotted values remains an identified empty frame. Its requested symbol, display interval, received source packet, selected path when uniquely known, every evaluated record, rejection reasons and download diagnostics remain inspectable and downloadable. Unknown identity, ambiguous identity, malformed arrays and all-invalid observations remain distinct. Counts unavailable from the parser display as unavailable, never measured zero. An existing received series with zero accepted points can report that actual zero.

No other series is substituted. A different requested identifier, unbound array, nonempty diagnostic array, stale generation, different current symbol or different interval cannot publish this diagnostic frame. Download completion alone cannot replace the current scalar source label. Empty-frame clearing removes the preceding plot, replay state, auxiliary plots and prior ticker watermark. Missing helper modules still produce an unavailable chart without inventing a received packet.

## Calendar display

Scalar intervals are deterministic UTC calendar groups even for one observation or sparse history. Weeks begin Monday. Fortnights are anchored at Monday 1970-01-05 and use floor arithmetic for dates before the epoch. Fixed 2/3/5-day groups are anchored at 1970-01-01. Months and quarters begin on their first calendar day. Every contributing source ordinal remains attached; every original period and value stays in the complete export.

The displayed line is the last accepted scalar in each group. First, minimum, maximum and last are descriptive grouped coordinates, not market OHLC. The legend labels the contributing source-period range, the source period of that last scalar and the separate group coordinate. None of these is a source publication or availability timestamp. Missing volume remains unavailable. Existing market-price grouping behavior is preserved and does not gain qualification from these scalar tests.

## Validation and limits

Whole-module invented inputs cover unknown, ambiguous, malformed and rejected histories, selection races, pre-epoch/sparse/single observations, signed/zero measurements, inherited category names and escaped text. The complete predecessor modules and reproduced failures are retained. Browser fixtures intercept every request and use both an inert drawing mock and the complete pinned chart library at desktop and mobile widths. No actual application/private/current-consumer packet is read and no producer is invoked.

The shared contract still does not establish original-source equivalence, source freshness, first-publication timing, predictive validity or portfolio consequences. Gap-aware line presentation, some legacy chart labels/axis formatting, generic warehouse decoding and broader page correctness remain open. The ten institutional acceptance workstreams remain open.
