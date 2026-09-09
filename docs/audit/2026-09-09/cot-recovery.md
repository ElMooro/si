# COT positioning and history recovery

The financial futures parser used CSV-style `*_all` names for four fields whose API identifiers omit `_all`. Missing values then became zero. The v2 parser uses the documented API fields and preserves missing/invalid observations without a rank. TLS hostname and certificate checks are enabled; requests use exact official hosts, bounded bodies and timeouts, no redirects and fixed error categories.

Each refresh fetches a complete current-provider-vintage sample for each configured contract. Legacy v1 histories are not reused because they may contain manufactured zeros. Report types are not spliced together. The fallback uses one complete legacy noncommercial series with an explicit classification label. This changes neither data entitlements nor the provider's revision semantics. A full sample request is limited to 280 rows over 264 weeks; four workers and a run deadline bound processing.

The output retains all returned observations, typed component positions, invalid rows, unavailable contracts and coverage counts. Percentiles exclude the current observation and require 26 valid prior observations. Missing/invalid positions, invalid open interest, stale reports and invalid history withhold ranks. Four-week change uses dates, not row count. Category clusters require matching report date and report type. All results remain descriptive and execution-ineligible.

`cot-extremes.html` reads its dedicated producer through the same-origin data route. It displays unavailable contracts, current rank counts, report dates and a selector for every complete calculation history embedded in the producer's output. The shared inspector exposes every field. Expiry removes stale ranks; a failed reload clears previous results. Text is escaped and numeric zero remains visible.

The page's historical-family contract deliberately remains partial: old files for contracts removed from the current universe and prior mutable versions are not enumerated. No public archive publisher or privacy gate was changed. The embedded histories cover all rows returned and used in this run, not all versions ever stored.

The source-reviewed `audit_refresh` branch suppresses notification calls. A selected-contract request cannot replace the global current feed. Normal scheduled notification delivery is unchanged in purpose, but no longer makes an unvalidated turning-point claim. Production verification must use the explicit quiet mode after exact source and configuration comparison.

References:

- CFTC API field definitions, TFF futures only: https://dev.socrata.com/foundry/publicreporting.cftc.gov/gpe5-46if
- CFTC report classifications: https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm
- CFTC disaggregated explanatory notes and historical classification limitations: https://www.cftc.gov/MarketReports/CommitmentsofTraders/DisaggregatedExplanatoryNotes/index.htm
