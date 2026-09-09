# Bloomberg publication and calculation recovery

The previously missing dashboard output had a legacy default destination and swallowed S3 failures. Source now declares the dashboard bucket, publishes its own canonical key, keeps new archives in a distinct Bloomberg namespace and raises a fixed retryable error when the primary write fails. Archive failure is explicit. Existing historical objects remain untouched.

The engine retains all observations returned by its configured provider requests. Macro comparisons use calendar cutoffs and observed cadence tolerance. Missing values, zero denominators and insufficient histories remain unavailable. Stock indicators use completed adjusted bars, Wilder RSI, chronological EMA, sufficient SMA history and an actual prior-year baseline. Missing or stale required inputs withhold the heuristic index. All heuristics are descriptive and execution-ineligible.

Credit thresholds now convert percent to basis points. Net liquidity converts reverse repo billions to millions, preserves zero facility usage and identifies mixed observation dates and the Treasury week-average basis. A monthly net-liquidity growth rate requires aligned component-level history and remains unavailable. Provider responses have time/size limits, strict finite JSON, permitted hosts and no redirects or credential-bearing exception text.

Authoritative unit references:

- https://fred.stlouisfed.org/series/WALCL — USD millions; Wednesday level.
- https://fred.stlouisfed.org/series/WTREGEN — USD millions; week average ending Wednesday.
- https://fred.stlouisfed.org/series/RRPONTSYD — USD billions; daily.
- https://fred.stlouisfed.org/series/BAMLH0A0HYM2 — percent; converted to basis points for the threshold model.

These changes do not supply first-publication vintages, executable quotes or independent out-of-sample model validation. Current provider history remains explicitly identified as current vintage. The two test suites check publication failure, typed missing data, calendar comparisons, indicator extremes, unit conversion and request/error boundaries.
