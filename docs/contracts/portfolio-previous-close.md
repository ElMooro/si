# Identified previous-close source evidence

Portfolio snapshot marks come from the existing configured stock previous-day aggregate endpoint. Each request is bound to one supported ticker, split-adjusted prices and the existing HTTPS origin/path. This is a descriptive mark, not a current quote, execution price, total-return series, independently reconciled instrument or verified currency. The provider's [previous-day aggregate documentation](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar) defines `t` as the aggregate-window start and the adjustment as split adjustment. Request acquisition time must not replace that measurement time.

## Complete HTTP source

The reader refuses redirects before following them, requires the final response URL to match the exact request, accepts only HTTP 200 JSON with identity encoding, and reads bounded chunks through actual EOF. If present, Content-Length must be a valid nonnegative integer matching the whole received body. A 128 KiB byte limit and twenty-second acceptance deadline prevent silently truncated acceptance. The existing socket timeout is eight seconds; acceptance deadlines are checked around reads and cannot preempt a blocked socket. Streams close on both success and failure. These bounds are operational limits, not provider completeness guarantees or a market-session calendar.

Complete accepted bodies are retained as exact base64 bytes with SHA-256 and length before semantic parsing. A complete but invalid body remains inspectable and cannot supply a price. Incomplete, oversized, incorrectly framed or redirected bodies never claim complete source evidence. The request trace retains origin/path, ticker, adjustment, acquisition start/end and fixed failure codes; it does not retain the credential-bearing query string or echo raw exception text. HTTP errors retain status only and close the body. This release changes no credentials or provider subscription.

## Identity and value checks

Strict UTF-8 JSON rejects duplicate keys at every depth, nonfinite numbers, overflow, invalid Unicode and non-object envelopes. The response must have status OK, the exact requested ticker, adjusted true and exactly one result, with integer queryCount and resultsCount both one. A present per-bar ticker must also match. Unknown or delayed status is withheld explicitly; no assertion of live provider coverage is inferred from invented tests.

Open, high, low and close must be typed, finite positive numbers with a coherent OHLC range. Volume must be typed, finite and nonnegative; measured zero volume stays zero. The aggregate-window timestamp must be a nonnegative exact integer within JavaScript's safe integer range. Existing holdings accounting separately checks age and future skew. Its 120-hour age policy does not establish that this is the most recent exchange print. An epoch timestamp remains historical evidence rather than being relabeled with request time.

The snapshot retains the complete response under accounting.source_prices. Each displayed symbol's price_source references its source hash and identity/status, without duplicating the body in each row. Unavailable quote objects cannot increase the priced count or turn missing values into zero. Currency and held-instrument binding remain unverified: a ticker match alone cannot distinguish a stock from a similarly named holding in another asset class. Capital sizing remains blocked.

## Verification and limits

The complete 44,708-byte predecessor and eight full invented failure scenarios are retained inert. Twenty-eight focused current-code cases test source identity, adjustment, exact cardinality, typed values, response bounds/EOF, malformed JSON, redirects, deadlines, cleanup, error redaction and actual-handler accounting. Eleven complete current-reader scenarios plus one complete handler run retain invented inputs, outputs and mocked writes. No actual account, provider endpoint or native producer is used for acceptance.

The exact candidate ZIP must pass all 146 snapshot tests plus four handler integrations through the offline release path. Read-only native acceptance checks the intended receipt, full source closure, live alias, original resources and original hourly schedules. Whole served static assets are compared with the commit-bound build.

Normal private publication, provider access/entitlements, broker reconciliation, predictive validity and actual capital use remain unverified. The two snapshot sinks still have no shared transaction or writer-order guarantee. Portfolio-wide dispatch, complete-body reservations and the shared acceptance deadline are specified separately in [Portfolio quote collection bounds](portfolio-quote-collection.md). Per-response integrity does not establish a hard process deadline or total memory guarantee.
