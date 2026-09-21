# Futures original-source qualification

The existing ES, NQ, CL, GC, SI, HG and NG universe is retained. VX remains
unavailable; no equity proxy, constructed contract, new subscription or account
endpoint is used. Catalog availability does not establish price-data entitlement.

Before native migration, ops 6009 retains the whole predecessor and actual
producer/consumer package evidence, then collects bounded original responses:

- Products and active single contracts for an explicit definition date.
- Schedules for the explicit product, venue and 90-day calendar window.
- Session bars for at most three contracts ordered by verified last trade date.

This is an expiry ordering, not a most-liquid-contract ranking. All catalog pages
must complete and every identity/date must qualify before selection. Missing or
conflicting metadata, duplicate identities and incomplete pagination yield no
selection. The first and last trading dates are not interchangeable with the
definition date or final settlement date.

Each request has an immutable original, acquisition time, secret-free request
identity and durable progress record. Response errors are retained privately;
redirects, altered scopes and reflected credentials are rejected. The audit has
168-request, 64-MB total, four-page-per-dataset and 4-MB-per-response bounds. A
claimed but incomplete audit cannot silently recollect on retry.

No return, curve, crisis, positioning or portfolio recommendation is produced
at this stage. Original price precision and all fields survive even if the
provider returns negative prices, zero settlement or nulls. Public heads and
schedules remain unchanged. Later native qualification must reconstruct the
calculations and preserve these limitations.

## Primary definitions reviewed on 2026-09-21

[Provider futures overview](https://massive.com/docs/rest/futures/overview)
defines aggregate timestamps as UTC nanoseconds, distinct from the exchange
trading date. A session can begin on the preceding calendar day.

[Contracts](https://massive.com/docs/rest/futures/contracts) supplies point-in-time
product identity, venue, trading dates, settlement date and tick sizes. The
implementation checks chronology explicitly rather than relying on the prose
inequality in the provider's `active` description.

[Products](https://massive.com/docs/rest/futures/products) supplies quotation,
currencies, unit of measure and quantity. These must establish conversions before
any contract-level exposure calculation; a price band proves none of them.

[Aggregate bars](https://massive.com/docs/rest/futures/aggregates) distinguish
the last trade price in a window from optional settlement. Volume counts
contracts; `dollar_volume` is a sum of price times size in quoted units, with no
contract multiplier. It must not be presented as dollar notional. Missing bars
can reflect no trading; returned pagination alone does not prove calendar
completeness or that the latest session has finished.

[Schedules](https://massive.com/docs/rest/futures/schedules) supplies product
session events and UTC event times. Cross-contract curves require a common
qualified session and a common price definition; an unmatched latest close
cannot establish backwardation or a supply shortage.
