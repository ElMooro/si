# Portfolio holdings risk 2.0.1

This is a private research calculation over a complete identified snapshot. It
does not establish account reconciliation, forecast skill, suitability or sizing
authority. Both `sizing_eligible` and `may_recommend_trades` remain false.

## Instrument and input meaning

The supported model remains USD cash equities/ETFs with multiplier one. The
acquisition adapter and exposure calculator use the same typed identity check.
A boolean ticker cannot become the text ticker `TRUE`, and a boolean multiplier
cannot become numeric one. Missing currency/multiplier retain the explicitly
documented legacy USD/one interpretation; explicit other values are rejected.

Only an explicit empty positions array means `no_positions`. A missing or
malformed list produces `INCOMPLETE`, with `n_positions: null` when no count is
known. Malformed records within a list remain represented in the complete frozen
input and as unavailable scenario rows with their original zero-based index.
There is no fabricated zero loss or silent conversion to an empty portfolio.
Invalid sector types cannot be used as dictionary lookup keys.

Malformed capital-book reason metadata makes NAV unavailable. Independently
supported holdings observations may still be shown, with their dollar/gross
exposure basis distinct from account NAV. This guard does not certify the
remaining capital contract or reconcile any actual account.

## Scenario aggregation

For each valid marked position, the hypothetical contribution is its signed USD
market value multiplied by the explicitly supplied sector shock. No missing
sector receives an index substitute. Incomplete position or shock coverage
withholds the total.

The total uses `math.fsum` on unrounded contributions and is rounded once for
display. Per-position display rounding is separate. For example, one hundred
one-dollar lots under a -26.5% shock total -$26.50. Summing one hundred displayed
-$0.27 contributions would incorrectly yield -$27.00. Splitting a holding into
identically classified lots must not change its aggregate consequence.

The packet's `rounding` field and page disclose this rule. Arithmetic remains
binary64, not an exact decimal accounting ledger. The shocks remain hypothetical;
the historical-looking scenario names do not establish calibrated historical
replay or event probabilities. Dividends, financing, fees, options, FX and
liquidity losses remain outside the holdings model's supported scope.

## Version and evidence

The UI accepts the reviewed 2.0.0 and 2.0.1 schemas and preserves research-only
permission checks. A frozen bundle binds the whole inputs, compiler identity and
output hash. Changing the compiler does not relabel or overwrite an older
bundle: incompatible replay is explicitly rejected. Recalculating invented
retained inputs produces a new compiler-bound bundle.

The publication-ordering protocol, source snapshot identity and predecessor
retention are separate contracts and remain unchanged by this calculation repair.
Exact native package/runtime/schedule and complete static asset checks establish
deployment acceptance. Normal private publication and actual-account correctness
remain unverified by that read-only acceptance.
