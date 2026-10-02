# Warehouse chart volume codecs

The public OHLC adapter retains its existing `value` output field for volume.
This field is specific to an OHLC response; a generic observation's `value`
must not be interpreted as volume. Chart consumes this legacy field only on a
warehouse-identified OHLC row with a separate close and no named volume field.
Named invalid, conflicting or null volume never falls back to that field.
Equality between price and volume does not determine either field's meaning.

Warehouse arrays use ordinal 5. Objects may contain `volume`, `value`, `v`,
`vol` or `Volume`; every present alias must be a finite nonnegative number and
all must agree. An absent, invalid or conflicting quantity produces null.
Zero is a measured value. Yahoo's quote-volume array uses the same typed rule.
This does not establish that every warehouse producer already meets the rule.

Binance's [documented kline tuple](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market#klinecandlestick-data)
places volume at ordinal 5 as a decimal string; ordinal 7 is quote-asset volume.
Only this codec decodes unsigned decimal strings. Blanks, booleans, hexadecimal,
exponent strings, nonfinite values and positive decimals that underflow to zero
are unavailable. The current numeric compatibility path remains supported.
JavaScript output is binary floating point, not a decimal-exact accounting unit.

Coarser bars sum all supplied constituent volumes. One unavailable constituent,
overflow, or a positive addend wholly absorbed by rounding makes the aggregate
unavailable. These checks do not prove that every expected interval was supplied.
The primary crypto row still owns all fields on overlap, including null volume;
neither a supplementary row nor a magnitude heuristic can replace that null.

Original unit identity, source ancestry, adjustment consistency, duplicate-row
conflicts, calendar completeness and price validation remain separate open work.
No source freshness, predictive validity, sizing permission or live data delivery
is certified by these codecs. Tests use complete invented frames and intercepted
requests; deployment acceptance checks the exact Worker package and receipt.
