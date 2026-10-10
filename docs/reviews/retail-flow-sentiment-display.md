# Retail Flow email sentiment display

The email renderer interpolated missing `bull_pct` as `bull None%` and accepted
arbitrary strings, booleans, nonfinite numbers and out-of-range values as
percentages. The change is confined to `render_email_html`: exact Python integer
or float values within the inclusive 0–100 range retain their previous string
format, including genuine zero. Everything else displays `bull unavailable`.
Checking the range before formatting avoids conversion/formatting failures on
arbitrarily large integers. Wrong-type sentiment is never interpolated into HTML.

This does not join the StockTwits sentiment map, repair producer coverage, change
ranking or analyst inputs/NET, modify the published JSON, or change sending,
recipients, schedules, idempotency or authority guards. Other existing HTML
interpolation is outside this narrow sentiment repair. No live email-client
rendering or production delivery acceptance is claimed. Secretary PR40 is separate.

## Offline evidence

Run `python3 aws/lambdas/justhodl-hot-stocks-digest/tests/run_tests.py`.
The runner retains the seven existing retail-authority regressions and adds six
dependency-free display/scope tests. They extract only the actual renderer AST;
they do not import boto3, call the handler or contact any service.

- 1,012 valid-value whole-HTML comparisons against the original renderer,
  including zero, negative zero, finite endpoints, adjacent floats and decimals.
- 21 invalid-value cases plus an absent field change only the sentiment label,
  including booleans, containers, numeric strings, NaN/infinities, range violations
  and positive/negative integers with 401 and 10,001 decimal digits.
- Five markup/text payloads and two hostile wrong-type objects cannot be coerced
  or injected through sentiment; a 12-row mixed packet preserves valid neighbors
  and all existing NET text. Inputs remain unchanged.
- All ten nonrenderer functions and every other module-level statement match
  baseline AST fingerprints. These include the handler, sender, analyst input,
  fallback, hot-list builder and recipient constants. The fixtures contain no
  recipient identifiers or private messages.

The renderer fixture and fingerprints are pinned to main
`2360dc62e8ff983e4ed42795bb3fe94a0ba22200`; the renderer fixture was checked against
that Git source. Source/config validation, repository preflight, secret scanning,
publication-boundary and deployment static/shell gates are run locally. Deployment
tests use dummy credentials, disabled metadata and an outbound-network blocker;
their simulated deployment actions are mocks. Exact results accompany the draft PR.

Independent exact-head acceptance is required before this branch proceeds beyond
draft review. This draft does not authorize merge or release.
