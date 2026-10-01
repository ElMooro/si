# Tape Reader count validity — draft review record

Base: `4989e08a0d0444967ebb577c1a14901153bc009b` (2026-10-01).
Scope: existing tape-reader producer, its page, its enhancement chart and offline regressions.
No tape-truth, calendar, GEX, divergence, baseline-window, provider, schedule or capital-engine changes.

## Problem and behavior

Previously `max(today_n_trades, 1)` made absent or zero transaction counts look like
one transaction. With one million shares this manufactured a one-million-share
average, `block_ratio=10000`, a `BLOCK_PRINTS` tag and 25 score points.

Counts now require a finite JSON number, excluding booleans and strings, with a
nonnegative integer value. Reported zero remains zero with `reported_zero` status,
but cannot be a divisor. Missing, negative, fractional and nonfinite counts are
unavailable. Volume operands require finite, nonnegative numbers; measured volume
zero is valid arithmetic (`0 / positive_count = 0`), not missing volume. The existing
minimum-dollar-volume filter still excludes zero-volume sessions from ranked rows.

Baseline size uses the same returned bars, and is available only when every paired
volume/count is valid and every count is positive. Neither missing counts nor zero
counts are averaged into a favorable baseline. Nonfinite sums are unavailable.
No partial-subset size estimate is substituted. An independently valid current or
baseline average remains available even if the comparison cannot be made.

`block_ratio` retains its compatibility field name and existing one-share baseline
denominator floor; its unit explicitly says relative average size. A valid elevated
mean gets `LARGE_AVG_TRADE_SIZE`, never a block/participant/venue/initiator assertion.
Valid size scoring keeps the previous formula and 25-point cap. If comparison is
unavailable its score contribution is omitted: **0–25 fewer points, no rescaling**.
Volume (30), dollar-volume (25), range (20), thresholds and ranking filter are unchanged.

Frozen synthetic examples (unchanged price/range and baseline):

| Case | Old score | New score | New size comparison |
|---|---:|---:|---|
| 1m shares, 10,000 trades | 0 | 0 | 1.00× |
| 1m shares, 1,000 trades | 25 | 25 | 10.00×, descriptive mean only |
| 1m shares, count missing | 25 | 0 | unavailable |
| 1m shares, count zero | 25 | 0 | unavailable; reported count remains zero |
| 3m shares, count missing | 79 | 54 | unavailable; other terms preserved |

Size-only rows can now score zero and leave `top_loud_tape`/`n_with_data` under the
existing filter. The existing contract gate's minimum-row expectation may alert if
actual count coverage is poor. No padding or weakening of that gate is proposed.

## Consumer trace and compatibility

Rechecked at the base above and independently reviewed:

- `tape-reader.html`: table, score summaries, tags, rationale, sorting; uses null-aware
  strict formatting, sorts unavailable last, reports size coverage among top rows.
- `jh-enhance.js`: page opts into the new measurement contract and strict numeric
  values; other pages retain previous behavior. Footnote says published daily
  aggregates, not live/intraday tape. Unavailable chart values are omitted, genuine
  zero remains zero.
- Both display paths require `measurement_contract=tape-reader-activity.v2`.
  Old cached publications are withheld until the existing producer publishes the
  new contract; this is an intentional deployment-transition availability gap.
- `justhodl-page-ai`: current handler delegates to deterministic
  `page_explanation_store`, with source-inventory research permissions false.
- `justhodl-ask-desk`: generic public-feed discovery can use the packet as research
  narrative context; no participant/direction claim is added by this producer.
- `justhodl-strategist` and `justhodl-causality-scanner`: generic discovery was
  checked, not just literal filenames. Their extractors abstain on the actual
  new handler packet; regression tests freeze that behavior.
- `justhodl-smart-wake`, fanout and contract registries are lifecycle/observability
  references. No protected capital path was established. No sizing/order consumer
  or capital policy was changed. Repository tracing is not proof about external consumers.

## Retained evidence and validation

`aws/lambdas/justhodl-tape-reader/tests/legacy-before.py.txt` retains the complete
predecessor bytes, SHA256
`7399a403501e4a4bb649b33b7c62d8d08fcf8c2b9214bd574fbe224ad3fbaf39`.
Tests extract functions without importing SDK/credential initialization. The mocked
handler fixture freezes the date at 2026-10-01 22:00 UTC and session at 2026-09-30.

Reproduction commands:

```sh
python3 -B aws/lambdas/justhodl-tape-reader/tests/run_tests.py
node --test tests/tape-reader-counts.test.js
node tests/tape-reader-browser.cjs
node --test tests/*.test.js
python3 scripts/check_page_scripts.py
python3 scripts/gen_engine_wiring.py --check
DEPLOY_TARGETS=justhodl-tape-reader python3 tests/deployment/run_tests.py
python3 scripts/validate_lambda_sources.py justhodl-tape-reader
python3 scripts/validate_lambda_configs.py justhodl-tape-reader
python3 aws/ops/_preflight.py aws/lambdas/justhodl-tape-reader/source/lambda_function.py aws/lambdas/justhodl-tape-reader/tests/run_tests.py
python3 tests/test_page_script_gate.py
python3 tests/test_sovereign_site_assets.py
python3 tests/test_offline_pages.py
python3 tests/test_brain_public_boundaries.py
python3 scripts/guard_stub_lambdas.py
python3 scripts/check_secrets.py
git diff --check
```

Browser runner requires Playwright and Chromium; it accepts `TAPE_READER_CHROMIUM`
or uses installed `/usr/bin/chromium`. Every request is intercepted, including
fixture data; no public/AWS/provider data request escapes. It checks actual page
rendering, missingness, click/keyboard sorting, 390px layout, enhancement and legacy
withholding. This is local acceptance, not a production release claim.

Independent review reran the focused suites, checked source-call preservation,
verified the frozen predecessor, and found baseline count-sum overflow; that finding
was corrected and now has a regression. No blocking code findings remained.
The reviewer subsequently checked the full eight-file staged inventory and independently
reran browser acceptance, signing off for a draft PR only.

Completed local gates on 2026-10-01: 8 producer tests, 7 focused frontend tests,
2,461 full frontend tests, 1,018 deployment static tests plus 15 candidate-shell tests,
599 page graphs parsed with zero errors, wiring registry check (143 wired, zero
missing/stale), 6 page-gate tests, 3 sovereign-asset tests, 6 offline-page tests,
15 public-boundary tests, source/config validation, preflight, stub guard, secret
scan (zero findings), staged-inventory and whitespace checks. Browser acceptance
passed using installed system Chromium with all network intercepted. Missing local
Python test dependencies were installed outside the repository; no runtime dependency
or provider configuration changed.

## Existing architecture debt and release boundary

The engine still directly calls Polygon grouped-daily data rather than replaying a
retained normalized warehouse source. No new provider calls, source inputs,
subscriptions, schedules, SDK calls or engines are introduced. Tests freeze the
unchanged provider helpers and call sites. Entitlements and current count coverage
are unverified; no account data or private Brain was inspected.

The existing baseline selection (`n+5` candidate weekdays), liquidity filter,
non-size OHLC/VWAP operand handling and score calibration remain outside this slice.
These aggregate statistics do not establish participant identity, block prints,
genuine signed flow or an investment edge. Do not silently reinterpret their score
as execution eligibility.

Draft only. No merge, production release, vendor invocation or AWS invocation was
performed. After an approved release, existing schedule/receipt and live-page
acceptance remain necessary; local fixture checks do not establish deployed state.
