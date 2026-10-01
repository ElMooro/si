# Chart volume correctness: first-slice coordination draft

Status: offline evidence only. No production repair, publication, or deployment.

## Source and ownership

Checked main `2cb92648c2c886d5268cfb832669f78d856daea5` on 2026-10-01.
The original audit checkout was `7e7044328ccdf49cf1cbb2cd68314817eb427109`;
the draft branch is synced to main `0d9074f348a8e16bdb4e36882952596080a7476d`.
The retained two function bodies match both heads. Their containing current-main
file SHA-256 is recorded in `tests/fixtures/chart-volume-calculation-predecessor.json`.
The source was read over GitHub; tests require no network or provider access.

`docs/SESSION_CLAIMS.md` at the checked main still assigns Stage 501 unavailable
scalar diagnostics, UTC group calendars and stale watermark repair to
**S-codex#symdir1001a**, with full validation and exact release pending.
Commit `2cb92648` (2026-10-01 07:33:12 UTC) changes `jh-chart-engine.js` for that
work. Earlier engine changes `2e113edac` and `4b22f8a7` belong to its cache/scalar
sequence. This is a current shared-file conflict, not a stale historical claim.

**S-codex#kperf1001q7** names `khalid.js`, `jh-khalid-sniper.js`,
`jh-chart-tvrail.js`, script references and tests for PR33. It does not claim
the RVOL primitive. Follow-up verification of `0d9074f34` confirms its owner
moved only that PR33 row to Done; the SymDir Stage 501 row remains active.
No explicit current claim names `jh-chart-vol-events.js`
or `jh-chart-distribution.js`; their latest path commits are `f3b95576` and
`ce583682`, respectively. Absence of a named claim is not permission to expand
this slice into those event engines.

Production edits are stopped under the user's shared-file instruction.
Coordination target: **S-codex#symdir1001a / jh-chart-engine.js**, specifically
`rvolSeries` and its existing RVOL oscillator consumer. No takeover or claim
write was made.

## Bounded proposed repair after owner handoff

Change only the existing chart RVOL pane calculation: current volume divided
by the arithmetic mean of exactly 20 preceding observations. Exclude the
current bar and all later bars. Require 20 preceding observations; do not
silently shorten the window. A genuine zero numerator is measured zero when
the denominator is positive. A zero denominator is unavailable. Missing,
boolean, negative or nonfinite operands are unavailable, not zero and not
grounds to borrow older observations. These checks cannot recover volume
already converted to zero upstream; transport repair remains separate.

Keep the existing `rvol` indicator ID, dimensionless multiple, chart timeframe,
and 1x/2x reference lines. Represent unavailable points using the existing
chart library's time-only whitespace shape, subject to offline browser
acceptance; verify the latest pane value shows unavailable rather than
retaining the previous number. Do not change quote RVOL, candle coloring,
event thresholds, backend metrics, units, eligibility flags or capital policy.
This is a descriptive chart correction, not a new signal or research engine.

OBV unchanged-close handling is a real separate defect, retained as a
counterexample, but **deferred**. OBV is cumulative signed price-change volume;
RVOL is a trailing unsigned ratio. There is no coherent shared primitive that
requires changing both in this first slice.

## Consumer/authority trace

At the checked main, `rvolSeries` is local to the chart closure (line 646).
Its sole invocation is the RVOL oscillator (line 2808): `addO` sends the result
to `addLineSeries().setData()` and formats the latest numeric pane value.
`obv` is likewise local (line 690), called by its oscillator (line 2755).
Repository search found no export or decision-engine consumer of these two
functions. Neither function sends a request or writes decision state.
This establishes the inspected source boundary; it is not a live runtime
or deployment verification. Other similarly named backend volume metrics
are separate implementations and must remain unchanged.

## Counterexamples and validation

Run `node --test tests/chart-volume-counterexamples.test.js`.
The suite intentionally proves the pinned predecessor defects and separately
checks an independent proposed-window oracle. Passing it **does not** mean
production is repaired, and the oracle is not a production candidate.

- Twenty volumes of 100 then 1000: existing pane 6.896551724x; prior-only 10x.
- Twenty zero baseline volumes: existing pane reports 1x for current zero,
  or 20x for current 100; both denominators should be unavailable.
- Twenty observations alone: existing pane emits at index 19; a complete
  prior-20 denominator is not available until index 20.
- Three unchanged closes with volume 100: existing OBV ends at 200 instead of 0.

Additional oracle cases cover genuine zeros, invalid inputs, exact windows,
future append invariance, volume-unit scaling and numeric overflow. A later
production repair must run these semantic checks against the actual repaired
function and add native-series/last-value browser acceptance.

Required release gates remain unchanged: `python3 scripts/check_page_scripts.py`,
the frontend behavioural suite, `python3 scripts/gen_engine_wiring.py --check`,
and relevant chart observation preservation/integration checks. Run on the
owner-approved latest head; this offline draft is not a substitute for them.
Use mocked/retained data for browser checks so opening chart does not trigger
provider requests. No provider, transport, SymDir, `stripMixInBars`, event
classifier, schedule, capital/risk or deployment change belongs in this slice.

Draft validation on the unchanged checkout base: all 39 targeted tests passed,
including eight new offline counterexample/contract cases; page syntax parsed
599 public graphs with zero errors; wiring reported 36 pages / 143 wired
entries, with zero missing or stale registry entries. The complete Pages
frontend command, `node --test tests/*.test.js`, passed 2,334 tests with zero
failures or skips (18.8 seconds). These checks apply to
the local evidence draft, not the newer owner's production release.

After syncing the draft to `0d9074f34`, the full frontend gate passed **2,378
tests** (22.9 seconds), page syntax again passed all 599 graphs, and wiring
again reported zero missing/stale entries. The three added paths do not exist
on that main base, so this draft has no production-file or existing-file conflict.

## Cost and handoff

Current draft: three added offline test/fixture/documentation files; zero
production files; zero provider requests, AWS work, new feeds or runtime cost.
Expected production scope after coordination: one existing RVOL primitive,
its tests and (only if required) its latest-value unavailable display. A rolling
implementation can retain O(N) runtime and bounded window state. No additional
chart data fetch is required. Wall-clock implementation/review cost is not
measured; ownership resolution and browser acceptance remain outstanding.

This evidence-only draft is prepared for a draft PR. No production repair,
merge or deployment is represented by it. RVOL production work still requires
the SymDir owner handoff.
