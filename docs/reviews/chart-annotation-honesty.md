# Historical annotation honesty — draft review

Historical markers can be located before the bars that made them appear. This
slice makes that limitation visible without changing any classifier, event,
marker, threshold, position, order or cache behavior. Only
`jh-chart-indux.js` changes production behavior: an active-study legend warning
and corrected help. Draft only; no release authorized by this review.

## Base and ownership

Base main: `f602f3fb4349ba492121016b0006508b6f74d786`.
Claims checked on that head. `S-codex#symdir1001a` owns Stage 502 and the next
legacy-volume counterexamples; engine/SymDir remain untouched. The prior cache
claim is released. No separate active claim names indux, vol-events or
distribution. This branch claims only indux warning/help and its offline evidence
as `S-codex#vtime1001h9`. No production changes to vol-events, distribution,
tape, campaigns, patterns, studio, chart.html or engine. Review-baseline source
hashes are retained in `tests/fixtures/chart-annotation/source-manifest.json`;
they are evidence, not an ongoing lock on the other owner's engine.

## Why the authorized static-warning fallback

`jhInduxLegend(ctx)` receives study settings and raw chart bars. Event help gets
only a kind, not an event identity. Neither receives a reliable evaluated-input
record. Display bars can be transformed and grouped, so assigning `lastBars`'s
last date to an event would be a guess. Marker constructors also drop additional
event fields, and crowding can hide events independently of their classification.

Accordingly, this slice does **not** add unused timing fields or expose a
per-event status. First availability stays unknown; there is no invented
`firstAvailableBarTime`, bar-close time or evaluated-through timestamp. Missing,
null, nonfinite and epoch-like dates cannot become an availability date because
the warning does not format one. The help explicitly discloses this limitation.

The new Timing button explains three separate concepts:

- Candidate: a pattern matches without a later reversal test.
- Coded confirmation: a specified response rule passed, not validated prediction.
- Retrospective selection: later bars, clustering or association affect which
  historical event is selected. This can coexist with coded confirmation.

It links to SC, CAPIT, BC, ABS and DIST explanations. Relevant study/event help
also carries the warning. Volume Tape and VSA are described separately where
rules differ; the prior invented minimum volumes, close locations and signed
flow/absorption assertions are corrected. Unrelated chart-pattern performance
claims are not validated by this slice.

Legend warning:

> Marker dates show event locations, not first availability. Later bars or corrections may revise annotations. Patterns are not predictive validation or measured signed flow.

The warning appears for enabled, non-hidden volume/cycle studies or active
DIST/campaign toggles; turning all of them off removes it. A native button with
an accessible name and visible keyboard-focus style opens existing help. No new
renderer hooks, observers, timers, network calls or classification passes.

## Retained timing counterexamples

All data below are deterministic invented fixtures, not market evidence.
Reproduction: `node --test tests/chart-annotation-honesty.test.js`.

| Case | Prefix result retained by tests |
| --- | --- |
| CAPIT / BC / volume ABS at index 60 | Absent with 69 bars; appears with 70, still positioned at 60 |
| SC at index 80 / VSA ABS at index 20 | Separate public minimum lengths 90 / 40 delay appearance |
| SC later rally at index 93 | SC at 90 absent through prefix 93, appears at prefix 94 |
| SC high-close candidate | Present at prefix 98, disappears at 99 as live-tail exemption expires |
| SC chain at 90, 98, 106 | Selected historical index changes 90 → 98 → 106 |
| Retained DIST high at 248 | No output at prefix 279; prefix 280 emits marker at 259 |
| DIST high 298, giveback 316 | Prefixes 319–323 move marker 318 → 319 → 320 → 321 → 322 |
| Reclaim at 320 | Appending it removes partial-window DIST; correcting that interior bar removes completed-window DIST |

The tests also append every bar on the same array, truncate, and correct every
scalar field. Complete events and marker projections are compared against the
retained predecessors, preserving labels, order, shape, color and position.
The source classifiers are deliberately not repaired: missing-volume CAPIT and
transport bugs remain separate. SC chain effects, structure clustering,
D-TOP associations and viewport crowding prevent equating event time, raw-rule
confirmation, final selection and visible appearance.

## Deferred detail wiring

A later separately reviewed change needs a selected event identity and the exact
input evaluation shared with the detail UI. Then add only consumed metadata:
conservative `evaluatedThroughBarTime`, `firstAvailableBarTime: null`, evidence
basis, and an explicit incomplete/criteria-met window state where available.
For SC, distinguish its close-response and later-rally paths. For DIST, use the
actual `gave + 6` completeness rather than trusting the clipped variable named
`confirm`. Keep retrospective selection separate. Preserve historical marker
locations. Never replay all prefixes in live rendering. None of that wiring is
silently implied to exist here.

## Validation and measured cost

- 11 focused tests pass, including actual chrome handlers under the DOM contract
  stub, activation/removal, help navigation/Escape, missing dates, complete legacy
  projection parity and zero extra classifier/distribution calls.
- Full frontend/worker suite: **2,417 passed**.
- Public page source gate: **599 graphs, zero syntax errors**.
- Wiring: **36 pages / 143 entries**, zero missing or stale.
- Page gate regression **6**, sovereign assets **3**, offline pages **6** pass.
- `node --check jh-chart-indux.js` and `git diff --check` pass.

Run `node tests/chart-annotation-benchmark.cjs`. Retained output:
`docs/reviews/chart-annotation-benchmark.json`, Node v24.19.0. Ordinary Node,
whole predecessor/current chrome source, 9 batches of 300 calls after warmup.

| Synthetic bar count | Prior legend median ms | Current legend median ms |
| --- | ---: | ---: |
| 1,000 | 0.04385 | 0.03742 |
| 10,000 | 0.02678 | 0.03391 |
| 11,536 | 0.02837 | 0.02938 |

Every run reports zero classification and distribution calls. The warning
checks the small study list, not the bar history. Timing variation includes
JIT/GC noise; no speedup or browser-performance conclusion is claimed.

**Remaining acceptance:** this is a DOM contract stub, not browser layout or
paint validation. Normal-sandbox Chromium launch failed in this environment
(crashpad/SUID-sandbox startup). No sandbox/TLS bypass or permissions workaround
was attempted. Independent managed-browser review must check 1440/390 widths,
warning visibility without excessive chart obstruction, keyboard Timing/help,
DIST/campaign toggle lifecycle and Escape before release.
