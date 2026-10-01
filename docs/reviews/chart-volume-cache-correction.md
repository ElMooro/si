# Chart volume cache correction — draft only

## Problem and exact scope

`jh-chart-vol-events.js` cached results solely by array identity. Correcting
OHLCV in an existing bar, appending bars or truncating the same array reused
obsolete events. A synthetic CAPIT candle remained marked after correction to
a flat, ordinary-volume candle.

The fix retains one exact snapshot of the six scalar inputs read by the
classifier and markers: time, open, high, low, close, volume. A cache hit now
requires the same array, length and scalar values. `Object.is` keeps unchanged
NaN cacheable without numeric coercion. Only misses allocate a new snapshot;
all existing classifier output and marker construction is reused unchanged.
Market-chart timestamps are scalar epoch seconds in the inspected consumers.
Arbitrary nested timestamp objects are outside this chart input contract.

No classifier threshold, eligibility flag, marker priority, forward-confirmation
rule, transport, schedule, capital/risk setting or provider request changes.
Historical SC/structure repainting and the previously identified price-cleaner
behavior remain separate issues. This fix promises equality to fresh computation
on the same received bars, not causal/predictive validity of those classifiers.

## Source and ownership

Branch base: `fcc9d8ac45575c5f48a81e51635b764c59333ccf`.
Predecessor last path commit: `f3b9557695282049e9e82862b4a5e7d9d954cb17`.
The full original is retained byte-for-byte in
`tests/fixtures/chart-volume-cache-predecessor.js.txt`, SHA-256
`5d524e4197cb3b8790b70766b67d6e08434f07ac2ce08a733b95a7af33ab39d0`.
The preservation test restores only the changed cache block and requires the
whole remainder to match that original exactly.

Current claims and all files in open PRs 22, 25, 26, 27 and 34 were inspected
before editing: no overlap with `jh-chart-vol-events.js`. Our draft-only claim
S-codex#vcache1001r8 is published on this review branch, not main. The SymDir
owner S-codex#symdir1001a remains active in the chart engine/observation work;
`jh-chart-engine.js`, `chart.html`, transport and SymDir files are untouched.
RVOL/zero-baseline production repair remains blocked for owner handoff.
The separate offline counterexamples are preserved in draft PR #35.

## Complete inspected consumer trace

- `jhVolumeTape` / `jhVolEvents` / `jhVolEventTable`: public wrappers in the
  changed module, returning events and Lightweight Charts marker objects.
- `jh-chart-engine.js`: volume histogram colors and event overlays;
  tape readouts become `setMarkers` inputs. No decision-state write.
- `jh-chart-tape.js`: Livermore/Wyckoff/accumulation/distribution markers and
  descriptive trend/panel text through `jhTapeRead` / `__jhTapeReadRaw`.
- `jh-chart-studio.js`: wrappers toggle indicator visibility.
- `jh-chart-patterns.js`: consumes tape markers for displayed pattern overlays.
- `jh-chart-distribution.js`: consumes historical TOP events for D-TOP markers.
- `jh-chart-campaign-marks.js`: consumes SC/BOTTOM/EOA for chart markers and
  intraday projection. It reads `data/bottom.json` separately; no event writes
  are sent upstream and this draft adds no requests.
- `jhStructureKinds`: exported constant; no additional repository consumer found.

The script is loaded by `chart.html`. Repository search found no backend,
order, sizing, allocation or money-decision consumer of these exports. This is
a source-level boundary check, not live runtime verification. Existing display
language and unrelated proxy/forecast qualification issues remain unchanged.

## Offline validation and bounded cost

`node --test tests/chart-volume-cache.test.js` exercises the pinned failure,
all six fields, old interior bars, append/truncate at every prefix, splice/order
changes, repeated wrapper calls, distinct arrays, NaN and the real tape consumer.
Every corrected result is compared with a fresh instance of the original
classifier; no source acquisition or altered classifier acts as the oracle.

`node tests/chart-volume-cache-benchmark.cjs` emits repeatable local cost evidence.
Results are retained in `chart-volume-cache-benchmark.json`. Cache hits add at
most six exact scalar comparisons per bar: O(N), no snapshot allocation on a
hit. Retained snapshot storage is 6N scalar slots for only the most recent
series (60,000 slots for 10,000 bars). Misses add an O(N) snapshot pass to the
existing classifier cost. These are algorithmic bounds; timings are synthetic
local measurements, not mobile/browser guarantees. No additional feed, AWS,
provider, schedule or operational spending is introduced.

Recorded local measurements: cache-hit median 0.997 ms for 1,000 bars and
9.914 ms for 10,000 bars (30 iterations each). Correction plus full existing
classification median 92.4 ms / 1,016.236 ms (five iterations each); these
include the original scans and do not isolate incremental snapshot overhead.
Repeated large-history renders may therefore incur visible cost; real-browser
performance remains a review consideration rather than a claimed acceptance.

Validation on the branch based on `fcc9d8ac4`: 12 targeted cache/consumer tests
passed; the full frontend command `node --test tests/*.test.js` passed 2,378
tests with no failures/skips (25.6 seconds); syntax parsed 599 public page
graphs with no errors; wiring reported 36 pages / 143 entries with zero
missing/stale entries. `git diff --check` passed. Main was checked again before
commit and remained at the branch base; no concurrent source conflict found.

Release still requires independent exact-head review and owner authorization.
No merge/deploy is authorized by this draft. Existing page syntax, wiring,
frontend and source-preservation gates must pass before review handoff.
