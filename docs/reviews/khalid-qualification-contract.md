# Khalid qualification contracts — draft, no release

Baseline: `96d10831a0bdaf190029d23fb715e7ccc991964e`.

The shared sniper panel previously fetched browser bars, ran a different checklist
and assigned its own SNIPER badge. This draft replaces that display path with two
separate, versioned backend contracts. It does not change any existing action,
threshold, risk permission, selection, confidence calculation, plan or sizing.
It establishes neither a valid historical strategy nor profitable performance.

## Contracts and exact scope

- `aws/lambdas/justhodl-khalid/source/scoring.py` adds only
  `backend_gate_observation`, recording the 13 already-evaluated native gates.
  Its original statements and decision outputs are checked against the baseline.
- `source/qualification.py` projects final `opportunity_radar` actions into
  `qualification_evidence`. Final discovery/lifecycle action takes precedence
  over the earlier scoring action. Discovery-only rows lack execution qualification.
  Existing readiness PASS means the published action is READY_TO_SNIPE with
  available native evidence and fresh required sources; FAIL means not ready.
  Missing execution or source freshness is UNAVAILABLE, while the original
  reported action is preserved for inspection.
- `source/lambda_function.py` adds one import and one projection call after all
  decisions. Its original source digest remains pinned by parity tests.
- Requested-strategy qualification uses PASS/FAIL/UNAVAILABLE/UNRESOLVED vocabulary
  but **all 23 currently unconfirmed definitions remain UNRESOLVED, complete=false**.
  A future approved definition requires another version. No old browser thresholds
  have become approved requirements. 3m, exact S&P identity, flow window/lag,
  point-in-time universe/exclusions, resilience and optional/required roles remain
  unresolved. SPY is not substituted for the index; legacy scores/holdings are not
  creations/redemptions or signed trades. Native provider-flow projection is untouched.
- `jh-khalid-sniper.js` renders the contracts with text nodes, ten-row pagination,
  filters, keyboard-operable details and responsive columns. It fetches only the
  existing `/data/khalid.json` publication. No browser bar/vendor fetch remains.
  Its frozen legacy pure measurement function remains available for offline callers
  with `qualification_authority=false`; no UI readiness path consumes it.
- `khalid.html` and `chart.html` only change this shared script's version reference.
  `jh-chart-tvrail.js` is the other mount caller and is unchanged. On chart mount,
  an available exact `jhActive` ticker filters the backend candidates. An absent
  ticker is unavailable; no local scorer supplies qualification. When no active
  chart identity exists, the shared backend candidate inventory is displayed.
  `khalid.js` and all main risk/action controls remain unchanged.

Ownership was checked against current claims and recent target history before
editing. The older provider-flow claim's PR13 was already merged at
`f701d7d52417b140b0e0f63aa43101c82a5873c7`; its projection is unchanged. Current
symbol-directory publication work has no changed file in this draft. No chart
engine, chart rail, ETF flow semantics, holdings UI or holdings coverage is edited.

## Clocks, definitions and display coverage

The new packet has explicit schema, generated/expiry clocks, revision, display
freshness scope and historical-validation limitation. The **26-hour limit is a
publication-display rule only**, not a new capital/risk policy. Source status,
source as-of, current object modification and source SLA are exposed separately.
Supporting producer inputs (including merged sources) are retained. Exact per-field
lineage through merged rows is explicitly incomplete. Historical per-field
effective/availability times are null, never inferred from
publication time. The browser also checks source age at display time for a PASS.

Shared backend definitions and the unresolved requested checklist are transmitted
once. Native values reference their stable criterion IDs; each contract explicitly
references its definition collection and shared clock policy. The UI expands these
references, showing every new field as a heading, criterion detail, clock/provenance
list, candidate identity detail or publication-provenance detail. The standalone
`backend_gate_observation` on execution rows is the same observation expanded in
the shared panel (its reported action is shown as pre-lifecycle scoring action).
No private Brain/account input is fetched or exposed.

The browser rejects unsupported versions, missing definitions/gates, duplicate or
mismatched identities, revision bindings, inconsistent actions, future/expired
publication clocks and purported PASS with stale source clocks. One invalid record
withholds the packet rather than rendering a partial qualification badge. Revision
is a producer content identifier and cross-record binding; it is not a signature.

## Validation and cost

- 70 native Khalid tests pass. New tests cover 130 paired scorer cases across five
  asset classes/two permission states, full-handler parity, lifecycle downgrades,
  stale/missing execution evidence, missing historical clocks and unresolved rules.
- Existing provider-flow preservation test retains its original baseline digest;
  only the separately-tested additive qualification lines/field are excluded.
- 2,250 frontend tests passed; focused current-script checks pass.
- Both actual page HTML/CSS contexts, actual shared renderer, actual Khalid
  controller and chart workspace module were tested at 1440 and 390 pixels with
  invented data and every request intercepted. Keyboard navigation, filters,
  readiness toggle/focus, details, pagination and missing chart identity were checked.
  Unrelated scripts/chart drawing were omitted. This is isolated draft UI evidence,
  not a production or upstream-data acceptance claim.
- Page-script parse: 599 public graphs, zero errors. Engine-wiring check: clean.
  Python preflight and exact staged-inventory checks apply before commit.

No new feed, entitlement, schedule, vendor request, native invoke or infrastructure
is introduced. Each mount makes one public static packet request. The draft removes
the prior browser universe/bar scan. Invented local projection fixtures added
21,193 / 277,270 / 2,609,170 compact bytes for 1 / 100 / 1,000 candidates; fixture
construction plus projection took about 0.001 / 0.062 / 0.702 seconds here. These
are local measurements, not production costs or capacity evidence. Rendering is
bounded to ten records per page; large real packets still need review before release.

Reproduce:

```sh
PYTHONDONTWRITEBYTECODE=1 python aws/lambdas/justhodl-khalid/tests/run_tests.py
node --test tests/*.test.js
PLAYWRIGHT_CHROMIUM_EXECUTABLE=/usr/bin/chromium node tests/khalid-qualification-browser.cjs /tmp/kqc-browser
python scripts/check_page_scripts.py
python scripts/gen_engine_wiring.py --check
```

Independent exact-head review is required. This PR is draft only: no merge,
deployment, policy promotion or requested-strategy definition approval is included.


## Independent-review corrections

The first review rejected head `646bd869e` for two display-contract defects.
This revision changes no backend file, action, threshold, sizing or payload.

- The panel now schedules revalidation at the earliest applicable PASS source
  deadline or publication expiry, rather than waiting only for publication expiry.
  Source age equal to its existing SLA remains valid; the first later millisecond
  is unavailable. Publication expiry is exclusive. Visibility restoration,
  persisted pageshow and window focus revalidate immediately, covering suspended
  or throttled background timers. Remount and terminal invalidation clean up
  old timers/listeners; stale PASS cards are removed without user interaction.
- Criterion values must agree with their declared backend type and status:
  measured numbers are finite numbers, counts are nonnegative integers, booleans
  are actual booleans matching their PASS/FAIL state, and unavailable values are
  null. Requested unresolved criteria retain null values/units. Unit overrides,
  string coercions and missing/invalid measured values are rejected. Numerical
  strategy thresholds are still evaluated solely by the backend.

Mutation regressions cover every native criterion and requested criterion, valid
missing/FAIL states, and deliberately contradictory numerical threshold outcomes
that demonstrate there is no second strategy evaluator in the browser. Four real
page-context browser cases each exercise source deadline, publication deadline,
background visibility resume, persisted pageshow and focus (20 freshness cases),
in addition to the existing keyboard/mobile controls. All requests are intercepted.

The reviewer reported an unchanged worker-ordering combined-run failure followed
by an isolated 21/21 pass. No worker/test changes are included here. This revision's
full combined frontend run passed all 2,250 tests; that does not establish that the
separate intermittent test behavior is resolved. Native tests remain 70/70; page
parse (599 graphs), wiring and diff checks pass. Payload cost remains 2,609,170
compact bytes per 1,000 invented candidates; no new requests or infrastructure.
