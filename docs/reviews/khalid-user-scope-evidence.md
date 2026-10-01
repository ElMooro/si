# Khalid user scope observations — draft, no release

Baseline: `064e4fe5403b073176615291d3c0ce0c792f1692`.

The inherited 23-item scanner contract bundles asset scope and exclusions with
unconfirmed historical and technical definitions. It includes scanner definitions
that were not explicitly supplied as user requirements. This draft preserves that
contract byte-for-byte and adds `user_scope_evidence` for the stated asset scope,
biotechnology exclusion, and existing stock-cap convention only. No full strategy
match, historical performance, or current eligibility certification is claimed.

## Contract and source boundaries

`aws/lambdas/justhodl-khalid/source/user_scope_evidence.py` is a pure projection.
The handler calls it after the unchanged qualification projection and every
selection, lifecycle, risk and sizing decision. It receives already-loaded active
feeds. It performs no source fetch, account access, scoring, or native input repair.
It does not add `mcap` to scoring inputs.

Native Fortress board/etfs/ledger and Katlin picks/watch containers bind a source
row to the candidate's exact ticker **and** asset class and named discovery source.
Known cross-class collisions, duplicate native rows and missing expected sources
are unavailable. No symbol rewriting, external ticker joins, economic-exposure
inference or guessed metals classification is used. Native stock/ETF/crypto
instrument observations can pass the instrument check; mixed commodity/bond/
country discovery labels cannot silently become native instrument types.

Biotechnology FAIL requires exact `Biotechnology` from a dated source-bound stock
industry. Other accepted labels are limited to 56 exact existing Fortress IND_ETF
keys, copied from baseline and checked against that unchanged producer in a test.
Only the vocabulary is reused, never the ETF mapping. This is an explicitly
incomplete product vocabulary, not a newly invented complete taxonomy: unknown,
case-drift, healthcare sector labels and out-of-vocabulary industries stay
UNAVAILABLE. Other healthcare labels in that vocabulary can pass this observation.
Fund holdings and crypto applicability stay UNRESOLVED.

Market cap reads only the native producer's USD field (`market_cap` for Fortress,
`mcap` for Katlin). No value coercion, heuristic unit conversion, or AUM substitution
is added. Values must be positive finite numbers no larger than the exact JSON
integer range; inconsistent supplied buckets are unavailable. Existing SMALL
means $300M <= cap < $2B, MID $2B–$10B, LARGE $10B–$200B, MEGA >=$200B. Those are
**product conventions, not a user-chosen cutoff**. Below $300M remains UNRESOLVED,
including MICRO/NANO; non-stock cap exclusion applicability also remains
UNRESOLVED. The UI offers no combined “user-qualified” or “strategy match” badge.

Producer identity, artifact, exact publisher clock, source health and the existing
84h Fortress/36h Katlin limits must agree. Katlin additionally requires its own
FRESH research status and numeric 36h age declaration; permission refresh cannot
renew research. Original research time is used. Finviz/census snapshot times must
be explicit and no later than research. Cap has an undocumented per-row choice
between those two upstream origins, so both clocks must be retained and qualified
as dated snapshots. They do not acquire a new freshness SLA. Historical effective
and availability times stay null. Old upstream snapshots are dated observations,
not independently freshness-qualified current measurements.

Rows contain only a candidate index, native row/source references and three compact
`[status,value,reason_id]` tuples. Definitions, vocabulary, reasons and clocks are
shared once. The legacy qualification revision and index bind candidate identity.
Revision binding is not a cryptographic signature.

## Display and preservation

`jh-khalid-sniper.js` validates and displays the independent optional contract.
An absent, invalid or expired scope packet does not withhold legacy backend
qualification. Scope expiry revalidates independently on its deadline and on
focus/visibility restoration. Values and statuses are displayed as supplied by the
backend; the browser does not re-evaluate cap thresholds or biotechnology rules.
It does validate value domains, source references, identity and clock bindings.

Default is all candidates in their existing order. Optional display filters show
biotech exclusions, existing SMALL convention failures, or unknown/unresolved
observations, with overlapping counts and a keyboard-operable reset. These filters
never remove backend data or change actions, risk, selection, confidence or sizing.
Both existing mounts use the same snapshot request; no new browser endpoint.

Only the sniper script cache token changes in `chart.html`/`khalid.html`.
Two existing chart HTML preservation tests normalize that exact token before their
unchanged full-document assertions; their preservation meta-test strips only that
new normalization before comparing the retained test sources. Chart engine/SymDir, tape and risk display
implementations are untouched. Existing backend parity tests strip the one new
projection line/field while retaining all prior pinned digests.

## Evidence and limits

See `khalid-user-scope-evidence.json` for public artifact hashes, timestamps,
benchmark counts and four isolated browser receipts. Public fixtures stay outside
the repository; the checked-in browser fixture is explicitly invented.

The real **3,643-candidate** 2026-10-01 11:25:50 UTC publication adds **542,067**
compact JSON bytes, below 1 MB (83.315 ms projection in this local measurement).
The original packet serializes identically before and after projection. Counts:

| Observation | PASS | FAIL | UNAVAILABLE | UNRESOLVED |
|---|---:|---:|---:|---:|
| Native instrument | 2,913 | 0 | 730 | 0 |
| Biotech exclusion | 1,277 | 250 | 1,969 | 147 |
| Existing cap convention | 888 | 593 | 741 | 1,421 |

These are source-snapshot observations, not full-strategy opportunity counts.
The separately captured Katlin packet is newer than the audited Khalid packet;
its source status remains unavailable. The benchmark does not reconstruct missing
historical Katlin inputs, repair clocks, or claim production counts after release.

Backend suite: 81 checks, including entire serialized-output equality with the new
field removed and exact legacy qualification equality. Tests cover huge integers,
invalid classes/units/buckets, exact labels versus healthcare/unknown/case drift,
source mismatches/collisions, native stale health, and preserved uncertainty.
All 2,543 frontend tests pass in the final combined run. Page parsing covers 599
public graphs with zero syntax errors; engine wiring and Python preflight pass.
Scope frontend tests cover values/identity/clocks, overlapping filter counts,
no second threshold evaluator and legacy independence. Four browser cases use real
page HTML/CSS and modules with intercepted invented inputs at 1440/390 widths:
keyboard filter/reset, details, default all, counts, no horizontal overflow,
independent scope expiry and one public snapshot request. Existing four-case
qualification browser suite also passes. These are not live release acceptance.

Independent working-tree review found two defects (unknown industry labels and
native Katlin stale health); both were repaired with regressions. Exact committed
head review is required before any release. Draft branch only: no merge, dispatch,
AWS/vendor/private calls, schedules, orders or capital-policy changes.

Reproduce offline:

```sh
PYTHONDONTWRITEBYTECODE=1 python aws/lambdas/justhodl-khalid/tests/run_tests.py
node --test tests/*.test.js
PLAYWRIGHT_CHROMIUM_EXECUTABLE=/usr/bin/chromium node tests/khalid-user-scope-browser.cjs /tmp/scope-browser
PLAYWRIGHT_CHROMIUM_EXECUTABLE=/usr/bin/chromium node tests/khalid-qualification-browser.cjs /tmp/legacy-browser
# Directory must contain the three captured public JSON files; this does not fetch.
python tests/benchmark_khalid_user_scope.py /tmp/khalid-scope-audit
python scripts/check_page_scripts.py
python scripts/gen_engine_wiring.py --check
```
