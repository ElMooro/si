# Katlin OOS correction — draft, release held

Baseline: `15698384ce4b0709b37dba24968ff45436d37ac5`.
User approval: October 1, 2026, 03:32 UTC, for the specifically discussed leakage
correction after independent review. No deployment is authorized by this draft.

## Corrected scope and consumer trace

The old OOS split selected training **entries** before the 60% boundary, including
63/126-session labels ending in the test period. Historical label availability
and decision instants are not retained in those observations. Purging nominal
overlap alone therefore cannot establish a historically available training set.

This correction publishes `oos={}` and a versioned `oos_validation` block with
per-horizon boundary/session information, crossing counts, missing-availability
counts and zero eligible training observations. It does not invent timestamps
from dates or trust arbitrary row-added clocks. OOS fitting remains unavailable
until a separately reviewed retained-availability input contract exists.

**The original broad trace needs an important distinction:**

| Path in `aws/lambdas/justhodl-katlin/source/lambda_function.py` | Role |
| --- | --- |
| `run_backtest` → full-history `feature_stats` | Intentional retrospective priors, unchanged. These are not the OOS training set. |
| `run_backtest` → old `oos` | Diagnostic split-fit/decile statistics affected by label-boundary leakage. Now withheld. |
| `load_feeds` → `F['backtest']` → `learned_prior` → handler | Reads full-history priors into `alpha_prior`, expected excess and learned pillar; unchanged. |
| `gates_and_tier`, `composite`, `trade_plan`, `build_basket` | Live history thresholds, score, eligibility, plan and model weights; functions/constants unchanged. |
| `validation_summary` → `katlin.html:renderValidation` | Desk diagnostic presentation. Legacy OOS metrics are also withheld on the next normal daily rebuild, while cohorts and priors remain visible. |
| `regime_history`, `barbell_base_rate` | Descriptive cohort diagnostics, unchanged. |
| `justhodl-khalid/source/lambda_function.py:_katlin_rows` | Downstream picks/watch adapter; unchanged. |

Repository inspection found no direct OOS-statistic input into scores or weights.
The permission refresh path may continue carrying an already-published old
validation section until a full daily rebuild. No claim of live correction is
made before that scheduled output is observed.

Only existing functions `run_backtest` and `validation_summary` change, plus a
pure `oos_boundary_evidence` helper. All other function/class ASTs and all
top-level assignments match the baseline. No config, schedule, source acquisition,
threshold, allocation formula, action permission or page source changes.

## Paired frozen-fixture evidence

`katlin-oos-paired-evidence.json` records an invented deterministic fixture:
60 stocks, 1,000 synthetic session positions, 22 observation dates and 1,320
observations. The test executes the actual predecessor/candidate `run_backtest`,
fit arithmetic and downstream decision functions with local data/I/O stubs.
Price-feature buckets are deliberately supplied fixtures, not a historical signal
study. The retained predecessor consists of the exact two changed functions.

| Diagnostic | Before | After |
| --- | --- | --- |
| 63-session OOS | 780 training rows; 480 test rows; spread 3.01%; correlation 0.45 | Withheld: 120 nominal training labels touch/cross boundary; all 780 lack verified availability |
| 126-session OOS | 780 training rows; 360 test rows; spread 12.03%; correlation 0.64 | Withheld: 240 nominal training labels touch/cross boundary; all 780 lack verified availability |
| Full-history priors | Retrospective fit | Identical SHA-256 |
| Six scored candidate rows | Priors, pillars, scores, tiers, gates, plans | Zero differences |
| Model basket / cash | Existing allocation | Identical |

These numbers are regression evidence, not market performance. Price mutations
after the split never restore OOS qualification. Full-history priors intentionally
respond to changed realized historical prices; suppressing that sensitivity would
change the strategy rather than repair the diagnostic.

## Validation and reproducibility

```sh
PYTHONDONTWRITEBYTECODE=1 python aws/lambdas/justhodl-katlin/tests/run_tests.py
PYTHONDONTWRITEBYTECODE=1 python aws/lambdas/justhodl-khalid/tests/run_tests.py
node --test tests/katlin-oos-render.test.js
PYTHONDONTWRITEBYTECODE=1 python aws/lambdas/justhodl-katlin/tests/test_oos_boundary.py --evidence > /tmp/katlin-oos-evidence.json
cmp /tmp/katlin-oos-evidence.json docs/audit/katlin-oos-paired-evidence.json
```

The native Katlin runner includes six new tests, 13 war-room tests and 28
existing boundary tests. Khalid's 65 regressions pass. The actual current page's
renderer displays the unavailable reason, omits OOS cards and preserves cohorts
and prior tables using the candidate's projection of a legacy artifact.

Two existing whole-function preservation tests initially failed as expected.
They now use `tests/katlin_oos_test_support.py` to assert the exact permitted
OOS block/dictionary changes and still compare every unaffected statement/field;
there is no wholesale function exemption. Predecessor fixtures remain unchanged.
Python preflight, syntax compilation, diff and staged-inventory gates are required.

## Migration, acceptance and rollback — instructions only

1. Independent review must accept the exact draft head, including the paired
   evidence and preservation-test changes. Parent then issues a release instruction.
2. Deploy only this existing function through the existing GitHub Actions lane.
   Preserve all schedule/config bindings and priors; no backfill, invoke, workflow
   change or old-object rewrite is included here.
3. Verify commit-bound code receipt. At the next normal daily rebuild, verify
   `data/katlin.json.validation.oos` is empty and the unavailable reason is visible
   on `katlin.html`, while cohorts/priors remain present. A permission-only refresh
   is not sufficient acceptance. At the next normal weekly backtest, verify
   `katlin-oos-boundary.v1`, empty OOS and per-horizon counts in the producer output.
   No scheduled run is accelerated by this draft.
4. Same-input replay must preserve `feature_stats`, scores/tiers/plans/weights;
   normal market/data changes across live runs must not be misattributed to this
   patch. Missing availability is a blocker, not a failed attempt to find alpha.
5. If runtime acceptance fails, parent reviews a targeted revert through the same
   deployment lane. Keep the legacy-OOS presentation guard if possible; a whole
   source revert can restore misleading diagnostic metrics and must be reported
   as a rollback to known-invalid validation, not as successful model validation.
   No historical objects or decision policies need migration/rollback writes.

Survivorship, historical flows, cost/capacity models and complete-strategy testing
remain unresolved and explicitly outside this correction.
