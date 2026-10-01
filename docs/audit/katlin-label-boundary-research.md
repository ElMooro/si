# Katlin label-boundary review — offline only

Baseline: `0035bf47da741a2916a0e34df173934e5ae297aa` (main fetched 2026-10-01).
No production defect is repaired by this branch. It supplies an offline evidence
filter and regression counterexample; independent review is still required.

## Why the production slice was stopped

All line references below are in
`aws/lambdas/justhodl-katlin/source/lambda_function.py` at the baseline.

| Producer/consumer | Effect |
| --- | --- |
| `run_backtest`, lines 3341–3415 | Fits full-history `feature_stats`, entry-index-only OOS training and cohort summaries into `data/katlin-backtest.json`. Training entries before the boundary can have 63/126-session labels ending after it. |
| Feed loader, line 1679 | Reads that same public artifact into `F['backtest']`. |
| `learned_prior`, lines 3535–3561; handler lines 3751–3777 | Uses full-history feature bucket deltas for expected excess, learned pillar, cross-sectional composite and tier processing. |
| `gates_and_tier`, line 2759 onward | Expected excess affects history gate, PRIME/READY and crash-barbell eligibility. |
| `build_basket`, line 3456 onward | Expected excess and variance affect model basket membership/weights under the existing cap. |
| `regime_history`, `barbell_base_rate`, `validation_summary`, lines 3422–3532; output lines 3806–3817 | Exposes historical cohorts, OOS diagnostics and learned statistics to the desk. |
| `aws/lambdas/justhodl-khalid/source/lambda_function.py`, `_katlin_rows`, line 516 onward | Adapts Katlin picks/watch into Khalid candidates, including pillars and trade-plan fields; upstream priors already affect which rows are picks. |

Repository searches found the direct `katlin-backtest` producer/read in Katlin,
with derived output consumed downstream. OOS diagnostics themselves are distinct
from the full-history prior; this branch does **not** assert that the OOS-only
field directly sizes positions. But the shared producer and fit pipeline are not
an isolated research surface. The authorized fallback avoids changing either.

No Lambda, shared runtime module, page, configuration, workflow, schedule or
production artifact is changed. The new script is not wired into any consumer.
It accepts a local file, performs no network calls and prints evidence metadata.

## Offline component and evidence contract

Run:

```sh
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -p 'test_katlin_label_boundary.py' -v
python scripts/research/katlin_label_boundary.py /path/to/local-evidence.json
```

The local JSON contains `horizon` (positive integer sessions), `test_start_index`,
timezone-aware `test_start_at`, and an `observations` array. Each observation has
unique `id`, `entry_index`, `entry_at`, `features_available_at`, and `labels` keyed
by horizon string. Each label supplies `endpoint_index`, `endpoint_at`,
`available_at`, `source_record_id`, and finite `excess_return_pct`.

Eligibility requires the exact fixed-horizon endpoint index, features known by
entry, entry before endpoint, endpoint no later than availability, and entry,
endpoint and availability strictly before test start. Both index and clock
checks apply. Missing clocks, naive/date-only timestamps, inconsistent horizons,
nonfinite returns and absent evidence references are excluded. No timestamp is
inferred from a date, file modification, collection clock or horizon arithmetic.
An early delisting endpoint is unsupported rather than accepted as a complete
fixed-horizon label. IDs and fold metadata must be valid; duplicates fail the audit.

This validates the supplied evidence's internal boundary consistency, **not**
source authenticity, exchange calendars, corporate actions or provenance.
`source_record_id` must ultimately resolve to reviewed retained evidence. Katlin's
existing aggregated artifact lacks this per-label availability contract: do not
feed its dates into the script as invented availability evidence.

Output includes accepted IDs, exclusion reasons/counts and fold/horizon metadata;
reason counts can exceed excluded rows because a row can fail multiple checks.
It always sets `research_only=true`, `decision_eligible=false`, and
`validated_strategy=false`. It emits neither a replacement backtest nor weights.

Tests extract only the existing pure nested `fit()` and actual legacy training
selection expression via AST, never importing a Lambda/cloud client. On invented
session/price fixtures, changing every test-period price changes the legacy
training fit; applying the offline filter makes the full fitted statistics
invariant for 63/126/252-session labels. Endpoint equality, delayed availability,
timezone equality, horizon-specific membership, malformed/missing evidence and
empty cohorts are tested. These are regression fixtures, not historical results.

## Validation in this draft

- Eight offline boundary tests pass, including a local-file CLI smoke test.
- Katlin's existing runner passes 13 war-room tests and 28 invoked boundary tests.
- Khalid's existing runner passes 65 scoring/projection tests; browser sniper
  tests pass 2/2.
- Repository preflight passes for both Python additions, with zero warnings.
- `git diff --check` and exact staged-file inventory are required before commit.
- No production source is edited or imported by the new regression tests. No
  historical data was acquired or performance study run. Independent review,
  merge and deployment have not occurred.

## Limits and deferred work

- The current Finviz universe (`build_universe`, line 2636; `run_backtest`, line
  3222) does not establish historical membership or remove survivorship bias.
- Historical flows, fundamentals, classifications and holdings availability are
  not supplied by the price history. Acquired retrospective histories cannot
  silently become contemporaneous evidence.
- This boundary filter does not solve dependence within training data, repeated
  cross-sectional events, multiple testing, execution costs, capacity or exits.
- `justhodl-backtest-harness` has a separate 21-session overlap defect, an
  annualized/unannualized Sharpe comparison and ticker-ordered drawdown problem.
  Keep its repair in a later independently scoped commit/PR; none is mixed here.
- Review the proposal in `docs/research/khalid-event-study-proposal.md` before a
  historical study or evidence display on `khalid.html`. No gate is auto-relaxed.
