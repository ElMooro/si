# Backtest harness Mode A withdrawal — draft, not validation

Baseline: `6233db36489885f91b1e53e7b83c641840f898ab`. No historical study,
source acquisition, production invoke, schedule change, merge or deployment was
performed for this draft. Independent exact-head review and release remain open.
Meta-labeler correction requires separate approval and is deliberately absent.

## Result and boundary

`backtest-harness-mode-a-withdrawal.v1` always publishes `status=BLOCKED`,
`qualified_rules=0`, `validated_strategy=false`, `decision_eligible=false` and
`historical_availability_verified=false`. Every Mode A rule has `PASS=false`,
`chosen=null`, `configs_tried=0`, null OOS metrics/gate and an empty curve. The
unchanged eight-rule inventory retains family and `configs_defined`; zero trials
and folds refer to no new Mode A fitting. Null metrics mean unavailable, not
zero return or failed strategy. No packet, clock or self-declared future contract
can restore qualification through these consumers.

The page preserves its sections, columns, navigation and meta-labeler controls.
Mode A claims are replaced with unavailable/BLOCKED, including when the source is
missing, legacy, malformed or declares a future/qualified contract. It never
renders the packet's old methodology, selected configuration, PASS or equity
curve. Headline explanations are present before data loads. Reported universe
size and stored observation count are labelled as such, without alignment or
freshness claims. Mode B and meta-labeler rendering remain the same for valid
inputs, including zero values; mobile layout permits the existing tables to scroll.

Alpha-decay no longer reads the harness to manufacture DEPLOYABLE archetypes.
It publishes no backtest-vs-live rows and a zero backtest-pass count with the
blocked contract. Its scorecard, regime processing, snapshots, decay comparisons,
live health alerts and history writes are preserved. Ask Desk masks both direct
harness artifacts and legacy alpha-decay derivatives *before* slimming: protected
Mode A status scalars come first, followed by the existing Mode B/live-health
context. Unknown top-level claims are not forwarded. Other source paths, including
meta-labeler TAKE/SKIP, retain their existing routing and semantics.

## Consumer trace and schemas

| Boundary | Reviewed contract / effect |
| --- | --- |
| Harness `lambda_handler` / `mode_a` | Public `data/backtest-harness.json`: `rules` list, `n_pass`, methodology. No corrected performance metrics produced. |
| Harness `mode_b` | Same inputs, full function AST, data cache and `data/_backtest/graded.json.gz` writes. Live-signal rows remain a list, not an archetype-to-signal mapping. |
| Meta-labeler | Reads the separate graded gzip, not Mode A rules; source hash unchanged. Its chronological grading/TAKE-SKIP methodology is not certified by this work. |
| Alpha-decay | Old `rule.PASS` directly manufactured `backtest_says=DEPLOYABLE`; removed. Old mapping expected a dict while harness writes a list; no speculative mapping repair is attempted. |
| Ask Desk `fetch_slim` | Direct harness extra source plus manifest-addressable alpha-decay are guarded. Private-source checks and other routes remain in place. |
| Quantum Desk `risk_extras` | Reads signal/alert/status lists from alpha-decay, not `backtest_vs_live` or its count. Actual-function replay produces identical `signal_health`. Source hash unchanged. |
| Signal Board | Current candidate inventory already abstains with false decision permissions; no Mode A scoring normalizer runs. Source hash unchanged. |

Repository consumer searches found no other current Mode A automatic sizing,
order or capital decision path. This is a source review, not a claim to have
inspected deployed/private artifacts. Katlin, Khalid, meta-labeler, capital policy,
workflows, configuration, schedules and upstream ring producer are unchanged.

## Reproduced counterexamples

The retained predecessor sources are byte-for-byte snapshots with SHA-256 hashes
in `tests/fixtures/harness-mode-a/manifest.json`. Tests compile pure functions by
AST; they do not import cloud-initializing Lambdas. All prices/clocks are invented.

* At the legacy first training boundary 110 with horizon 21, entries before 110
  can consume endpoints at/after 110. Seeded prices held identical before 110,
  then multiplied by 4 or 0.2 afterwards, change actual deep-drawdown training
  statistics and selected configuration from `x=45` to `x=35`. The withdrawn
  producer cannot call `feats`, `collect_trades`, `stats` or `expected_max_sr`;
  tests replace them with throwing sentinels during its actual handler replay.
* The same 60 returns in grouped versus interleaved ticker order give identical
  Sharpe but legacy drawdown of -70.6% versus -21.7%, flipping the old PASS gate.
  This is not a time-indexed, funded portfolio equity curve.
* 490 returns of -10% and 510 of +10% produce reported annualized Sharpe 0.07.
  The legacy unannualized gate is approximately 0.0481; comparable annual units
  give approximately 0.1667. The old comparison passes the example incorrectly.
  Its `(maxdd or -99)` also maps legitimate zero drawdown to -99.
* Actual upstream `ingest`, with one missing ticker session and ring length 3,
  yields SPY `[102,103,104]` and A `[201,202,204]`: equal-length rings represent
  different sessions. Prices alone do not restore dates or first availability.

No simple purge fixes the absent availability, current-universe selection,
calendar alignment, dependence, multiplicity, execution, costs or capacity.
These are reasons to withhold qualification, not claims that a corrected strategy
would succeed or fail.

## Validation and reproduction

* `python tests/test_harness_mode_a_withdrawal.py`: 9 tests, including actual
  old/new Mode B cache/graded/publication equivalence, alpha history/live-health
  equivalence, Quantum Desk signal health, Ask Desk direct/derived legacy guards,
  missing/zero/type/future-contract cases, and protected-source hashes.
* Each affected Lambda runner executes these regressions. Ask Desk also passed
  its existing 4 private-boundary checks and 7 Signal Board consumer tests.
* `node --test tests/*.test.js`: 2,260 passed, including 3 new renderer tests.
* `node tests/harness-mode-a-browser.cjs` (Playwright + `/usr/bin/chromium`):
  8 actual-browser scenarios at 1440/390 pixels, all requests intercepted with
  invented fixtures. Mode A remains blocked; Mode B and TAKE remain visible;
  Board navigation works; no page errors or horizontal document overflow.
  Screenshots are written locally to `/tmp/harness-withdrawal-{1440,390}.png`.
* Deployment runner: 1,018 static checks plus 15 shell checks passed. Temporary
  boto3 dependency, dummy credentials, metadata disabled and external socket
  denial were used. Printed release/dispatch receipts are test fixtures only.
* Python preflight: eight changed Python files, zero warnings. Page checker:
  599 graphs, no syntax errors. Wiring: 36 pages / 143 engines, no drift.
  Brain/public boundary tests: 15 passed. Secret scan: no findings.
* Packaging smoke: all three source-plus-shared ZIPs contain and compile the
  boundary and handler; isolated `python -S` imports the boundary from each ZIP.
  No handler invocation or deployment is performed by this packaging check.

## Cost and release surface

Direct/transitive shared-dependency inventory reports exactly
`justhodl-backtest-harness`, `justhodl-alpha-decay`, `justhodl-ask-desk`. The new
stdlib-only shared file will also be physically bundled by the existing generic
packager in other future Lambda builds, but those engines do not import it.
No new layer, dependency, provider call, data read, write destination or cadence.
Alpha-decay saves one existing S3 read per run. Harness skips feature generation,
63 configuration training passes and 24 OOS passes per run. Mode B's DDB/provider
work and cache behavior remain unchanged. No production savings measurement or
new dollar-cost claim is made. Ask Desk retains its existing retrieval/LLM budget.

Exact intended changed-file inventory (17 files):

1. `docs/SESSION_CLAIMS.md`
2. `docs/reviews/harness-mode-a-withdrawal.md`
3. `aws/shared/backtest_harness_authority.py`
4. `aws/lambdas/justhodl-backtest-harness/source/lambda_function.py`
5. `aws/lambdas/justhodl-backtest-harness/tests/run_tests.py`
6. `aws/lambdas/justhodl-alpha-decay/source/lambda_function.py`
7. `aws/lambdas/justhodl-alpha-decay/tests/run_tests.py`
8. `aws/lambdas/justhodl-ask-desk/source/lambda_function.py`
9. `aws/lambdas/justhodl-ask-desk/tests/run_tests.py`
10. `backtests.html`
11. `tests/test_harness_mode_a_withdrawal.py`
12. `tests/harness-mode-a-withdrawal.test.js`
13. `tests/harness-mode-a-browser.cjs`
14. `tests/fixtures/harness-mode-a/harness.py.txt`
15. `tests/fixtures/harness-mode-a/alpha.py.txt`
16. `tests/fixtures/harness-mode-a/backtests.html.txt`
17. `tests/fixtures/harness-mode-a/manifest.json`

Release must not treat this withdrawal as evidence for promotion. Legacy deployed
artifacts/pages remain possible until an independently reviewed release; this
branch makes no live-fix claim. Meta-labeler approval and any replacement historical
study remain separate blockers to broader certification.
