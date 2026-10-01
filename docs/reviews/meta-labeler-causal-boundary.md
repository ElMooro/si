# Meta-labeler historical causality repair

Scope: the separately authorized TAKE/SKIP meta-labeler, approved 2026-10-01
13:47 UTC. This is not PR30's Mode A withdrawal or Katlin's OOS correction.
Baseline: `e5956182093119f917956e23e9bc2403f5cd751d`.
Branch: `codex/meta-labeler-causal-boundary`. Draft only; no merge or deploy.

## Finding and correction

The actual writer is
`aws/lambdas/justhodl-meta-labeler/source/lambda_function.py`. Its input is
`data/_backtest/graded.json.gz`, produced by the unchanged harness `mode_b`.
Rows have only id/type/ticker/date/conf/dir/ex. They do not establish decision
instants, label-end times, first label availability, feature availability or
historical data vintages. Harness dates can also be reconstructed from text.
Current publication timestamps cannot recover those missing facts.

The former writer sorts signal dates and takes `int(n * .7)` rows for training.
It never purges overlapping labels, can split one date between training and
test, and chooses its top-five type vocabulary from the entire dataset. Its
scaling means/standard deviations are train-only; that does not repair the
other leaks. `spy_context` includes the signal date's closing price without
proving that close was available at the signal's decision time. Today's fitted
model is also applied to historical pending signals without a model-availability
boundary. Fixed threshold/optimizer constants do not prove a valid selection
process or a qualified untouched final test.

Offline execution of retained predecessor functions demonstrates:

- changing a not-yet-available training outcome changes the actual training
  labels, while all ten fixture signals share one date and the split is 7/3;
- changing only final test-row types changes the model vocabulary and training
  feature matrix;
- changing only the signal-date close changes the SPY context;
- an independent reviewer reproduced 300 same-date rows yielding an `active`
  210/90 split and a pending recommendation without any provenance.

These are synthetic bug reproductions, not real-data performance estimates.
The byte-exact predecessor and SHA-256 manifest remain under
`tests/fixtures/meta-labeler/`. No real historical performance is reconstructed.

The smallest defensible correction is unconditional withdrawal. The writer
publishes `meta-labeler-withdrawal.v1`, status `unavailable`, qualification
`BLOCKED`, decision eligibility false, nullable counts/metrics, and empty gates
and per-type results. It performs no provider, secret, graded-history or signal
read and fits no model. It does not translate unavailable into SKIP or a zero
hit rate. Input archives and harness Mode B are untouched. This patch does not
implement a new qualified model or claim the user's full strategy is validated.

## Required causal boundary before reactivation

This is a specification for separately reviewed restoration, not an implemented
validator. No input flag, version, timestamp, extra row count or future contract
can reactivate this code.

1. Every observation needs immutable source/version references, an exact
   timezone-qualified decision instant `t`, original signal creation and
   feature values, feature release/ingestion times, and documented execution
   timing. Feature availability must be <= `t`; same-day closes are excluded
   unless their release preceded `t`. Current revised/adjusted price history
   cannot impersonate the historical vintage.
2. Labels require actual entry and exit sessions, benchmark alignment, the
   explicit label end `e`, and first label availability `a` including source
   and ingestion delays. At a fit/test boundary `B`, every training row must
   have `t < B`, `e < B`, and `a < B`. Missing, invalid, inconsistent, equal or
   later times fail closed. Never infer 21 trading sessions from calendar days.
3. Split on distinct decision instants/session groups, never within tied dates
   or duplicated signal identities. Purge labels overlapping validation/test
   boundaries; account for dependent/overlapping outcomes in uncertainty.
   Enforce any documented embargo inside nested temporal validation.
4. Fit vocabulary, missing-data handling, scaling, feature selection, model
   hyperparameters and any threshold selection solely within eligible training
   data. Freeze these artifacts before an untouched final test. A held-out
   type must use a predeclared unknown representation, not change the training
   vocabulary. Model selection needs inner causal folds with their own label
   availability cutoffs; no test feedback may tune the reported model.
5. A prediction must bind the exact fitted artifact, training cutoff, source
   versions and model release time to its decision instant. A model fitted
   today cannot claim it would have gated an earlier pending signal. Prospective
   recommendations and historical replay must be distinguished and evaluated
   only when their own labels become available.
6. Require an independently reproduced, exact-code-and-data-bound replay,
   coverage/exclusion reasons and boundary mutation tests before reactivation.
   Unknown history stays unavailable. Trading costs, survivorship, portfolio
   construction and validation of the full strategy remain separate questions.

## Consumers and compatibility

- `backtests.html`: actual dedicated renderer now masks missing, legacy,
  malformed, future and self-promoted packets before and after fetch. Statistics
  and recommendation tables are hidden; a plain withdrawal notice replaces them.
  Existing Mode A blocking, Mode B rows/zeros, navigation and table structure stay.
- `justhodl-ask-desk.fetch_slim`: projects the meta-labeler key through an
  unconditional shared allowlist; no old metrics, gates or narrative reach the
  answer context. Catalog description reflects withdrawal. Other keys retain
  their existing behavior. Tests invoke fetch/projection only, never paid LLMs.
- Signal Board: registry contains a legacy normalizer name, but current
  `signal_board_candidate.build` compiles a derived-source inventory with ABSTAIN
  and false eligibility for every row. It does not execute the old normalizer or
  propagate performance. Source bytes remain archival evidence; no edit needed.
- Page AI: the manifest names both harness and meta-labeler for backtests.
  The current handler delegates to `page_explanation_store.run`; its source
  summary captures identity/shape/publication, not model metrics. The output is
  unqualified research. Legacy page commentary may still be displayed as
  archived unqualified prose by the shared widget; the backtests withdrawal
  explicitly states that such commentary cannot restore metrics/recommendations.
- Generic engine/data inspectors, wiring/contract registries and original-source
  archives expose source metadata or raw evidence, not an additional TAKE/SKIP
  decision path. Raw historical bytes are not rewritten or made qualified.

No harness writer, Katlin code, sizing/fusion logic, schedules, paid API, private
Brain data, live trade or local AWS operation is changed or used. AGENTS.md and
checkout .agents/skills were absent in both the provided checkout and fetched
main; DEPLOY_LANE.md, CLAUDE.md, AUTONOMY.md and current claims were read. Unsafe
legacy credential-recovery instructions were superseded by the user's prohibition;
only runtime-authenticated Git/GitHub was used.

## Validation and release plan

Focused tests reproduce predecessor leaks and exercise the real new writer,
Ask Desk projection and page renderer. The Mode A predecessor fixtures remain
unchanged; its old meta-labeler preservation assertion is redirected to retained
exact bytes because this distinct repair is now authorized. Mode B equivalence
and protected Quantum Desk/Signal Board hashes still pass.

Before merge: complete Python/frontend/deployment gates, page syntax/wiring
checks, intercepted browser checks at 1440 and 390 pixels, staged inventory
verification and independent review of the exact draft head. Intentional source
removal is marked `[shrink-ok]`; this is a complete withdrawal writer, not a stub.

After parent acceptance, merge the exact reviewed head using existing GitHub
Actions lanes. Expected targets are meta-labeler, Ask Desk (shared helper
importers) and Pages. No manual producer invocation or schedule change is part
of this plan. Verify each Lambda release receipt against the merge SHA and
source hashes; verify Pages' commit-bound manifest and served bytes. Inspect
public meta-labeler output after its next natural run for the withdrawal contract,
null metrics and empty gates; until then the old direct JSON may remain cached,
though updated dedicated consumers mask it. Receipt acceptance alone does not
prove natural publication. Repeat public browser checks; do not send paid Ask
Desk requests for QA. If any additional affected target appears, review its
cause before release.

Rollback: do not blindly revert to the leaky writer. Keep this withdrawal helper
and producer contract active while repairing any consumer/deployment regression
in a reviewed forward patch. If a presentation-only rollback is necessary,
retain unconditional masking. Do not restore historical metrics or TAKE/SKIP
without the separate causal-provenance restoration review above. Stop and report
failed receipt/publication checks; do not change schedules to force a result.
