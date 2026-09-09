# Risk and release evidence appendix — 9 September 2026

This appendix records source and offline-test evidence for the original fusion/risk audit. It does not certify the deployed estate, execution readiness, investment performance, or hedge-fund-grade operation. The original audit examined `34ddd51`; the verification of Claude's changes examined `6c594c3`/`125a68a`. The remediation below was subsequently implemented in the shared fixes checkout. Release guard commit `8705d12` is included; live release recovery and artifact verification remain separate evidence.

## Original findings and source closure

“Closed at source” refers to the concrete reproduced defect. It does not imply all further data recommendations have been implemented or that the corrected package is serving production.

| Original ID | Verification of Claude's changes | Subsequent source result and evidence |
|---|---|---|
| FR-01: Katlin bypasses authority | Fixed | Binding Khalid Risk permission/cap remains, with local desk opinion and raw Risk Gate separately disclosed. Katlin tests exercise a binding 50% authority and entry prohibition. |
| FR-02: stale/missing evidence or explicit zero permits capital | Partial | Closed at source: `aws/shared/capital_contract.py` enforces producer/schema/status, finite coherent cap, canonical critical sources, bounded clocks and expiry. Katlin requires usable local gate evidence; `capital-view.js` and page refresh timers remove expired permission. |
| FR-03: single-name limit breached | Fixed | Original post-quality single-name fix retained. Separately discovered RV-03 gross rounding failure is closed: conservative rounding and final invariant failure hold all recommendations. Katlin's basket redistribution also respects its name limit. |
| FR-04: independent authority and ignored book | Partial | Unsafe fallback is closed: Risk Sizer requires same-account/book reconciled positive equity NAV, signed priced positions, complete orders and matching NAV history. Existing lots/orders count toward name, cluster and gross limits. Operational sizing remains blocked because Portfolio Snapshot explicitly emits `MISSING_RECONCILED_CAPITAL_LEDGER`; real broker reconciliation has not been invented or replaced with a model portfolio. |
| FR-05: zero NAV discarded | Fixed | Zero remains a real loss observation; unknown history holds. Subsequent book checks require coherent dated NAV history belonging to the same account and book. |
| FR-06: historical replay reads today's feeds | Fixed | Replay remains invariant to current overlay changes. Live overlays are read outside historical calculation. First-publication/vintage correctness remains an additional data requirement. |
| FR-07: disclosed live deductions dropped | Fixed | Weighted legs plus separately disclosed overlays reconcile to the live composite; replay remains separately identified. |
| FR-08: monthly indicators calculated from daily forward-fill | Fixed | Sahm and truck calculations use native monthly observations; insufficient native history stays unavailable. |
| FR-09: Risk Gate page mismatches producer | Partial | Closed for the reproduced rendering failures: nested values, source/status/age, complete evidence and full JSON are accessible in `risk-gate.html`; no `[object Object]` or summary truncation is the only route to evidence. |
| FR-10: fusion rejects composite-only credit contract | Present | Closed at source: optional alternatives are validated when present, against the production registry. `fusion_engine.py` tests accept the actual credit producer contract. |
| FR-11: intentional exclusions falsely degrade health | Present | Closed at source: policy scoring exclusions remain visible separately and do not imply missing/invalid source health. Homepage policy accepts the corrected contract. |
| FR-12: Khalid Risk page hides permission evidence | Present | Closed for the identified evidence omissions: `khalidrisk.js` exposes domain details, source health, policy, failures and lineage, with complete evidence access and expiry handling. |

The malformed/future/NaN authority cases (RV-01), incomplete/short/unpriced/stale book cases (RV-02), aggregate rounding failure (RV-03), and nested renderer omissions (RV-04) are covered by the corresponding shared-contract, sizing and UI regressions. These counterexamples are synthetic tests, not assertions that a particular malformed artifact was observed in production.

Risk Gate now reads genuine `data/term-premium.json` ACM tenor/decomposition evidence and the scoped Treasury `data/settlement-fails.json` contract. It no longer presents `T10Y2Y` as ACM. The TradingView JPLG resolver prevents an IMF loan stock from replacing BOJ percentage growth; the explicit `boj-loan-growth-yoy.v1` / `% YoY` cache and consumer contract rejects incompatible cached rows. These changes do not create executable funding quotes or vintage-complete historical data.

## Tests actually run

On 9 September, the offline risk checker was rerun in the shared fixes checkout while HEAD was `22199a1` (which contains the risk work and `8705d12`). Parallel agents also had unrelated working-tree changes; this is not evidence for an independently assembled release checkout.

| Command / suite | Observed result | Scope |
|---|---|---|
| `python aws/ops/checks/audit_20260909_risk.py` | 69 Python tests: Katlin 12, Risk Sizer 16, Risk Gate 12, Engine Fusion 15, Khalid Risk 9, TradingView 5; plus 20 JavaScript tests | Controlled fixtures, real handler paths with mocked I/O, capital expiry and dedicated-page rendering. No supplied live artifacts and no AWS/network calls. |
| `python tests/deployment/run_tests.py` before the `8705d12` commit | 113 Python tests and 11 candidate shell scenarios | Shared implementation checkout including parallel page/fleet tests; not the smaller release checkout's count. Exact archive hash, numbered execution, validation, promotion/rollback, schedule preservation and release ordering. |
| Focused alias-protection tests within that deployment run | 11 tests | Owned publish/create/permission revision changes accepted only after full business-identity comparison; code/configuration/alias routing drift rejected, including drift during an owned mutation and later unexplained revision changes. |
| Ops 5235 plus protector/primer/order preflight | Four files, zero warnings | Static compile/safety checks only. Dispatch receipt, exact original base/current HEAD and uncertain-request nonduplication tested with local mocks. |
| Khalid suite at `bd1d797` | 51 tests | Includes the actual read-only validation handler; no output publication. Separate from the six-engine risk checker above. |
| Research capital ledger peer review | 15 accounting tests at that handoff | Cumulative fill/order capacity, closing reservations, intraday dividend settlement and opening-flow return treatment. Simulated research remains `publication_eligible=false` and is not broker NAV. |

Owned implementation references include `8f0dcb5` (capital/book/expiry), `7169ef2` (governed fusion/UI), `0dcdb7c` (Risk Gate/JPLG), `ef51850` (read-only risk validation), `c843ab2` (critical-input and consumer checks), `8d1b7ba`/`9e690fb` (Risk Sizer privacy), `bd1d797` (shared Khalid release path), `9758409` (layer follow-up), and `8705d12` (controlled revisions/caller-first release/recovery). Commit identifiers document source lineage; each release needs its own exact package and test evidence.

## Governed release scope

The original core release covered ten targets. Source commit `7cc19c3` adds the new public archive index, bringing the allowlist to eleven; the later freshness-monitor validation adds a twelfth target in `aws/shared/governed_targets.py`:

| Target | Target | Target |
|---|---|---|
| `justhodl-backtest-engine` | `justhodl-calibration-snapshotter` | `justhodl-engine-fusion` |
| `justhodl-katlin` | `justhodl-khalid` | `justhodl-khalid-risk` |
| `justhodl-portfolio-snapshot` | `justhodl-research-backtest` | `justhodl-risk-gate` |
| `justhodl-risk-sizer` | `justhodl-public-archive-index` | `justhodl-fleet-freshness-monitor` |

For selected governed releases, the shared helper validates the exact ZIP hash, stable configuration, revision-pinned numbered version, actual executed version and metadata schema before alias promotion. The twelve-function allowlist is source coverage; it does not prove every target was selected, deployed or refreshed in one release. New-service bootstrap requires explicit source configuration and a confirmed absent function; missing established dependencies still block deployment. The bootstrap/caller regression handoff at `7cc19c3` passed 21 focused tests.

Alias preparation precedes code staging. Selected scheduler, event coordinator, liveness, backend-agent and portfolio-admin callers are deployed first; exact caller ZIP hash and Active/Successful state must pass before producers can be staged. Caller failure aborts the batch. Bare governed names and `$LATEST` invoked by the corrected routers resolve to `live`; explicit numbered versions and other named aliases remain explicit.

Recovery checkout verifies the requested full commit, its ancestry and identical workflow definition before staging that exact source (`c9b912e`). Metadata preflight rejects unsupported descriptions, structured JSON preserves inherited environment values (`64ea830`), and secret-bearing configuration errors emit only safe metadata (`1fc1b4a`). Schedule normalization (`d76e8d5`) accepts both documented named-rule formats before any staging. Cadence-only strings and abbreviated Scheduler references retain existing bindings and record that decision; they do not create inferred rules. The focused schedule/release handoff passed 52 tests. These are source and offline test results, not successful production rollout receipts.

The read-only verifier addition at `619223b` observes the source-recorded classic EventBridge bindings for liquidity-profile and retail-sentiment, including their actual rule cadence and fully paginated target metadata. Differences from cadence-only configuration remain explicit observations. Its 36-test handoff does not establish that those live reads have run.

The remaining estate does not thereby receive numbered-candidate validation, alias qualification or immutable release guarantees. Legacy engines without the configured contract continue through the general deployment path. Unselected callers, external IAM clients, independent Function URLs, historical versions, and schedules outside inspected targets need their own inventory/evidence. The change does not establish all-fleet immutable inputs, reproducible vintages, or validated execution models.

Risk Sizer's full account-backed S3 originals and authenticated mirror require the matched private publisher, HTTP guard, Worker/policy and owner-page release. Both `data/risk-sizer.json` and `risk/recommendations.json` are private. Cache-Control alone is not access control; anonymous HTTP must fail before account reads. A passing candidate metadata response does not prove those external access boundaries or a fresh production publication.

## Ops 5234: exact current-layer proof and historical limit

The follow-up examines both `$LATEST` and `live` primary/weighted version layer bindings, requires the reviewed deterministic layer archive hash, and compares every non-intended configuration field before considering alias promotion. Only the core-layer/FRED changes are intended. Code hash, other environment variables and non-core layers must reconcile; unexplained configuration drift, incomplete discovery or alias-only uncertainty remains blocked. Weighted aliases require review rather than an inferred migration.

Its result is current-state layer/configuration evidence for discovered consumers. It is not handler validation or proof that a prior migration preserved an unknown earlier configuration. The report explicitly retains `prior_5232_configuration_safety=UNVERIFIABLE_WITHOUT_PREMIGRATION_SNAPSHOT`; that historical evidence limitation is separate from any observed current error. It cannot retroactively prove the pre-5232 state. Ops 5234 is to be sequenced after the Lambda release so its configuration updates do not race alias preparation.

## Data and controls still required before institutional execution

The following are specific remaining requirements, not newly available feeds or claims of enforced functionality. Existing model-book and public analytics must keep their own identity and cannot stand in for owner account data.

| Recipient | Required producer/data | Remaining use or implementation |
|---|---|---|
| Portfolio Snapshot → Risk Sizer | Authenticated broker/account ledger: `account_id`, `book_id`, currency, reconciled `equity_nav`, cash, liabilities, `as_of`, `reconciled_at`; complete signed positions, mark timestamps and price status; complete open orders with remaining exposure; same-book NAV history | Reconcile balances, executions, cancellations, corporate actions, cash flows and order reservations before producing READY. Current position CRUD is not that ledger; default BLOCKED remains necessary. |
| Risk Sizer and Katlin | Licensed executable quote adapter: symbol/instrument/venue, bid/ask and sizes, currency, quote observation/receipt time, expiry, session/auction status, halt/tradability flags | Enforce spread, stale-quote, market-session and executable-price constraints. A last trade or research close is not an executable quote. |
| Risk Sizer and Katlin | Complete liquidity-capacity rows for every candidate: dated dollar volume/ADV, participation limit, comfortable position, liquidation horizon, known/unknown coverage and NAV basis; borrow locate, availability, rate and recall terms for shorts | Translate capacity to the actual account NAV and enforce incremental participation/liquidation/borrow limits. Current limited-name lists or hypothetical AUM cannot establish every candidate's capacity. |
| Risk Sizer and Khalid Risk portfolio overlay | Date-aligned adjusted returns and same-book factor loadings, net market beta, ES/stress scenarios, direct/proxy coverage and exposure timestamp | Date joins now match both interval start and end sessions, require at least20 pairs and use the latest60; conflicting duplicate dates fail closed. Connected correlated names share one cap; missing pair evidence uses a disclosed sector fallback. Upstream adjustment-basis and factor/covariance validation remain required. Market permission alone does not establish portfolio suitability or hedge effectiveness. |
| Risk Sizer and Katlin probability/return claims | Versioned out-of-sample calibration by horizon/universe/regime: prediction publication time, realized outcomes, sample counts, payout/loss distributions, costs, spread/slippage/impact/financing/borrow, confidence intervals and drift | Current sizing declares heuristic conviction and symmetric-payoff fractional Kelly with `calibration_status=UNVALIDATED`. A label does not produce calibrated probabilities. Independent, cost-adjusted walk-forward evidence is required before interpreting the number as a win probability or expected return. |
| Risk Gate, Engine Fusion and research backtests | Immutable raw observation snapshots with original series/observable IDs, units, frequency, `observed_at`, first `available_at`, revision/vintage, source calendar/SLA, input-set hash and actual dependency ancestry | Make first-publication historical replay reproducible and measure overlapping evidence. Removing today's overlays does not establish point-in-time vintage correctness or independence of composite signals. |
| Risk Gate funding/structure legs | Actual tenor-specific cross-currency basis quotes; repo specialness/rates/haircuts; bond liquidity and auction observations with market/tenor/unit/source timestamps | Keep current rate-differential and plumbing proxies explicitly identified. Do not promote proxies into executable market quotes or count duplicate root facts as independent signals. |
| Account execution/risk service | Broker order IDs and idempotency keys; accepted/rejected/partial-fill/cancel state, venue limits, buying power/margin, instrument multipliers/FX/derivative exposures, circuit breakers and tested reconciliation/recovery | Establish order lifecycle, loss/capacity enforcement, operational monitoring and recovery on the actual account. This audit slice adds research/permission controls; it does not establish a complete institutional order-management or execution platform. |

A future positive capital permission must still be interpreted within its defined exposure basis and valid input window. Live release acceptance requires the selected package/configuration hashes, actual aliases and caller routes, newly generated artifact clocks/contracts, private access checks and rendered page behavior. Source closure and passing fixtures are necessary evidence, not hedge-fund certification.

Recovery follow-up: Risk Sizer returns are keyed by (start_session,end_session), preserving gaps rather than shifting rows. Missing/invalid dates, nonfinite/negative closes and conflicting duplicate dates cannot imply a measured correlation. Connected components enforce one cluster cap across A–B–C exposure chains. Unmeasured correlations are null; sector fallback and upstream adjustment-basis limitations are explicit. The19-case actual sizing suite passes, including dated-pair, transitive-cap and existing private-book/authority tests. This follow-up still requires its own promoted-version receipt.
