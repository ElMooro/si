# Accounting and macro audit remediation — 9 September 2026

This is a source and offline behavioral-verification record. Deployment, promoted
Lambda-version parity, authenticated page access and newly generated production
artifacts require the release team's separate evidence. The author did not invoke
AWS, deploy code, populate a ledger, upload synthetic history or send messages
outside the agent team. A source fix is not a production sign-off.

Source baseline for the re-audit was `6c594c3`, following the original `34ddd51`
audit. Remediation began on `125a68a`. The prior verification found INST-07 fixed,
INST-08–13 only partially fixed, plus two new handler failures. Tracker labels and
arithmetic-only tests were not accepted as proof.

## Accounting findings

| Finding and original defect | Implemented behavior | Behavioral evidence |
|---|---|---|
| INST-07 — quantity-only edits could retain incorrect cost basis or side | Retains Claude's finite-input, existing-position and concurrency guards; recomputes total basis and side from signed quantity. Snapshot refresh explicitly targets promoted `:live`, not `$LATEST`. | `aws/lambdas/justhodl-portfolio-admin/tests/run_tests.py`: actual update handler, missing position, nonfinite input, quantity/sign change and exact async `Qualifier='live'` invocation. |
| INST-08 — unverifiable marks and invented account capital; mixed priced/unpriced positions crashed sector aggregation | Requires finite positive prices and valid mark times; rejects missing/future/stale prices. Unpriced value/P&L/stop results stay null; mixed books finish. Capital book remains BLOCKED until real reconciled cash/liabilities/reserved orders and NAV history exist. Position value is never account NAV. | Same actual snapshot-handler suite plus `justhodl-portfolio-snapshot/tests/run_tests.py`: priced/unpriced mixture, stale/missing/future/nonfinite marks, immutable quantity basis, validation-only zero writes. |
| INST-09 — same-week calibration versions and timezone ordering could backdate weights | Unique immutable calibration version IDs, conditional create, listing of `calibration/versions/` by the actual reader, normalized UTC comparison. Mutable legacy weekly objects cannot supply performance provenance. | `justhodl-backtest-engine/tests/run_tests.py` and `justhodl-calibration-snapshotter/tests/run_tests.py`: real producer/reader integration, same-week and same-second versions, Sunday snapshot rejected for preceding Tuesday, offset-equivalent UTC cutoff. |
| INST-10 — completed horizon returns booked on signal dates and promoted as portfolio NAV/Sharpe/annualized return | Every attribution version is explicitly ineligible for portfolio/publication claims. Added a separate real daily marked research-ledger adapter, immutable reader and dedicated page section. It processes supplied events rather than converting outcomes into trades. Missing or invalid inputs return empty BLOCKED curves. | Backtest runner exercises actual immutable reader, actual validation-only handler and DOM publication gates. `aws/shared/tests/test_research_capital_ledger.py` exercises the real adapter with independently specified synthetic account snapshots; see scope below. |
| INST-11 — broken historical builder, latest-record leakage, unmatched critique/regime and overstated sample significance | Restores functioning historical decision builder, enumerates/deduplicates every historical decision, uses entry-time regime and only matching available critique. Corrects significance sample to actual non-null paired alpha observations with conservative uncertainty. Historical associations remain descriptive. | `justhodl-research-backtest/tests/run_tests.py`: actual builder, historical decisions, identity/availability mismatch exclusion, missing-alpha sample and validation-only zero writes. |
| INST-12 — grade endpoints had mismatched sessions, requested dates masqueraded as observations, incompatible adjustment basis and zero-return fallbacks | Preserves actual endpoint session and proven price basis; requires aligned instrument/benchmark marks and compatible baseline provenance. Unfinished current-day bars defer. Incomplete legacy evidence becomes pending/UNSCOREABLE rather than fabricated zero/correctness. Existing completed grades are not rewritten. | `justhodl-outcome-checker/tests/run_tests.py`: actual grading path with mocked mark providers, mismatched dates/bases, unresolved baselines and retry/session guards. |
| INST-13 — malformed timestamps, failed reads and incomplete expected-output checks reported FRESH | Enumerates and validates nested/declared JSON contracts. Failed reads, malformed/future timestamps, partial validation, unresolved families and coverage gaps become UNKNOWN/MISSING. Health cannot be HEALTHY without complete verified expected coverage. | `justhodl-fleet-freshness-monitor/tests/run_tests.py`: actual freshness evaluator/handler paths with malformed times, failed bodies, nested/expected artifacts, missing config/registry and unknown coverage. |

The four bounded producers/readers (snapshot, calibration snapshotter, backtest and
research backtest) accept `{"mode":"validate_only"}`. Their real computation returns
an `audit-accounting-1.0` envelope with `ok`, `validation_only`, `status` and positive
`artifact_size_bytes`; tests prove no business writes, downstream invokes or
watchlist synchronization. Outcome/freshness have distributed writes and do not
claim this mode: they require behavioral gates plus deployed package and output
verification.

Actual owner snapshot publication is separate from model research. Snapshot HTTP
calls require the private service guard before any account read. Normal publication
requires authenticated private storage and uses `private, no-store` for its canonical
S3 copy. An authentication/publication failure propagates; candidate validation
skips publishing. Tests cover anonymous HTTP denial before all account reads and
no snapshot persistence after private publisher failure. Worker routes, public S3
denials and remaining owner-account producers belong to the security release.

## Cross-engine donor work

All receivers use `aws/shared/donor_contract.py` and the production builders in
`aws/shared/macro_donor_inputs.py`. Contracts retain exact source artifact,
generation/observation timestamps, independent age limits, inspected fields, units,
status and errors. Zero is valid; missing/stale/invalid data cannot become a benign
zero score. Daily and weekly/monthly/quarterly observations have distinct age
limits. Evidence with overlapping economic meaning does not add duplicate votes.

| Dependency | Receiver and exact added data/use | Data and interpretation boundary |
|---|---|---|
| D01 | Liquidity-credit: repo-market score, distribution tails, percentile/z-score context, spreads, facilities, reserves and calendar. Funding review floor is `max(base, verified repo score)`. | Explicit heuristic review, not a calibrated execution signal. Missing repo degrades coverage; unobserved facility timestamps remain UNKNOWN. |
| D02 | Bond war room: same dated repo family, joined with settlement evidence into named funding/settlement review states. | Daily funding and weekly fails remain different horizons; no causal same-day assertion or repeated score votes. |
| D03 | Liquidity-capacity: guarded repo state plus 20%/10%/5% participation sensitivity scenarios. | Funding-adjusted executable capacity BLOCKED_UNCALIBRATED until historical impact/participation validation; systemic repo is not a stock liquidity quote. |
| D04 | Bond war room: dealer tenor inventory, net/wow/z-score, financing and transaction panels; signed tenor inventory ranking and elevated-inventory review. | Fails normalization stays blocked without a proven matched-week, collateral-comparable transaction denominator. Inventory is not trading volume. |
| D05 | Repo: complete Treasury FTD/FTR/gross, all collateral classes and totals, weekly date and source, settlement watch. | Treasury gross must reconcile and coverage must be complete. Existing raw fails inputs already count this evidence; added vote is zero. |
| D06 | Bond war room: same full settlement-family evidence paired with funding/dealer context. | No daily-DTCC relabeling of weekly NY Fed FR2004 data; no duplicate numeric votes. |
| D07 | Bond war room: term-premium levels/deltas/z/percentile, curve and ACM yield decomposition. | Fitted yield must reconcile to risk-neutral yield plus term premium within 2bp. These are model estimates, not observed policy expectations. |
| D08 | Bond war room: graded auctions, CUSIP/date/tenor/accepted size and all dimensions. | Tail requires actual matched when-issued yield, timestamp before the auction, and auction timestamp. Missing WI remains BLOCKED; populated unsupported tails are suppressed. |
| D09 | Bond war room: guarded foreign-holdings context and a separate SLT transaction contract. | Live donor totals/transactions were empty. Transactions require source dataset SLT, actual transaction amount and valid period. Holdings valuation changes and indirect awards do not prove foreign selling. |
| D10 | Eurodollar plumbing: guarded CBS total/counterparty/offshore/EM claims and periods; existing visible aliases retained. | CBS all-currency foreign claims remain exposure context. USD currency/sector/FX-adjusted funding panel is BLOCKED_MISSING_LBS. |
| D11 | Euro-fragmentation: active CISS countries/sovereign context, provenance/frequency and separately identified discontinued series. Existing country/Bund spreads now use common dated 10Y observations. | CISS/spread observation-date mismatch is explicit context, not confirmation; no repeated SovCISS vote. |
| D12 | Liquidity-credit: CISS composite/regime, active/legacy series, source and frequency context. | Overlap-excluded contextual evidence contributes zero to numeric score. |
| D13 | Backtest: FRED-vintage UNRATE/DGS10/WALCL/RRPONTSYD `known_on`/`available_at` receipts selected at normalized decision time, preserving donor `updated`. | Date-only known-on becomes usable next UTC day. Availability evidence does not falsely claim reconstruction of all historical model features. |
| D14 | Liquidity-inflection: release-aware WALCL−WTREGEN−RRPONTSYD history, vintage conflict rejection and availability-first forward fill. Current RRP billions are converted to common USD millions before subtraction. | WTREGEN vintage is absent. Historical event studies therefore remain BLOCKED; fewer than 96 release-aware business observations also blocks warmup. Composite analog/performance claims stay blocked until all component histories are immutable/PIT. |
| D32 | Liquidity-capacity: complete gross and net firm-book exposures, desks/conflicts, positive-gross zero-net names, modeled dollar amounts and net/gross exit sensitivity. | This is a model desk book, not owner broker capital. Unknown session volume has no inferred capacity. Removed the $500k volume floor and include unknown-volume gross in liquidatable-share denominator. |

`aws/shared/tests/macro_donor_test_support.py` tests the real guarded builders and
actual receiver adapter functions; it also executes the complete capacity handler
with mocked firm book and market data. Ablations cover fresh/stale/zero funding,
incomplete fails, ACM identity failure, unsupported WI/TIC claims, CBS versus LBS,
active versus legacy CISS/date mismatch, missing/conflicting/backdated vintages,
RRP unit conversion, offsetting gross holdings and absent/tiny trading volume.
Each of the seven macro receivers has `tests/run_tests.py`; they deliberately reuse
these regressions. Repeated runner executions are not additional independent tests.

## Genuine daily ledger implementation and limits

Commit `1b6a3e1` adds `research-capital-ledger-1.0` and the production reader.
The full input contract is [LEDGER_CONTRACT.md](../../../aws/lambdas/justhodl-backtest-engine/LEDGER_CONTRACT.md)
(the repository path is `aws/lambdas/justhodl-backtest-engine/LEDGER_CONTRACT.md`).

Input discovery is exact and bounded:

```json
{"source_key":"backtest/ledger/versions/<SHA256>.json","sha256":"<same SHA256>"}
```

The pointer lives at `backtest/ledger/latest.json`, is limited to 64KiB, and addresses
an exact raw-byte SHA256-verified version limited to 25MiB. There is no mutable weekly
fallback, outcome-to-fill converter, synthetic-price fallback or production fixture
upload. Future ledger input prefixes require private IAM-readable storage.

Required inputs are identities/schema/publication, a reconciled flat initial account,
instrument master, immutable exchange-session calendar, explicit sessions/open/close
marks, immutable policy/fold/config and training-label cutoffs, contemporaneous feature
and order snapshots, sequenced decisions/fills/cash/corporate actions, locates/ADV,
daily financing and independent daily cash/liability/receivable/position/NAV
reconciliations. Coverage must explicitly include zero-event families.

The adapter supports USD cash equities/ETFs in a simulated margin account using
trade-date cash accounting. It rejects live owner broker books, FX/derivatives,
non-unit multipliers, non-flat starting books and unsupported corporate actions.
It does not implement settlement-date cash liquidity, a broker connector, strategy
search, execution routing or model/source certification. The synthetic fixture in
`test_research_capital_ledger.py` documents shape only; it is not investable history.

Actual behavior includes chronological complete order snapshots, cumulative partial
fill consumption, accepted side/limit checks, reduce-only constraints, pending-buy
cash reservations, gross/net/name/cumulative participation limits, and capital checks
on decisions with no fills and at every close. Raw marks and split/dividend events
avoid double adjustment. Dividend receivables settle before intraday events/close.
Cash contributions chain flow-time NAV subperiods using actual opening marks, so an
overnight gap is not diluted into a deposit. No-trade sessions still show losses.

The unique ledger regression suite covers these cases. An independent risk-agent
review reproduced three failures in the first candidate (partial order overfills,
intraday dividends and no-fill reservations); all were fixed and re-tested before
commit. It found no remaining blocker within the stated scope. That review is not
certification of a supplied track record. READY means the supplied replay reconciles;
`publication_eligible` remains false. Annualization is null below 252 sessions.

## Strict holds and remaining real inputs

- Owner risk sizing: actual reconciled book/account/currency, cash, liabilities,
  signed positions/multipliers, valid current marks, complete open orders and
  matching actual NAV history. No code turns summed positions or hypothetical
  model books into capital. New entries remain held while this contract is absent.
- Portfolio performance: real historical simulation inputs and independent source
  snapshots described above, available before the relevant decisions. A working
  replay adapter does not create missing history or confer model certification.
- Outcome grading: compatible baseline adjustment provenance and aligned actual
  instrument/benchmark sessions. Legacy or future producer rows without this
  evidence cannot receive fabricated grades. Liquidity-inflection signal logging
  explicitly remains blocked until its producer has verified baseline marks/basis.
- Historical macro validation: WTREGEN vintage and enough release-aware observations;
  all other composite inputs must also have immutable decision-time history.
- Market/provider extensions: actual timestamped WI auction quotes; verified SLT
  transactions/valuation distinctions; BIS LBS USD/sector/FX-adjusted flows; proven
  comparable fails-volume denominators; trailing ADV/spread/depth and out-of-sample
  funding/impact calibration for executable capacity. Current sensitivity views do
  not claim that these missing feeds or calibrations exist.
- Live release: deploy reviewed commits and required shared helpers, establish
  `:live` aliases before protected invokes, deploy authenticated private routes/S3
  denials before owner publishers, and collect fresh outputs with deployed package
  parity. `aws/ops/checks/audit_20260909_accounting.py:inspect_payload(key, doc)` is
  read-only and treats these explicit safe holds as expected states.

Source commits: `7694b4b` accounting truth; `f02bcba` bounded validation modes;
`87022dd` donor contracts; `336affa` macro integrations; `ef1cd7b` BIS compatibility;
`bc3d762` promoted snapshot invocation; `7090342` private snapshot publication;
`233495f` snapshot HTTP guard; `1b6a3e1` daily research ledger. The release team's
source manifest and live verification report determine which commits are deployed.
