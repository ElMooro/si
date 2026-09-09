# Accounting release evidence and remaining data requirements

Prepared 9 September 2026. This appendix certifies inspected source changes and
offline behavioral evidence, not deployment or investment performance. The release
team must attach its own promoted-version hashes, output timestamps, privacy checks
and page results. No cloud calls or deployment actions were performed to prepare
this appendix.

The re-audit compared original source `34ddd51` with `6c594c3`; implementation began
on `125a68a`. Claude had repaired the narrow INST-07 defect, but INST-08–13 retained
material failures. The following source repairs address those reproduced cases.
Explicit data holds are intentional outputs, not evidence that missing data exists.

## Original findings: repaired behavior and remaining boundary

| ID | Repaired and verified in source | Remaining data or release condition |
|---|---|---|
| INST-07 | Actual position updates preserve finite inputs, quantity/cost/sign consistency and existing-position/concurrency guards. Snapshot refresh invokes promoted `:live`. | Promoted snapshot alias must exist before the admin caller is released. This does not supply a broker reconciliation. |
| INST-08 | Snapshot rejects stale/future/missing/nonfinite marks; unpriced values, P&L and stops stay null. Mixed priced/unpriced sector aggregation no longer crashes. Position market value is not mislabeled account NAV. | `capital_book` stays BLOCKED without reconciled book/account/currency, cash, liabilities, signed positions/multipliers, current marks, complete open orders/reservations and matching NAV history. No owner ledger adapter or verified capital was fabricated. |
| INST-09 | Immutable unique availability-stamped versions, UTC ordering, no mutable weekly performance fallback. Snapshotter model now owns `calibration/model-latest.json`; calibrator report exclusively owns `calibration/latest.json`. ETag conditional indexes recover orphaned versions and cannot lose concurrent updates or regress latest. | Existing report `latest.json` may still contain old snapshot-schema bytes. It needs a real calibrator refresh. Current calibrator has no proved quiet/publish-only mode and normally emits `EVT_CALIBRATOR_WEIGHTS_UPDATED`; a release must not silently invoke it as a quiet read. Until scheduled/report refresh is observed, report availability is pending. |
| INST-10 | Attribution metrics are excluded from NAV/annualized-performance/publication claims. A separate daily marked, capital-constrained research ledger adapter processes independently supplied fills, cash, marks, actions and financing, with strict daily reconciliation and point-in-time policy inputs. | **No real historical ledger was uploaded or manufactured. No audited performance certification was obtained.** Missing inputs return BLOCKED with empty curves. Numerically READY research replay still has `publication_eligible=false`; it cannot authorize trading or represent live owner capital. |
| INST-11 | Actual historical builder works, deduplicates historical decisions, joins entry-time regime and matching available critique, and uses the actual paired-alpha sample for significance. | Missing contemporaneous historical context remains absent. Historical associations are descriptive and do not certify causal efficacy or out-of-sample performance. |
| INST-12 | Outcome grading requires actual aligned instrument/benchmark sessions, proven adjustment basis and compatible baseline provenance; current-day unfinished bars defer. Missing evidence is pending/UNSCOREABLE, never a fabricated zero return or grade. | Legacy baselines without provenance cannot be retroactively made valid. No rewrite of existing completed grades was claimed. Liquidity-inflection logging remains held until verified baseline price/basis is supplied. |
| INST-13 | Failed reads, malformed/future timestamps, partial expected-artifact validation and coverage gaps cannot report FRESH/HEALTHY. Nested and declared contracts are checked; unknowns remain explicit. | A registry or dynamic family without complete ownership/expected-output evidence still yields UNKNOWN coverage. Freshness is not financial-data or model certification. |

The actual behavioral runners are under each named Lambda's `tests/run_tests.py`.
Backtest/calibration tests use the actual producer and reader. They interleave two
actual snapshot handlers after one has read an old ETag, observe the conflict,
and verify both immutable versions plus the newest model pointer. Additional cases
cover same-second versions, UTC cutoff, denied index reads, explicit retry exhaustion
and orphan recovery. The ledger suite checks bookkeeping using independently specified
synthetic fixtures, including no-trade losses, partial fills, cash reservations,
intraday dividends, external-flow neutrality, shorts and daily reconciliations.
These fixtures remain tests, not investment history.

Snapshot, calibration snapshotter, backtest and research backtest support
`mode=validate_only`: real computation with an `audit-accounting-1.0` envelope and
zero business writes/invokes. Outcome and freshness do not claim this mode. The
read-only output checker is `aws/ops/checks/audit_20260909_accounting.py`, whose
`inspect_payload(key, doc)` returns violations; it now expects the model-specific
calibration key. Release verification must use the same source generation.

## Donor requirements by original dependency ID

All source integrations below are implemented through guarded, dated receipts in
`aws/shared/donor_contract.py` and `aws/shared/macro_donor_inputs.py`, plus the
recipient adapters. Source artifact identity, units, generation/observation dates,
coverage and errors remain inspectable. Repeated economic observations do not gain
extra independent votes. The table distinguishes missing provider evidence from
already implemented context; it does not require a new feed where one is sufficient.

| ID / recipient | Exact donor and data now consumed | External inputs or validation still required |
|---|---|---|
| D01 / liquidity-credit-engine | `data/repo-market.json`: stress score, distribution tails/quantiles/z/percentile, spreads, facilities, reserves, calendar; verified funding review floor. | Valid observation times for each facility/distribution, including zero-use facilities. Out-of-sample calibration is required before the explicitly heuristic review can become a validated execution signal. |
| D02 / bond-warroom | Same repo artifact and dated contributors beside rates/settlement evidence. | Continue current daily observations. Same-day causal claims require synchronized intraday funding/price evidence; daily repo and weekly fails are retained as different horizons. |
| D03 / liquidity-capacity | Same repo artifact, guarded state and 20%/10%/5% participation sensitivity. | Historical execution/impact outcomes, trailing ADV, spread/depth and a separately validated funding-to-impact model. Executable funding-adjusted capacity remains BLOCKED_UNCALIBRATED; repo is not a stock liquidity quote. |
| D04 / bond-warroom | `data/nyfed-primary-dealer.json`: tenor inventory, net positions, weekly changes, z-scores, financing and transactions. | For fails normalization, actual matched-week transaction volume with matching Treasury/collateral coverage and proven units. Inventory alone is not a valid turnover denominator. |
| D05 / repo | `data/settlement-fails.json`: Treasury FTD/FTR/gross, all collateral classes, totals, historical diagnostics and weekly date. | Complete reconciled receive/deliver/class totals and actual observation dates. The existing NY Fed weekly evidence is sufficient for its stated settlement context; daily DTCC claims would require a separate daily source. |
| D06 / bond-warroom | Same full settlement family, paired with dealer/funding context and no duplicate score vote. | Matched weekly dealer/fails observations for normalized comparisons; synchronized daily settlement evidence if a daily claim is proposed. |
| D07 / bond-warroom | `data/term-premium.json`: official ACM latest/deltas/z/percentile, curve and fitted/risk-neutral/premium decomposition. | Continue dated official model observations. Identity must reconcile within 2bp. Model uncertainty/independent policy-expectation evidence is required before treating the residual as an observed expectation. |
| D08 / bond-warroom | `data/auction-grades.json`: CUSIP, auction/issue dates, tenor, accepted size, all graded dimensions and tenor summary. | Actual comparable when-issued yield, quote timestamp preceding the auction, auction timestamp and CUSIP/tenor matching. No WI quote was obtained or invented; unsupported tail claims remain BLOCKED. |
| D09 / bond-warroom | `data/tic-flows.json`: guarded holdings context and separate transaction contract. | Populated official SLT transaction amounts, source dataset identity, valid period/publication time and distinct valuation changes. Probe totals/transactions were empty. Holdings declines and indirect auction awards cannot stand in for foreign sales/purchases. |
| D10 / eurodollar-plumbing | `data/bis-crossborder.json`: CBS consolidated all-currency claims, counterparties, offshore/EM regions and periods. | A distinct BIS LBS USD currency/sector panel, FX/break-adjusted changes, periods and release times. Current CBS context is retained honestly; USD funding panel remains BLOCKED_MISSING_LBS. |
| D11 / euro-fragmentation | `data/ciss-stress.json`: active CISS country/sovereign series, legacy definitions and provenance, alongside same-date 10Y country/Bund spreads. | Current comparable country/Bund observations and active CISS definitions. Dates must align for a joint confirmation claim; mismatches stay visible context. No new source is required merely to display the existing valid context. |
| D12 / liquidity-credit-engine | Same CISS composite/regime/series/provenance/frequency, with explicit overlap exclusion. | Continue fresh active-series observations. Adding numeric weight would require incremental, out-of-sample evidence beyond overlapping credit/financial-market inputs; current contribution remains contextual. |
| D13 / backtest-engine | `data/vintage/_index.json` and `data/vintage/{UNRATE,DGS10,WALCL,RRPONTSYD}.json`: dated value, `known_on`/`available_at`, preserving donor `updated`. | Complete historical feature snapshots with source release timestamps and immutable decision-time availability. Date-only releases are usable next UTC day, not intraday. The integration does not claim full reconstruction of historical strategies or all model features. |
| D14 / liquidity-inflection | `data/vintage/{WALCL,WTREGEN,RRPONTSYD}.json`: availability-first net-liquidity reconstruction, conflict rejection and common USD-million units. | **WTREGEN vintage was absent in the probe.** All three histories, reliable known-on/release times, at least 96 available business observations for warmup, and immutable histories for every additional composite input are required. Historical flip/lead-lag/analog claims remain BLOCKED. |
| D32 / liquidity-capacity | `data/firm-book.json`: full gross/net name/desk exposures, offsets and conflicts; no positive-gross/zero-net omission or invented volume floor. | Actual dated instrument volume/ADV and market microstructure data for capacity; actual broker positions/orders/fills if the use case moves from modeled desk allocation to execution. This donor is a modeled book. |

Guard/ablation regressions run the real builders and actual capacity handler using
in-memory I/O. They cover zero versus missing data, staleness, incomplete fails,
ACM identity, WI/TIC unsupported claims, CBS/LBS distinction, CISS date/definition
mismatch, vintage availability/unit conversion, offsetting positions and unknown
volume. Seven receiver runners share these cases; repeated execution is not counted
as additional independent evidence.

## Ledger input boundary

The implemented schema is `research-capital-ledger-1.0`, scoped to USD cash
EQUITY/ETF instruments with multiplier 1, a reconciled flat starting simulated
margin account and trade-date cash accounting. It rejects actual owner broker
books, nonflat starts, derivatives, FX and unsupported corporate actions.
Settlement-date liquidity, a broker connector, strategy generation and independent
model/source certification are outside this adapter.

An authorized upstream history producer must supply the exact content-addressed
object declared by `backtest/ledger/latest.json`, under
`backtest/ledger/versions/<SHA256>.json`. Required data include:

- Research book/account identity and independent initial cash/liability/NAV snapshot.
- Immutable exchange calendar and every session's actual raw open/close marks.
- Policy/config hashes, folds, training-label cutoffs, embargo and feature availability.
- Ordered decisions, complete open-order snapshots, fills/fees, liquidity evidence
  and short locates; closing order provenance includes cancellations/replacements.
- Explicit cash flows, splits/dividends and settlement times; daily financing/borrow.
- Independent daily cash, liability, dividend receivable, signed position and NAV
  reconciliation; completeness assertions also cover zero-event families.

The detailed contract and synthetic shape example are in
`aws/lambdas/justhodl-backtest-engine/LEDGER_CONTRACT.md` and
`aws/shared/tests/test_research_capital_ledger.py`. Neither is a supplied real ledger.
Missing/bad evidence invalidates the whole replay; there is no partial-performance
fallback. READY only means the supplied numerical replay reconciles. Publication
remains false pending independent certification, and annualization is null below
252 sessions.

## Final source commits and release checks

| Commits | Accounting-owned change |
|---|---|
| `7694b4b`, `f02bcba` | Core INST repairs and bounded read-only validation modes. |
| `87022dd`, `336affa`, `ef1cd7b` | Shared donor contracts, macro integrations and compatible BIS display aliases. |
| `bc3d762`, `7090342`, `233495f` | Promoted snapshot invocation, private publication and HTTP guard. |
| `1b6a3e1` | Real research ledger adapter, immutable reader, page gates and bookkeeping regressions. |
| `22199a1` | Calibration model/report ownership split, conditional indexes/recovery and aligned readers/checker. |
| `b99ff75` | Personal trade journal HTTP guard and private ledger/statistics mirrors. |
| `b8b0747` | Personal calculator HTTP responses are stateless/no-store; only trusted default scenarios publish labeled examples. |

The last two privacy fixes supplement the accounting review. Source checks prove
anonymous personal trade reads are denied before account access and caller-specific
calculator scenarios cannot overwrite public snapshots. Worker authorization,
legacy snapshot cleanup, historical denial and page POST transport are security
release work and require their own live evidence.

Before release closure, match promoted source/config hashes, establish protected
`:live` targets, verify private routes/public S3 denials before owner publication,
and inspect newly generated outputs. In particular, verify both distinct calibration
schemas and record the calibrator report as scheduled pending until genuinely
refreshed. Do not remove capital, missing-history, unknown-freshness or publication
holds to make a dashboard appear complete. Their disappearance requires the stated
real data and independent validation, not a cosmetic status change.
