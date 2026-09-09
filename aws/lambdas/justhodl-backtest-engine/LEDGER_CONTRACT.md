# Historical research capital ledger

The backtest engine has two distinct outputs. Legacy signal attribution remains
ineligible for NAV/performance publication. `portfolio_performance` is populated
only by `aws/shared/research_capital_ledger.py`, which replays an independently
supplied immutable event ledger. It never constructs trades from completed outcomes.

The adapter supports a USD equity/ETF simulated margin research book, with a flat
reconciled starting account and trade-date cash accounting. It rejects actual owner
broker accounts, options, futures, FX books, non-unit instrument multipliers and
unsupported corporate actions. Settlement-date cash/liquidity is not modeled.
Publication remains blocked pending independent model validation and provenance
certification, even when the numerical replay reconciles. This is not a broker
adapter, strategy generator or an execution authorization.

## Source discovery

`backtest/ledger/latest.json` must point to an exact content-addressed object:

```json
{"source_key":"backtest/ledger/versions/<64 lowercase SHA256 hex>.json","sha256":"<same SHA256>"}
```

The SHA256 is computed over the exact UTF-8 object bytes. The loader rejects a
mismatched key/hash, invalid document, object over 25 MiB, or missing input. Nothing
writes these inputs from this engine. Input production and independent source
reconciliation are still required; test fixtures must never be uploaded as history.

## Required object fields

The complete runnable shape is exercised by `aws/shared/tests/test_research_capital_ledger.py`.
Its `fixture()` is synthetic test data, not a sample investment track record.

| Field | Required meaning |
|---|---|
| `schema_version` | `research-capital-ledger-1.0` |
| `book_type`, `account_type`, `accounting_basis`, `currency` | `SIMULATED_RESEARCH_BOOK`, `RESEARCH_MARGIN_ACCOUNT`, `TRADE_DATE_CASH`, `USD` |
| `ledger_id`, `book_id`, `account_id`, `generated_at` | Nonempty research identities and actual publication timestamp |
| `initial` | `as_of`, `available_at`, `source_snapshot_id`, positive cash/equity, nonnegative liabilities, zero receivables and empty positions; NAV reconciles to cash minus liabilities |
| `instruments` | Symbol map of `asset_class` EQUITY/ETF, currency USD, multiplier 1 |
| `calendar` | Verified immutable descriptor; payload includes exchange XNYS and exact ordered session dates |
| `sessions` | Explicit date/open/close/valuation timestamps every held instrument's raw closing mark, and independently sourced complete closing order snapshot; no inferred weekdays or forwarded prices |
| `policies` | Immutable strategy/constraint payload, policy/fold ID, training end/label cutoff, embargo hours, availability and evaluation boundaries |
| `decisions` | Decision/policy IDs, decision timestamp/sequence, immutable feature inputs, contemporaneous raw risk marks, and complete open-order snapshot captured at that instant |
| `fills` | Unique fill/source IDs, symbol, decision/order ID, signed quantity, actual raw price/fees, execution/availability timestamps, event sequence, trailing dollar ADV descriptor and short locate where required |
| `cash_events` | Unique source/events, timestamp/sequence, amount and EXTERNAL_FLOW or FEE; external flows supported only at session open |
| `actions` | Explicit sourced SPLIT ratio or DIVIDEND cash/share, effective opening timestamp, sequence, actual availability; dividend payment timestamp |
| `financing` | Every session, including zeros: source/availability, cash interest, margin interest, per-symbol short borrow fees |
| `reconciliations` | Every session's independent source snapshot: cash, liabilities, dividend receivables, signed quantities and equity NAV |
| `coverage` | Explicit true completeness assertions for fills/cash/corporate_actions/marks/financing/reconciliations/orders, including zero-event sources |

A verified immutable descriptor contains `source_record_id`, `observed_at`,
`available_at`, `payload`, and SHA256 of the canonical payload JSON (sorted keys,
compact separators, no NaN). Calendar/config/features/risk marks/liquidity/locates
must be known by their relevant decision. Closing marks must belong to the exact
closing session and arrive by valuation time. These timestamps come from the
upstream producer; this adapter does not backdate them.

Policy constraints are fractions of reconciled equity: `gross_limit`,
`absolute_net_limit`, `name_limit`, `participation_limit`, plus `minimum_cash`,
`max_risk_mark_age_seconds`, and `max_liquidity_age_hours`. The adapter checks each chronological decision (including decisions with no fill),
post-fill holdings plus remaining open-order reservations, cumulative same-name
session turnover against trailing dollar ADV, and closing capital constraints.
Shorts require available share locates and explicit daily borrow-fee coverage.
Pending orders contain order ID, symbol, BUY/SELL side, remaining quantity and limit price. Open buy orders also reserve cash. Fills must reference a live accepted order and consume its remaining quantity cumulatively. Repeated fills cannot exceed that remaining quantity. Reduce-only orders must match the direction and aggregate magnitude of existing holdings. Complete closing order snapshots provide independent provenance for cancellations/replacements; absence is never interpreted as an empty book.

Splits change share quantities and require unadjusted marks. Dividends accrue a
receivable/payable at the ex-session opening and move it to cash at payment time, including before intraday events and closing valuation. Fills charge actual supplied fees and each calendar session charges supplied
financing. Open-session external contributions/withdrawals require actual raw opening marks. Returns chain the pre-flow and post-flow subperiods, preserving overnight price gaps without attributing a capital deposit to performance. A no-trade session still
marks every position and can record a drawdown.

Any missing session, stale/misaligned mark, future feature, checksum disagreement,
unsupported action, insufficient locate, capital breach, or reconciliation failure
invalidates the entire replay and returns `BLOCKED` with empty curves. There is no
partial-performance fallback. A valid replay returns `READY`, daily NAV/returns,
flow-adjusted compounded return and daily drawdown. Annualization remains null below
252 sessions. Top-level publication is blocked independently of `READY`.

## Verification

Run `python aws/lambdas/justhodl-backtest-engine/tests/run_tests.py`. Tests exercise
the real ledger adapter and real immutable S3 reader with only in-memory I/O. They
cover no-trade drawdown, overlapping positions, open-order reservations, tiny ADV,
fees, split/dividend accounting, deposit neutrality, shorts/borrow, future
features/training leakage, corrupted source checksums, daily reconciliation,
unsupported instruments and owner-data rejection. No test invokes AWS or a provider.
