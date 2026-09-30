# Holdings accounting v1

The snapshot reports descriptive unrealized price P&L on legacy cash-equity
holdings. It does not establish reconciled account NAV, investment performance,
instrument identity, currency conversion, income, fees, tax lots or execution.
Capital-book sizing remains blocked. The existing provider and publication
ordering remain unchanged; neither is newly certified by this repair.

`accounting.schema_version = holdings-accounting.v1` binds the interpretation.
`valuation_at` is one frozen clock for every holding. Original position records
and fetched price frames are retained in the private snapshot. Rejected nonfinite
numbers use a typed string representation so the artifact remains legal JSON.
These are normal producer outputs, not permission to read actual private inputs
for deployment acceptance. Whole invented predecessor inputs/outputs and the
previous source remain inert in `tests/fixtures/pre-snapshot-accounting/`.

Only finite numeric quantities, marks and unit costs are accepted. Booleans,
numeric strings and missing fields are unknown. Actual zero quantity or zero
cost remains zero. A valid mark must be positive and timestamped no more than
120 hours old or five minutes ahead, using unrounded seconds. Display age is
rounded separately. These are existing operational age limits, not proof that
previous-close prices qualify for every valuation purpose.

Rows with invalid or duplicated symbols remain in source order, with their
original record index, and are excluded from arithmetic. A known quantity and
mark establish market value; P&L additionally needs a nonnegative unit cost.
Stored total cost is never trusted: signed basis is quantity times unit cost.
Unavailable or overflowing legs are null with reasons. All sums use unrounded
legs and `math.fsum`, then round once for display. A nonempty sleeve with no
eligible rows is unknown; an explicitly empty book is zero.

The summary separately counts all records, priced records, known basis records
and P&L-eligible records. Partial sums are explicitly scoped and list excluded
symbols. Known unpriced basis has its own count. P&L percent uses the sum of
absolute eligible cost bases, avoiding long/short net-basis cancellation. It is
a descriptive unrealized price-gain ratio, never an account return or NAV ratio.

Legacy `current_weight_pct` describes signed net holdings, and is withheld when
mark coverage is partial. New gross holdings weights declare their priced-only
scope. Target-weight drift is withheld because the target denominator is not
established. A previous close crossing a stop is only a price comparison; it
does not prove an execution. Zero-quantity rows cannot signal a stop crossing.

The complete payload is serialized with `allow_nan=False` before either output
sink is called. Unrelated malformed enrichment can still fail publication;
failure cannot publish a partially valid JSON artifact. Existing authenticated
private publication, AWS settings and hourly schedule bindings are preserved.

Acceptance uses 15 accounting regressions, 21 existing sync/reader regressions,
four actual-handler checks, the admin/risk/scenario suites, page behavior and
isolated browser fixtures. The snapshot candidate validator checks the entire
ZIP and executes these tests offline without runner credentials, networking or
native invocation. Admin production source is unchanged; its shared test update
still causes a deployment and therefore requires its own exact release receipt.
Actual private publication and transaction permissions remain unverified.
