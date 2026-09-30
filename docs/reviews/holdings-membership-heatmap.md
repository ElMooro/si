# Browser-only reported membership heatmap — draft review

Scope: `etf-holdings.html`, `flow-lookthrough.html`, and shared `jh-etf-holdings.js`. Adds an opt-in selected-fund membership panel; preserves the original fund inspector, security search, comparison, scenario, navigation and IDs. No producer, desk, ARK, PR13, monitoring worker, schedule, registry, workflow or capital-authority changes.

Ownership: main claims and path history checked before work; no target workstream claim or edits in the preceding 72 hours. Based on fetched origin/main. This review file records the scoped draft; shared SESSION_CLAIMS was preserved because it belongs to excluded PR13 work. Branch: agent/shopiz/holdings-membership-heatmap. No merge or deploy authorization.

## Measurement and loading contract

The verified canonical holdings root (or its existing look-through projection) supplies content-addressed snapshot/row references. The new panel never uses directory observed_fund_count as a ranking and never requests membership shards or historical snapshots. It loads complete current row parts for 1–8 explicitly selected configured funds, sequentially, with cancel/retry and an 8 MiB page-local cache. A new attempt is capped at 40 network objects and 8 MiB of declared artifact bytes; retained-object reads reject bodies exceeding the referenced length and verify exact bytes/hash. HTTP errors or interrupted/budget-limited attempts cannot display partial rankings.

Qualification requires complete returned pagination, valid nonfuture publication/acquisition clocks, unexpired source checks, a single chosen effective date, and zero missing/duplicate identities or field errors. Selected snapshots must have full row counts and unique source row IDs; qualified rows must match the date/processing cohort, and equal identity hashes must not have different identifier tuples. Whole funds with unresolved identity coverage are conservatively excluded. Ticker collisions remain separate identities. Qualified membership counts use unique fund identities, not quantities or weight signs.

The panel shows eligible/selected/configured counts and per-fund dates, expiry, exclusion reasons and explicitly unverified configured tags. Raw observed counts remain separate and can include zero/short reported rows. No eligible fund means no ranking, only labelled unranked raw observations. A successful ranking is solely within the named eligible selected sample, never all 300 funds or all securities. Display is bounded to twelve identities; existing search/inspectors expose all rows. Full-universe ranking is deferred rather than approximated from a downloaded subset.

Existing comparisons retain exact current/prior effective dates and separately expose unadjusted quantities and raw weights. No new buy/sell, daily-change, added/removed-trade or favorability score is created. Thirty-day query cutoffs are not daily changes. Same-date revisions/corporate actions are not trades. No aggregate dollar exposures or allocation weights; source units remain unqualified. No fund-of-funds look-through or leveraged/inverse exposure netting.

Refresh/selection changes/cancel discard the panel result. A periodic clock check removes expired qualification; retry reuses immutable verified content and recalculates eligibility. Old asynchronous completions cannot overwrite newer requests. Panel errors do not blank the original research sections.

## Source status and live-read limit

Corrected evidence supplied by the parallel task: current canonical holdings are fresh, 294/300 complete and unexpired; unavailable funds are BERZ, BULZ, FNGD, FNGU, GDXD, GDXU. The ETF desk's 100 reused holdings were stale because it embedded the preceding canonical run, while its 16 extras were current. This change does not use the desk cache. The supplied live observations are not acceptance performed by this branch.

Canonical access from this environment previously returned HTTP 403 / Cloudflare error 1010. No alternate hosts, edge bypass or protected originals were used. Local tests use only the existing clearly synthetic fixture under tests/fixtures. Public live byte/coverage acceptance remains outstanding before merge/deployment. The look-through page continues to verify its own existing root and source clocks; it never silently substitutes a fresher root.

## Cost and rollback

Zero added scheduled invocations, vendor requests, S3 PUTs or recurring storage. Extra public GET/transfer is opt-in and bounded as above, excluding the unchanged page bootstrap/inspector requests. Synthetic ARKK+SPY cold load: 5 verified objects, 240,858 bytes; repeat: 0 network objects/bytes. This is a fixture measurement, not an estimate of actual ARK portfolios. Server-side response rejection/network transport framing may add overhead; no pricing commitment is made.

The browser script remains a direct root static asset; both pages bump its query version to 20260930-membership2. No Lambda ZIP changes. Rollback is to revert this draft's three production files together and restore their previous asset version. No persistent data migration is involved.

## Validation

- Full frontend/worker suite: 2,178 tests passed.
- Focused tests cover partial/stale/future sources, mismatched dates, changing denominators, missing/duplicate identity, ticker collisions, null/zero, unqualified units, same-date revisions, verified artifact loading, cancellation, retries/cache and budget rejection.
- `node tests/holdings-membership-browser.cjs`: both pages at 1440 and 390 pixels. All HTTP intercepted with synthetic data. No page errors or horizontal overflow; opt-in requests, cache, cancellation, error isolation verified. Screenshots retained locally under /tmp; not live acceptance.
- Page script graph: 599 public graphs, zero syntax errors. Wiring: 36 pages / 143 wired, zero missing/stale declarations.
- Page syntax regressions: 6 passed; public boundaries: 15 passed; sovereign assets: 3 passed; offline pages: 6 passed.
- Secret scan, targeted preflight, diff whitespace check and exact staged inventory passed before commit.

Independent review and live acceptance are still required. No predictive qualification or capital permissions changed.

## Independent review follow-up — absolute byte ceiling

Reviewer P2 reproduced an initial-publication verification regression: a correctly hashed 17,825,792-byte padded output was accepted because manifest bytes replaced the loader ceiling. Three new regression tests failed against d5b56438d before the repair (including oversized acceptance and malformed bounds reaching transport).

The loader now validates a positive integer declaration against a code-owned absolute 16 MiB constant before transport. Publication manifests also validate their output length explicitly, including missing declarations; no undefined/default bypass. Both streaming and nonstreaming actual-body limits remain enforced. Added cases cover over-limit, NaN, infinity, fractional, negative, zero, string, null/missing declarations and a legitimate exactly-16-MiB hashed publication. Query versions on both pages advance to membership2. No acquisition, risk, schedule or producer changes.

Follow-up validation: 21 focused tests; full frontend suite and both-page 1440/390 synthetic browser previews rerun. Exact final counts and commit are recorded in the draft PR. Live acceptance remains blocked and no deployment is claimed.
