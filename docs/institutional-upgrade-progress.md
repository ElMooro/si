# Institutional upgrade implementation ledger

User-authorized objective, 2026-09-18: traceable important measurements, reproducible decisions, and recommendations connected to supported portfolio consequences. Preserve current working repairs, abstention, existing consumer fields and no-paid-AI policy. Production remains on AWS; GitHub Actions deploys with runner IAM.

This ledger records implementation status, not a claim that the fleet is already institutional grade. The September 18 audit and blueprint are the baseline. No model receives sizing authority simply because its code deploys.

## Acceptance workstreams

- [ ] Correct foundational performance grading, provenance and downstream trust.
- [ ] Correct shared macro transformations, source timing and market-tape meaning.
- [ ] Correct Treasury instrument comparability and residual buyback narratives.
- [ ] Extend shared observation identity, immutable capture and point-in-time contracts.
- [ ] Publish coherent, reproducible decision snapshots with complete input manifests.
- [ ] Connect supported portfolio exposures and explicit scenarios to decision explanations.
- [ ] Extend independent-evidence ancestry and validation permissions through consumers.
- [ ] Repair shared page correctness, freshness, navigation and evidence inspection.
- [ ] Validate domain engines and migrate consumers in dependency order.
- [ ] Verify deployment receipts, live artifacts, page behavior and recovery cases.

## Current work

Base: `de15c0ab49c77a0a1a7b9a6d62e199e24ae6de0f` after `git pull --rebase origin main`.

First step: ops 5710 performs a bounded read-only outcome/baseline investigation on the AWS runner. It reports selected fields from public-engine signals only and preserves the original ledger. No diagnosis is considered verified until its committed report is read.

## Stage 1: Treasury measurement integrity (deployed and verified)

- Auction desk 1.3.0 preserves verified TIPS/FRN/nominal/bill identity, separates comparable cohorts, rejects stale curves and excludes prior-close nominal par gaps from demand scores. The observed 2026-09-17 TIPS real yield cannot generate a nominal -235.7 bp comparison.
- Buyback accepted par is not presented as settlement cash, easing or a risk-asset trade. The note is deterministic and requires no paid AI provider. Top-level decision eligibility remains measurement only.
- Auction cards, calendar and tape expose the correct security kind and quote basis; unavailable participation remains null. Existing field names are retained for consumers.
- Ops 5711 verifies the exact source receipt and Lambda code hash, refreshes the data and checks the live economic-contract regressions. Ops 5710 investigates the performance ledger without modifying it.
- Windows deployment checks now use portable Git paths and normalized source line endings. The release ZIP fixture uses exact bytes; production package matching remains strict.
- Validation: deployment static suite 400 checks and 12 candidate shell checks passed; Treasury engine 11 tests; frontend 351 tests. Exact release commit `581be5ea0012eb5d0e71ce2342a72de75959e386`, Lambda run `35364329183`, all push workflows green. Receipt source is 60,972 bytes (full native git push, no Contents API limit).
- Ops 5711 verified the live Lambda code hash and refreshed `data/auction-desk.json` to 1.3.0 at `2026-09-18T15:47:22+00:00`. Public same-origin JSON and browser rendering verified: TIPS row, unavailable nominal comparison, deterministic explanation and measurement-only eligibility. Proxy caching briefly served 1.2.3 while same-origin had 1.3.0; browser was rechecked after expiry.
- Remaining on this page: legacy tenor/funding panels, static historical-return tables and narratives need separate repair; the new desk contract does not validate them.

## Stage 2: performance lineage and research permissions (deployed and verified)

Ops 5710 scanned all 202,552 outcome rows and found 569 rows across the three investigated engines. All 569 lacked entry-mark evidence. A concrete stored outcome compared a 28.47 baseline for bare `BTC` with a 79,899 endpoint and reported +280,542.781876%. The harvester used equity quote symbols while the old checker treated bare BTC as Bitcoin. This is an instrument-identity collision, not investment performance.

- Canonical pricing identities separate token pairs from equity/ETF symbols. Bare ambiguous symbols are not priced without asset identity. Harvested ranked membership is an observation, not an invented UP forecast.
- Scorecard streams the complete ledger, removing its silent 100k cutoff. Verifiable returns require identified, dated entry/exit evidence, compatible currencies and corporate-action vintages, and an explicit direction. Rejected outcomes are counted by reason; original ledger records are retained.
- Benchmark attribution uses paired evidence, never processing-time `checked_at`. Wilson demotion diagnostics use the upper interval bound. Overlapping outcome rows do not gain promotion or sizing authority simply because their raw count is large.
- Trust cannot inherit a boost from a stale/unverified scorecard or unrelated regime overlay. Scorecard page displays evidence gaps and permissions, including a distinct legacy state.
- Ops 5712 checks all four exact release receipts, exercises the two real BTC pricing routes without ledger writes, refreshes scorecard/SSM and trust, and verifies public-artifact expectations.

Important remaining work: capture immutable entry/exit price evidence and source snapshots for new prospective evaluations, register model/protocol versions and benchmarks, establish independent effective samples and out-of-sample studies. Current harvested quote baselines are explicitly context only; this stage prevents false authority rather than inventing missing historical evidence.

Still open: point-in-time raw evidence and snapshot replay, corrected outcome lineage and statistical permissions, macro transformations, validated portfolio consequences, cross-engine independence, and the remaining fleet/page migrations. Existing historical reaction studies have not earned sizing authority.

- Stage 2 proof: `26ba59fdcaf63886b00a4dcbd0d72a4bf8aee754`, deploy run `35366674148`, direct verification `35367362323`. All four exact receipts and runtime code hashes match. Ops 5712 refreshed the full 202,552-row ledger view: 185,231 quarantined, zero verified/promotion authority, zero ledger mutations. Live scorecard and trust JSON generated 16:16:12Z / 16:16:15Z, and the browser shows explicit research-only permissions. Actual quote routes returned distinct Bitcoin spot / BTC ETF prices. The initial serial verification raced creation of the first receipt; rerunning after deployment passed. New verification waits handle an absent initial receipt.

## Stage 3: macro identity, calendar growth and replay (in progress)

- Curated FRED YoY transformations use provider-declared frequency and exact matching prior-year calendar periods. Missing latest or base observations cannot shift the comparison. Source unit, seasonal adjustment, definition, observation/comparison dates and response vintage survive transport. CPI computed from CPIAUCSL is labeled SA YoY; China CHNGDPNQDSMEI remains nominal and stale when its source ends in 2023.
- Raw responses and definitions are conditionally archived under full request/content hashes. Replays verify bytes; capture failures cannot claim evidence. Credentials are excluded from source references. This is application-level conditional storage, not Object Lock certification or reconstructed first-publication history.
- The indicator bus blocks all generic substitutions for curated definitions. Market tape uses COMP for Nasdaq Composite and USD BROAD for the Fed broad index. Quotes require exact symbols and observation timestamps. Ticker dates, missing inputs and evidence are visible; stale packets cannot refresh themselves through a new page load.
- Ops 5713 verifies exact receipts, invokes the chain, reloads archived source bodies, recalculates every available curated YoY and verifies the published bus/tape contracts. Release-calendar-specific freshness and migration of other legacy series remain separate work.
