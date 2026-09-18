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

## Stage 3: macro identity, calendar growth and replay (deployed and verified)

- Curated FRED YoY transformations use provider-declared frequency and exact matching prior-year calendar periods. Missing latest or base observations cannot shift the comparison. Source unit, seasonal adjustment, definition, observation/comparison dates and response vintage survive transport. CPI computed from CPIAUCSL is labeled SA YoY; China CHNGDPNQDSMEI remains nominal and stale when its source ends in 2023.
- Raw responses and definitions are conditionally archived under full request/content hashes. Replays verify bytes; capture failures cannot claim evidence. Credentials are excluded from source references. This is application-level conditional storage, not Object Lock certification or reconstructed first-publication history.
- The indicator bus blocks all generic substitutions for curated definitions. Market tape uses COMP for Nasdaq Composite and USD BROAD for the Fed broad index. Quotes require exact symbols and observation timestamps. Ticker dates, missing inputs and evidence are visible; stale packets cannot refresh themselves through a new page load.
- Ops 5713 verifies exact receipts, invokes the chain, reloads archived source bodies, recalculates every available curated YoY and verifies the published bus/tape contracts. Release-calendar-specific freshness and migration of other legacy series remain separate work.

- Stage 3 proof: `97cde1a1be648d8e29fcac074e179ae7a6d2ae5f`, Lambda run `35369180860`, pages `35369180802`, ops `35369180850`. Exact receipts/runtime hashes passed. Ops 5713 replayed 15 curated macro series and verified 9 ticker evidence objects. Vault/bus/tape generated at 16:42:17Z/16:42:21Z on September 18. Browser verified the corrected COMP/USD BROAD labels, dates, source links and explicit gaps. Deployment 400+12, TradingView 13, bus 2, tape 4 and frontend 356 checks passed.

## Stage 4: measurement envelopes and canary diagnostics (deployed and verified)

Live SOMA/BLS/BEA packets contained `wrap() missing ... field` errors. Canary ICSA and IC4WSA shared a trace ID and used processing time as observation time. The shared adapter contract and callers are being repaired together.

- Measurement v2 separates observation, receipt and processing clocks; includes series/value/unit/vintage in content traces; retains zero confidence; distinguishes structural source labels from replay proof; never grants sizing permission.
- Warm bridge preserves exact warehouse input bytes and actual monthly/quarterly observation identities. SOMA includes all seven total components, excludes separately reported TIPS inflation compensation from the sum and reports reconciliation. BLS excludes M13 annual averages. Treasury datasets whose collectors discarded dimensions cannot become arbitrary scalar readings.
- Canary retains complete original FRED/BLS responses, including each per-series fallback, rather than truncated or synthesized raw evidence. Core definitions and age ceilings govern diagnostics. RRP billions are normalized to millions; the legacy reserve ratio is explicitly a mixed-frequency proxy. Curve movement uses matched-date yield changes. A Chauvet/Piger probability is not substituted for the unavailable Cleveland model. Missing units, stale dates and failed captures stay explicit; permissions remain research-only.
- The quality report counts the full JSON tree and separates structural envelope coverage from replay verification. Legacy trend entries and the configured guard list are not reinterpreted as runtime proof.
- USD BROAD observes the weekly H.10 publication schedule for its daily observations (11-day observation-age ceiling); daily rate ceilings remain 7 days.
- Remaining: full release calendars, original-source lineage for legacy warehouse records, dimensional Treasury collector repair, complete definition catalog, and migration beyond these producers. A provenance envelope alone does not certify a source or forecast.

- Stage 4 proof: `512b6ff78bd45d21bcec07f4833717716b0111cb`, deploy `35371960934`, ops `35371961381`. All five exact receipts and runtime hashes matched. Ops 5714 replayed 63 FRED series and the complete canary diagnostics, and SOMA/BLS/BEA warehouse projections. Canary generated 17:05:11Z; SOMA 17:04:36Z, observed September 16. Public JSON and source receipts are HTTP 200 with a normal browser user agent. Deployment 400+12, warm bridge 28, canary 9, TradingView 13, tape 5 and coverage report fixtures passed.

### Stage 4 follow-up: upstream BLS vintage (deployed and verified)

The replay exposed a separate live fault: the newest CPI warehouse observation was 2019-M12 (256.974). The collector requested 2000..2026, beyond BLS's 20-year registered request limit. It now requests a supported window ending this year, preserves older periods, archives the exact original response and the previous warehouse vintage, and refuses to replace history after a failed read or empty response. The bridge adds explicit per-measurement observation-period freshness so a current processing clock cannot make a 2019 observation current. Ops 5715 verifies a current CPI observation against original BLS bytes and preservation of the old history.

- BLS follow-up proof: `0a29d338bf58ffe30da305e0418e585e10cd47d3`, deploy `35373094020`, ops `35373093898`. Ops 5715 verified exact receipts/hashes, the 2007..2026 request, all 15 series updated, CPI 334.98 for 2026-M08 and all 240 old periods retained (320 total). Original BLS bytes and the prior warehouse vintage replayed; public JSON generated 17:15:14Z with correct observation age. Deployment 400+12, collector 7 and bridge 29 tests passed; both public release verification commands passed.

## Stage 5: reproducible Calls research and independent audit (deployed and verified)

- Typed public input projections are frozen in conditionally written content-addressed research runs. Each binds seven input projections, the compiler's source hashes, generated time, twelve displayed evidence fields and the exact deterministic output. Unknown scope labels remain invalid; arbitrary narratives and account fields cannot enter the public archive.
- Calls history binds the brief hash, measurement evidence IDs and replay reference. Shared FR2004 scopes remain one source group rather than six votes; unmapped composite ancestry is explicit. No model or sizing permission is invented.
- Current brief publication uses conditional writes and refuses to replace a newer run with an older one. Retained runs survive publication failures for inspection; private mirror failure still prevents current/account/decision publication.
- The independent audit Lambda reads public artifacts only, reproduces the compiler output, verifies current-public/history binding, writes audit events/proofs and fails on content or authority mismatch. Ops 5716 installs a 15-minute schedule and verifies the full chain. The source replay CLI requires no AWS credentials and never executes archived code.
- Calls page exposes dates, units, source links, overlap, replay records and a matching independent proof. Original-provider verification, private portfolio-state replay and validated forecast authority remain separate acceptance workstreams.

- Stage 5 proof: `b2fac9f21677a2cf6373fbf26de47fddc4dd9620`, deploy `35374938628`, pages `35374938590`, ops `35374938746`. All four exact receipts/runtime hashes passed. Seven retained projections and twelve evidence fields reproduce output `ff6e8f55a6663a784ddd603919d144964f0cb6ca969364106d3516a6b00c054f`; independent replay is scheduled every 15 minutes. Public brief generated 17:34:36Z, matching browser proof 17:35:08Z. Source CLI reproduced the downloaded public record. The live disclosure opened successfully and displayed the two overlapping FR2004 scopes. Deployment 400+12, frontend 359, privacy/Calls/replay and independent-auditor fault tests passed. Position-sizer-v2 is private; its exact runtime receipt was verified without trying to expose its account output.

## Stage 6: dated private holdings risk and consumer permissions (deployed and verified; real-provider probe follow-up)

- Replace array-position return alignment, default volatility/correlation/beta, duplicate-lot overwrite and net-value denominators with matching observed-session intervals, one common sample, explicit missingness, signed lot aggregation and separate gross exposure. Minimum 60 return intervals; zero measurements remain zero. This is split-adjusted price risk, not total-return account performance.
- Preserve original market response bytes and frozen private snapshot/model inputs beneath the existing protected portfolio-risk archive prefix. Deterministic replay binds the exact model code and output; private data never enters the public report. Conditional archive writes are application-level retention, not an Object Lock claim.
- Account-normalized risk requires a reconciled, matching USD capital book and NAV = cash + signed marked positions - liabilities. Holdings-only risk remains separately labeled when NAV is unavailable. Static legacy crisis assumptions remain hypothetical; missing sector mappings cannot borrow SPY's return. ETF look-through remains unverified pending constituent reconciliation.
- Retire the old sizer's unsupported alpha-score-to-return/share allocations; preserve observed holdings and WAIT with null targets. PM keeps hypotheses separately and publishes no authorized trim/add/hedge actions. Fix its dict/list scenario mismatch. These research compilers send no automatic messages.
- Portfolio, sizing and PM pages expose unavailable NAV, sample dates, limits and withheld allocation authority. Ops 5717 checks exact releases, original-response/private-input replay and unauthenticated S3/edge denials without reporting personal values.

- Stage 6 proof: `d8461363475ee29fd74988a5f34433ae60195d5a`, deploy `35377853943`, pages `35377854007`, ops `35377853888`. Three exact receipts/runtime hashes match. Private replay generated 18:04:29Z and reproduced; direct S3 and edge access denied; legacy sizing withheld and PM WAIT. The live account snapshot has no positions, so this specifically proves the empty-account path, not live held-security covariance or original quote capture. Ops 5717's generic source-bytes wording does not establish populated market-input coverage. A validation-only synthetic SPY case and ops 5718 provide a separate real-provider check, without account reads or writes. Ten mathematical/replay regressions, private handlers, deployment 400+12 and frontend 363 passed. Curl and browser verified the live page labels, script tag, private sign-in state and unavailable risk (no false zero). Browser QA also identified the sizing login being mislabeled as an engine error; the follow-up corrects that and clears prior displayed private values on failed refresh.
