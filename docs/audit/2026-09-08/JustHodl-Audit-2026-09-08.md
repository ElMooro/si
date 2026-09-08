# JustHodl.AI institutional engineering audit

**Date:** 8 September 2026  
**Source revision:** `34ddd51f2fe59426a43bef7f00946d8e98143727` in the public `ElMooro/si` repository linked from JustHodl's data page.  
**Decision:** I would not approve the reviewed implementation for institutional production capital, private client research, or a validated performance record until the critical findings below are resolved.

The platform has substantial data collection and many useful specialized calculations. Its principal weakness is consistency: account authorization, capital permission, numerical definitions, historical knowledge time, output ownership, and display contracts are not enforced across the fleet. More engines and more data will not repair those failures. The existing data plane should be made reliable and usable before broadening it.

This audit includes exhaustive **inventory and automated source screening**, targeted human-style code review, live browser observations, and isolated synthetic reproductions. It is **not a completed line-by-line, live-payload, or production certification of every engine and page**. Those distinctions are explicit in the accompanying inventories. No production code, configuration, customer record, credential, or deployment was changed. No live vulnerability exploitation or trade execution was attempted.

## What was actually covered

| Surface | Coverage | What it establishes |
|---|---:|---|
| Live engine directory | 860 entries | Observed engine names, displayed status and page associations; directory claims are not accepted as ground truth |
| Source engine inventory | 860/860 have source | Individual source paths, candidate reads/writes, registry differences and proposed review/data actions |
| Python syntax | 1,182/1,182 files parse | Syntax only; no assurance of calculation, authentication or production correctness |
| Public HTML source candidates | 510 | 464 root HTML files, 44 first-level index pages and two additional tool/web pages; includes 17 redirects, not 510 unique dashboards |
| Frontend syntax | 821 inline scripts + 46 root JS files pass | 867 syntax checks; no imports or application code executed in this screening |
| Live page sweep | 143 captured page views from 164 distinct attempted routes | Rendered text and diagnostic snapshots, often early load state; not full interaction or all-field verification |
| Command desk | Additional captured live view | Authoritative 50% cap and embedded Katlin 75% cap simultaneously visible |
| Page/field completeness | Not certified for any whole page | Specific missing/misbound fields and renderer limits proven; complete production payloads and coverage fixtures still required |
| Existing tests | 42 frontend + 70 risk/fusion/Khalid tests passed; 8 deployment checks passed | The present suites can pass despite the independently reproduced defects |
| Focused reproductions | Security, accounting, historical joins, risk maths, renderer/scanner and API readiness | Actual source functions evaluated with synthetic inputs, fake storage and no production requests |

The browser later suffered repeated session/navigation failures. Retries did not recover it. Those are recorded as **audit-environment limitations**, not website outages. Initial loading text, empty tables during loading, redirects, console messages from earlier pages, and cloud-browser errors have not been counted as confirmed product bugs. The static inventory covers the routes that could not be rendered. No authenticated workflows, mobile viewport suite, complete chart series comparison, or all-control interaction test was completed.

The local manifest was generated on September 7; the live directory snapshot was observed on September 8. Differences can reflect deployment drift as well as generator defects. Deployed Worker/Lambda code hashes, IAM, WAF, schedules, current output schemas, full raw histories and broker reconciliation were unavailable. Source defects are conditional on the affected code being deployed; the report does not allege customer impact or actual investment losses.

## Findings that should control the roadmap

| Priority | Finding | Evidence and practical impact | Detailed reference |
|---|---|---|---|
| P0 | Private Brain/Journal authorization fails | Anonymous synthetic reads and writes succeed; user ID length acts as authentication, and debug enumerates identities | INST-01 |
| P0 | Anonymous userdata can reach authenticated records | Guest lookup falls back to the authenticated namespace; reproduced with fake data | INST-02 |
| P1 | Source credentials and Origin-based privilege bypass | Administrative/provider literals are present; a caller-controlled Origin grants Enterprise access across seven consumers | INST-03, INST-06 |
| P1 | Competing capital authorities | Live home shows 50% while Katlin says 75%; Katlin source omits the authoritative risk artifact | FR-01 |
| P1 | Missing/stale risk can increase permission | A year-2000 gate with other feeds absent produces FULL_RISK/100%; explicit zero multiplier can leave 10% | FR-02 |
| P1 | Published position limit is violated | Synthetic recommendations allocate 9.21% despite declaring an 8% maximum | FR-03 |
| P1 | Portfolio and drawdown calculations can fabricate safety or profit | NAV 100→0 produces zero drawdown; quantity-only edit creates false profit; absent market price becomes cost | FR-05, INST-07/08 |
| P1 | Historical tests use future information | Sunday model weights apply to earlier trades; today's risk/critic feeds alter past signals | FR-06, INST-09/11 |
| P1 | Headline NAV does not represent a tradable daily book | Full future horizon return is booked on entry day; overlaps and capital constraints are omitted | INST-10 |
| P1 | Monthly risk indicators use daily repeated observations | Sahm fixture reports 0 instead of 1 percentage point; truck YoY reports 0 instead of -10% | FR-08 |
| P1 | Pages display the wrong engine's feed | Seven confirmed producer/output mislabels; risk-gate producer and page use different field names | Page coverage; FR-09 |
| P1 | Wiring status does not prove complete rendering | CSS words count as outputs; generic cards omit later arrays, fields and rows; registries drift | Page coverage |
| P1 | Fusion rejects a real producer and health accepts invalid models | Credit alternative-field validation fails; empty/ancient documents can return healthy | FR-10; Fusion API |
| P1 | Execution liquidity is modeled with the wrong observable | Daily high-low range is a 35%-weighted spread proxy, not a measured bid–ask spread | Data requirements |
| P1 | Release gates do not protect the whole platform | Worker deploy has no test job; behavioral/alias gates cover only a small engine subset | INST-14 |
| P2 | Degradation and interface labels are unreliable | Intentional fusion exclusions force DEGRADED; calendar can say schedule loaded while reporting failed data | FR-11; live findings |

P0 means immediate security containment should be evaluated against the deployed revision. P1 means a release blocker for the relevant institutional capability. A recommendation-only/shadow component is not alleged to have placed trades. P2 items still matter to operator trust but do not by themselves prove an execution control bypass.

## Live observations that are supported by captured evidence

The engine directory displayed **710 WIRED**, **40 ORPHAN · FRESH**, **12 ORPHAN · STALE**, **10 ORPHAN · DEAD FEED** and **88 NO OUTPUT DECLARED**. The header groups the last two orphan categories as 22. These are its own labels, not 150 independently confirmed broken engines. Infrastructure/API engines can legitimately lack a dashboard artifact, and our source counterexamples prove both false-positive and false-negative classification modes.

| Page | Observed behavior | Required response |
|---|---|---|
| Command desk `/` | Authority says INVEST SELECTIVELY, cap 50%, fusion DEGRADED; embedded Katlin says FULL RISK, cap 75% | One binding authority; clearly separate research opinion from effective permission |
| `ai_predictions.html` | Prediction load fails with Failed to fetch | Trace the requested endpoint/auth/CORS/provider response; show explicit non-actionable state and timestamp; root cause not proven from this message alone |
| `auction-crisis.html`, `correlation.html`, `crisis.html` | AI synthesis reports provider HTTP 400 | Validate shared request model/payload and capture sanitized provider error; preserve valid numeric panels separately |
| `eu-dump-radar.html` | AI briefing reports HTTP 400 | Same request/contract investigation; no claim the numerical engine failed |
| `econ-calendar.html` | Reports HTTP 429 and unavailable schedule while also saying Schedule loaded and showing zeros | Failure must not imply zero scheduled events; separate unknown from a verified empty calendar; retry/backoff/cache policy |

These messages were observed during 14:02–14:05 UTC. Home data changes during the session; numeric values here are audit snapshots, not current trading advice. The raw Khalid fields WAIT_FOR_CONFIRMATION, risk_score 78 and status OK have different semantics and are **not** counted as a numerical contradiction: they describe entry readiness, observed risk severity and input health respectively. The page should label those meanings.

## Specific engine connections to implement first

The package contains **34 curated donor-to-recipient contracts**, plus the risk/portfolio contracts below and the individual engine inventory. Existing references are labeled as static evidence; every proposed edge needs field-level verification and a test of its actual effect. Reuse already banked data, with validation, before creating a duplicate collector.

| Recipient | Donor and exact data | Purpose |
|---|---|---|
| Katlin and Risk Sizer | `khalid-risk`: `policy.allows_new_entries`, `exposure_cap_pct`, `hard_vetoes`, `source_health`, `generated_at`, schema version | Enforce one portfolio permission/cap and carry its veto reasons into every recommendation |
| Risk Gate | `term-premium`: `latest.tp10/tp5/tp2`, `deltas_bps.d21`, `z_10y`, decomposition and date | Replace a yield-curve-slope proxy with the existing ACM term-premium output |
| Risk Gate / Repo / Bond War Room | `settlement-fails`: Treasury deliver/receive/gross, z-score, percentile, as_of, complete, scope, unit | Display actual settlement evidence and avoid duplicate counting of the same root series |
| LCE / Bond War Room | `repo-market`: percentile distribution, tail_bps/tail_z, transaction volume, SOFR-relative spreads, facilities, reserves | Measure funding stress distribution and activity rather than headline rates alone |
| Risk Sizer / Sizing Engine | `portfolio/risk.json`, `factor-risk`, `liquidity-capacity`; reconciled account state | Constrain final marginal risk, concentration and capacity on the same actual book |
| Conviction | `engine-trust`: sample counts, Wilson bounds, regime skill, alpha status; `signal-orthogonality`: clusters, IC, information rank | Weight demonstrated skill and independent evidence; do not reward the same underlying observation repeatedly |
| Stock Screener | `estimate-revisions`: dated EPS/revenue revisions, analyst count/dispersion, fiscal period; `earnings-quality`: full ranked quality output | Separate improving forecasts from cheap-looking but unreliable accounting |
| Sector Rotation / ETF Constituents | `etf-true-flows`: NAV/share-based net flows, anomalies, stock impact and dated holdings | Separate actual subscriptions/redemptions from price-driven AUM movement |
| Backtest / Liquidity Inflection | `vintage-fred`: `vintages[].date/value/known_on`; immutable model `available_at` | Reconstruct only information available at decision time |
| Squeeze / Short Book / Trade Tickets | `short-interest`: settlement_date, short_interest, days_to_cover; broker locate/borrow/recall data | Distinguish delayed short interest from daily short volume and executable short capacity |
| All execution-aware engines | Real timestamped bid/ask, quoted sizes/depth, fees, slippage, borrow, financing and margin | Replace range/mark proxies with observable execution inputs |

Names such as `risk-sizer` and `sizing-engine`, or `engine-fusion` and `jh-fusion`, refer to **different implementations**. Their contracts must be reconciled rather than assumed interchangeable.

## Required page-to-engine standard

Every public page needs an owner contract: primary engine, supporting engines, every output artifact, schema version, required fields, units, universe, timestamps and expected cadence. A page can present a short summary, but every user-facing engine field must be inspectable through typed sections, pagination, drilldown or complete export. Classify intentional internal-only outputs explicitly. Do not expose credentials or private client data merely to achieve a numeric coverage target.

For each output field record the JSON pointer, semantic label, formatting rule, section/column, and omission policy. For each array record the available and displayed row counts, pagination/filtering and export. In tests, compare a production-shaped fixture to that contract and assert actual displayed values and warning states. Literal file references and successful HTTP responses are insufficient.

The required lineage is: **raw observation → normalized feature → engine result → authoritative risk decision → portfolio constraint → rendered page**. Each step must retain the exact input snapshot/version and explain rejected inputs. Statistical dependence and shared raw sources must be tracked across engines. Feedback may update future model versions, but must not feed a downstream opinion back as independent evidence in the same decision cycle.

Minimum context accompanying any decision is source observation time, publication/availability time, ingestion and calculation time, schema/model/code version, raw unit and transformation, eligible sample/universe coverage, quality reason codes, confidence interpretation, horizon, and binding risk constraints. A freshly rewritten artifact does not make old source data fresh.

## Engineering order and acceptance gates

1. **Contain security defects.** Verify deployed parity; protect private routes and debug/maintenance endpoints; remove Origin-based privilege; repair guest namespace fallback; rotate exposed active credentials through managed configuration. Add cross-user and anonymous negative tests. Preserve and investigate logs without reading unrelated users' records as a test.
2. **Unify capital permission.** Connect every recommendation/basket to the validated authority, define gross/net/risky exposure and derivatives treatment, handle zero/unknown explicitly, apply final constraints after every multiplier and rounding, and expire stale permission. A simulated feed failure must never increase allowed exposure.
3. **Repair accounting and historical truth.** Reconcile fills/lots/positions/cash; use trustworthy dated marks; fix NAV and stop direction; remove current-data historical joins; store immutable as-of models/features; regenerate affected grades, NAV and performance claims under new versions.
4. **Repair ownership and display contracts.** Fix the seven confirmed mislabeled mappings and risk-gate schema mismatch; replace heuristic ownership with declared producer contracts; surface complete eligible output fields; make generators atomic and CI-gated. Do not rerun the existing wiring generator blindly.
5. **Integrate existing data with measurable benefit.** Start with the curated contracts. Run ablations on unchanged point-in-time samples; compare net returns, calibration, turnover, drawdown and tail losses to simple baselines. Reject integrations that add cost and apparent consensus without independent information.
6. **Add genuinely missing production inputs.** Broker reconciliation, executable liquidity/borrow/fees, when-issued auction quotes, complete licensed histories and point-in-time reference data. Validate entitlement, native frequency, known-on timestamp and coverage before promotion.
7. **Make release controls fleet-wide.** Required tests for changed producers and consumers, authenticated tenant boundaries, numerical edge cases, contract rendering, historical leakage and rollback. Publish immutable versions only after gates pass. Use an independent constrained paper/shadow period and a separately approved promotion rule before capital deployment.

Institutional acceptance is evidence-based, not an engine count or an AI-vote count. Monitor orphan/unknown ownership, eligible field coverage, missing critical sources, source-age violations, consumer snapshot coherence, accounting breaks, forecast calibration by regime/horizon, cost-adjusted out-of-sample skill and actual incident/rollback history.

## Data-definition issues to preserve during implementation

FRED identifies WALCL as a weekly Wednesday level in millions, WTREGEN as a weekly average in millions, and RRPONTSYD as daily billions. Unit conversion alone does not reconcile these observation conventions. Retain source publication dates and a defined common basis. NY Fed dealer statistics are weekly and released for the prior week; they must not be relabeled as daily DTCC observations. FRED's ICE high-yield OAS notes limit current downloadable observations to three years from April 2026 and describe distribution restrictions. Verify JustHodl's retained licensed history and rights; this audit does not establish that rights or older data are absent.

Official references: [WALCL](https://fred.stlouisfed.org/series/WALCL), [WTREGEN](https://fred.stlouisfed.org/series/WTREGEN), [RRPONTSYD](https://fred.stlouisfed.org/series/RRPONTSYD), [NY Fed dealer statistics](https://www.newyorkfed.org/markets/counterparties/primary-dealers-statistics), [ICE OAS notes](https://fred.stlouisfed.org/series/BAMLH0A0HYM2), [FRED historical knowledge time](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html).

## What is needed to finish production verification

The remaining work requires read-only exports of deployed Worker/Lambda code hashes and aliases, schedules, IAM/WAF bindings, expected-output manifests, representative complete output JSON and their schemas, source observation/vintage metadata, recent sanitized logs, CI run results, and authenticated test accounts in staging. For accounting and execution, use sanitized broker reconciliation and fill fixtures. Do not send passwords, API keys or real client data in chat.

An owner-side engineer should run a bounded, read-only production census pinned to one release and archive the evidence. Reconcile every live engine/output/page against this audit's rows. Confirm every required eligible field is displayed or accessible; exercise empty/stale/partial/error fixtures and filters/pagination on every page; validate chart values and units; test desktop/mobile layouts and access roles. Re-run the provided synthetic counterexamples against fixes, then verify unaffected consumers. This is the missing final sign-off, not a task claimed complete here.

## How to use the package

- `engine_inventory.csv`: 860 individual engine records, source files, static candidate reads/writes, live labels, proposed data checks and explicit review limitations. Category-based proposals are review prompts, not 860 completed semantic audits.
- `page_inventory.csv`: 510 individual page records, declared/candidate producers, script/data references, drift, live smoke status and all-field verification status. Source candidates are not a verified deployed route list.
- `curated_engine_data_dependencies.csv`: 34 specific source-anchored donor/recipient field contracts, with existing direct-reference evidence.
- `engineering_worklist.csv`: ordered fixes, acceptance criteria and evidence IDs.
- `evidence/`: safe synthetic reproduction outputs, syntax summaries, live error summaries and registry-counterexample evidence. No discovered credential values are included.

Detailed source evidence and instructions follow. File/line references are relative to the audited source revision. The clean repository snapshot was left unchanged.


---

## A — Security, accounting, historical integrity and release controls

Audit date: 2026-09-08. Source root: `/workspace/scratch/e1cd24b8d426/justhodl-source`. This is a bounded audit of security, portfolio accounting, backtest integrity, freshness monitoring, and release controls. It is not an assertion that every Lambda was behaviorally tested. No AGENTS.md was found in the source/accessible workspace search. No AWS commands, live exploit requests, real billing requests, production writes, or real customer-data retrieval were performed.

**Decision: the reviewed implementation should not yet be relied on as an institutional production system or a validated investment track record.** There are reproducible authorization failures and point-in-time/backtest defects, not merely missing polish. Some new controls are good, especially verified workspace identities, transactionally checked workspace revisions, and validated aliases for three engines; their protections are not consistently applied to the older routes or the rest of the fleet.

Evidence classification: **R** = source behavior reproduced offline with synthetic fixtures; **S** = inspected source/control-flow evidence; **L** = live confirmation. This subaudit has R and S evidence only. Runtime deployment parity, deployed IAM/WAF policies, provider-key validity, actual impacted customers, and historical production losses remain unverified.

### Priority findings

#### INST-01 — P0 — Brain and Decision Journal have no effective account authorization (R)

Source: `cloudflare/workers/justhodl-data-proxy/src/index.js`.

- Journal identity is selected from `?uid=` at lines 208–211. GET returns the selected journal without credentials at 216–219. Writes classify a non-owner UID of at least 20 characters as authenticated at 226–227, skip the PIN, and replace the journal at 242. A UUID is an identifier, not proof of authentication.
- Brain uses the same query-selected identity at 376–383; the `isAuthedUser` Boolean is based on string length. GET serves the notes at 554–572 with no access check. Writes skip the PIN when that Boolean is true at 575–584.
- `/brain-debug` has no auth gate at 332–365 and lists actual account/device identifiers at 347. Therefore “an attacker needs the UID” is not an adequate mitigating assumption for this implementation.
- These routes return before `verifySupabaseUser` is declared/used at 718 onward. The AI Worker also forwards Brain/Journal/debug/purge routes at `cloudflare/workers/justhodl-ai-proxy/src/index.js:307–312`; its origin check at 302–305 does not authenticate a user.

Offline reproduction: anonymous GET of synthetic Brain notes succeeds, anonymous debug lists its synthetic UID, anonymous PUT replaces a synthetic Journal, and anonymous PUT deletes a synthetic Brain note. All return HTTP 200. Only in-memory fixture storage was changed.

Impact: disclosure and modification/deletion of user research and decision history; a supposedly locked decision journal can be rewritten. This undermines both privacy and the integrity of a trading audit trail.

Required fix: use a verified token subject for every private read and write; reject body/query ownership; isolate guest records using an authenticated device credential; move debug/maintenance behind an administrator role; use append-only journal events with server time, actor, prior hash and immutable decision snapshot. Acceptance: User A cannot read/write/delete User B; anonymous calls return 401; no UID-length branch grants access; the AI proxy forwards/verifies credentials correctly.

#### INST-02 — P0 — Anonymous userdata fallback reads authenticated users' records (R)

Source: `cloudflare/workers/justhodl-data-proxy/src/index.js:852–873`, especially 867–871.

Authenticated data is stored under `u:<verifiedUid>`. Anonymous GET first tries `anon:<suppliedUid>`, then falls back to `u:<suppliedUid>` when the anonymous object is absent. This reopens the namespace the comments say was isolated. Offline synthetic UUID request with no Authorization returns the authenticated fixture's favorites with HTTP 200.

Required fix: remove anonymous fallback to the authenticated namespace. Migrate legacy anonymous records with a separate, authenticated ownership-proof process. Add regression coverage for known account UUIDs with no token, invalid tokens, and another account's token. Do not fetch real users' records to validate the fix.

#### INST-03 — P1 — Public API credentials and quotas can be bypassed by a caller-controlled Origin header (R)

Source: `aws/shared/api_auth.py:267–284`, helper at 366–398. It returns `ENTERPRISE` and bypasses all counters when no key is supplied and Origin/Referer matches the site. CORS does not stop nonbrowser clients from setting those headers.

Verified consumers:

| Engine | Source call site |
|---|---|
| nasdaq-datalink-agent | `aws/lambdas/nasdaq-datalink-agent/source/lambda_function.py:93` |
| justhodl-treasury-proxy | `aws/lambdas/justhodl-treasury-proxy/source/lambda_function.py:18` |
| justhodl-fred-proxy | `aws/lambdas/justhodl-fred-proxy/source/lambda_function.py:40` |
| justhodl-edge-engine | `aws/lambdas/justhodl-edge-engine/source/lambda_function.py:190` |
| justhodl-options-flow | `aws/lambdas/justhodl-options-flow/source/lambda_function.py:4289` |
| justhodl-investor-agents | `aws/lambdas/justhodl-investor-agents/source/lambda_function.py:188` |
| justhodl-stock-analyzer | `aws/lambdas/justhodl-stock-analyzer/source/lambda_function.py:444` |

Offline `authorize` invocation with only a site Origin returned Enterprise, no error, with no key lookup or rate counter access. Required fix: authenticate verified user/API identities; use an authenticated Worker-to-Lambda service identity and protected upstream; apply per-account quotas, per-provider budgets, and anonymous quotas if intentionally offered. Preserve CORS solely as a browser policy. Separately, rate-table ClientError returns zero and fails open at `api_auth.py:201–206`; alert and cap degraded operation instead of silently disabling quotas.

#### INST-04 — P1 — Checkout trusts user identity and purchased plan supplied by the browser (R/S)

Source: `cloudflare/workers/justhodl-data-proxy/src/index.js:957–989`.

`/create-checkout` does not call verified authentication. It accepts `userId`, `priceId`, `plan`, and return URL from the body, sends the arbitrary user into Stripe metadata at 974–976, and sends the arbitrary plan at 977–979. The webhook later trusts this metadata for entitlement writes at 1010–1019 and 1034–1039. There is no server mapping from approved Stripe price IDs to entitled plans in this path.

Offline request with synthetic low-cost price ID and `enterprise` plan forwarded that plan and a body-selected user to a mocked Stripe call without authentication. This proves trust of the supplied fields; it does not prove a particular Stripe price exists or a live paid bypass has occurred.

Required fix: derive user from verified subject; allowlist prices and map price-to-plan on server; validate return hosts; derive entitlement from the paid subscription item and current status, not client-originated metadata. Test buying a lower tier while submitting a higher tier, cross-user checkout, expired sessions, and unexpected price IDs.

#### INST-05 — P1 — Billing webhook acknowledges failed persistence, permanently losing entitlement updates (R)

Source: `cloudflare/workers/justhodl-data-proxy/src/index.js:1026–1044`.

The response from the Supabase profile write is ignored. The handler returns 200 even if that response is 503. Its catch also returns HTTP 200 explicitly, suppressing payment-provider retries. Offline signed fixture webhook with mocked Supabase 503 was acknowledged with 200. A failed profile write can coexist with a successful edge entitlement write, creating conflicting truth between stores; a failure of both may lose the paid update entirely.

Required fix: atomically persist/deduplicate webhook event IDs to a durable inbox; retry processing; require success before acknowledgement or acknowledge durable enqueue only. Check response statuses and record failed processing in an operational queue. Handle out-of-order subscription events using authoritative subscription state rather than whichever event happens to arrive last.

#### INST-06 — P1 — Administrative and provider credentials are embedded in source (S)

Values are intentionally withheld; validity was not probed.

| File | Lines | Category |
|---|---:|---|
| `cloudflare/workers/justhodl-data-proxy/src/index.js` | 271, 440, 459, 498 | Hard-coded administrative maintenance credential comparisons |
| `aws/lambdas/justhodl-volatility-squeeze-hunter/source/lambda_function.py` | 57 | FMP credential fallback |
| `aws/lambdas/justhodl-ai-chat/source/lambda_function.py` | 31–32 | Polygon and CoinMarketCap credentials |
| `aws/lambdas/justhodl-activity-nowcast/source/lambda_function.py` | 44 | FRED credential fallback |
| `aws/lambdas/justhodl-credit-stress/source/lambda_function.py` | 88 | FRED credential fallback |

Required fix: remove literal secrets, rotate through provider/secret-manager workflows after checking active consumers, scan full git history/build artifacts/logs for copies, and add a blocking secret scan. Rotation must account for all copied fallbacks so a missing environment value cannot revive an obsolete key. The maintenance routes need role authorization, not a replacement global string in query parameters. This audit did not rotate any credential.

#### INST-07 — P1 — Quantity/cost edits corrupt portfolio P&L and can invert stop behavior (R)

Source: `aws/lambdas/justhodl-portfolio-admin/source/lambda_function.py:120–163` and downstream `aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py:301–327`.

`cost_basis_total` is recomputed only if both quantity and unit cost are supplied in the same request (142–146). A quantity-only or cost-only edit leaves an inconsistent total. Position type is set only at creation (admin 94), never when quantity crosses zero. Snapshot trusts the stored cost (306), subtracts it to get P&L (309), and chooses the stop direction from stored position_type (313–314).

Offline fixture: 10 shares at $100 cost, then quantity-only update to 20, retains $1,000 total cost; at unchanged $100 market price snapshot arithmetic reports a $1,000 gain instead of zero. A subsequent update to -20 leaves `LONG`, selecting the long stop condition for a short.

Required fix: transactional read/validate/write or derive quantities and basis from immutable fills/lots; recompute all dependent fields whenever either input changes; update side on sign change; disallow accidental upserts to nonexistent positions with a condition; validate finite inputs. Add fill/lot-aware realized vs unrealized P&L before institutional use. Acceptance must cover partial edits, sign flips, split adjustments, concurrent edits, and absent positions.

#### INST-08 — P1 — Missing market prices silently become entry cost in the portfolio (S)

Source: `aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py:303–310, 316–340`.

When enrichment has no current price, line 307 substitutes cost per share. The system then computes market value, P&L, stops, sector totals and weights from that substituted value. A failed price feed can therefore produce apparently calm valuations and zero unrealized P&L. The rec may retain a missing `current_price` from enrichment while financial totals use an invented current mark.

Required fix: pass price, provider, exchange timestamp, receipt timestamp, adjustment basis, and valuation status together. Use explicit last verified mark with its age according to valuation policy, or return unpriced exposure and suppress dependent risk decisions; never convert a feed failure into a valid market price. Reconcile priced/unpriced NAV and stop evaluation coverage on the page.

#### INST-09 — P1 — The “walk-forward” calibration actually applies future weights to earlier trades (R)

Sources: `aws/lambdas/justhodl-calibration-snapshotter/source/lambda_function.py:93–100, 108–110, 153–186`; `aws/lambdas/justhodl-backtest-engine/source/lambda_function.py:231–245, 257–278, 975–978`; display promise at `backtest.html:177`.

Snapshotter reads live weights on Sunday after the calibrator and labels them with the Monday start of that same week. The backtest chooses a snapshot if `week_start <= trade_date`, drops the snapshot `as_of` when loading, and strips trade timestamps to dates. This applies information created after a trade to that trade while the page explicitly says it eliminates look-ahead bias.

Offline fixture: a snapshot created Sunday 2026-09-06 12:00, week starting 2026-08-31, is selected for a 2026-09-01 trade. A correct availability-time join must reject it. In addition, the “append-only” history key is overwritten when the same ISO week runs again (snapshotter 179–185), so reruns can retrospectively change the research record.

Required fix: immutable snapshot ID, `calibrated_at`, `training_end_at`, `available_at`, feature/model/code version; select newest snapshot whose full availability timestamp is no later than decision time, with training-label availability cutoff before fit. Never backdate a newly computed model to the start of its training week. Replace weekly overwrites with immutable versions plus an index. Recompute and version all affected claims; suppress “look-ahead eliminated” until validated.

#### INST-10 — P1 — Walk-forward NAV and Sharpe book future multi-day P&L on the signal's entry date (S)

Source: `aws/lambdas/justhodl-backtest-engine/source/lambda_function.py:986–1020, 1027–1084, 1086–1137`.

The path calculates the completed horizon return, then puts that entire contribution into `wf_daily_pct[start_i]` at 1059, where start_i is the logged signal date. NAV compounds that artificial daily series; Sharpe and drawdown use it. It also explicitly omits concentration and gross caps (1002–1004). This is a signal-attribution curve, not a daily marked, capital-constrained portfolio. A positive week-long outcome that suffered a large interim drawdown will never show that interim drawdown here.

Required fix: maintain entry/exit times, positions, cash, execution prices, daily adjusted marks, overlapping exposure and capital constraints; accrue realized/unrealized P&L on correct dates with trade costs, borrow and financing. Report signal-attribution metrics separately from tradable strategy NAV. Add a fixture with identical terminal returns but different intraperiod price paths; its true drawdown and risk must differ. Do not promote the current curve to headline performance when a sample-count gate opens.

#### INST-11 — P1 — Historical research attribution joins current critique and sometimes current regime (R)

Source: `aws/lambdas/justhodl-research-backtest/source/lambda_function.py:151–175, 216–247, 298–340`.

The engine evaluates an oldest historical entry but creates a critique lookup from current critique files by ticker. It uses that current critique for historical disagreement and consensus fields. If the old entry lacks a regime stamp, it fills it from the current research file. Historical outcome groups can change based on analysis written after the measured returns occurred. The ensemble result even labels a positive mean spread “signal is alpha” without a required significance/coverage test at 335–340.

Offline fixture: an August 1 research call inherits a September 8 disagreement score and a latest-regime label. Required fix: join critique and regime by immutable decision/research ID with available_at <= decision timestamp; retain unknown for unavailable history. Track all historical calls including superseded/deleted coverage; separate fixed-horizon cohorts; use sector/asset-appropriate benchmarks and confidence intervals before interpreting attribution as evidence of alpha.

#### INST-12 — P1 — Outcome grading changes with job delay and can finalize an unpriced benchmark (S)

Source: `aws/lambdas/justhodl-outcome-checker/source/lambda_function.py:334–381, 407–428`; `get_price` at 161–193 and `score_relative` at 225–235.

Windows expired within two days use current prices, while older windows use historical date-close. Thus the same intended horizon can be marked at different times depending on when the checker runs. The live fallback can also mix an FMP current quote, a previous-session Polygon close, or an undated report fallback across asset and benchmark. Missing benchmark input returns `(None, 0.0)`; this is still stored in existing_outcomes and the outcome table, and future runs skip that window at 336–337. The calibrator does exclude `correct=None` records, which is a positive guard, but that does not repair/retry the original grade.

Required fix: grade every window at a specified market timestamp on a trading calendar; align asset and benchmark marks and adjustment basis; store actual marks' timestamps/provider provenance; leave missing data pending/unscoreable with explicit retry policy and coverage denominator, rather than finalized zero excess return.

#### INST-13 — P1 — Freshness monitor can mark empty/stale-content artifacts healthy and does not detect absent expected outputs (R/S)

Source: `aws/lambdas/justhodl-fleet-freshness-monitor/source/lambda_function.py:147–185, 232–244`.

It enumerates existing S3 keys only and labels freshness solely from LastModified. A never-created/deleted expected artifact never enters the result set. A writer copying old data or writing empty output gets a fresh LastModified and passes. Size is reported but not validated. Enumeration silently stops at MAX_KEYS_PER_RULE (153–154) without a truncation/coverage status.

Offline fixture: a zero-byte `.json` object with fresh LastModified is `FRESH`. This is inconsistent with the monitor's stated aim to detect provider empty responses and missing expected output. It does not establish that a live artifact is currently empty.

Required fix: compare a governed expected-output manifest to actual keys; record missing/truncated coverage; parse schema, required rows/fields, source observation time, availability time and generation time separately. Track successful source retrieval independently of artifact rewrites. Reuse those statuses in fusion and UI decision gates. Required fixtures: absent key, zero-byte/invalid JSON, fresh wrapper with old source timestamp, partial provider output, future timestamp, and list truncation.

#### INST-14 — P1 control gap — New protections cover only a small release subset; crucial existing regression suites are not deployment gates (S)

Sources: `.github/workflows/deploy-lambdas.yml:150–175, 305–313, 415–438`; `.github/workflows/deploy-workers.yml:104–124`; `.github/workflows/pages.yml:51–53, 78–83, 105–119`; `.github/workflows/page-gate.yml:1–20`.

- Lambda release always compiles top-level Python, but dispatches per-engine tests only for settlement-fails, engine-fusion, khalid-risk, and khalid. It validates/promotes a versioned live alias for engine-fusion, khalid-risk, and khalid. Other changed functions can be sent directly to $LATEST without behavioral validation. Existing invest, jh-fusion, bridge and symdir tests are not invoked by this preflight case list.
- The Worker deploy workflow runs `wrangler deploy` without a test job. It can release precisely the authorization defects reproduced here.
- Pages deploy runs only `tests/khalid-ui.test.js`, leaving the workspace, frontend-freshness, home, risk UI and September 7 regression suites out of its test step. Several content/build steps intentionally ignore failure with `|| true`.
- `page-gate` is an independent push workflow, not a `needs` dependency of the Pages deployment; its static literal checks cannot prevent runtime undefined-data errors and do not constitute browser behavior validation.

Required fix: dependency-aware CI dispatch for all changed engines/Workers/pages and their consumers; required auth isolation, point-in-time, accounting and schema/UI contract tests; deterministic dependency pins; promote immutable candidates only after evidence validates; deploy a page together with compatible data contract changes. Retain the existing candidate revision/code-hash pinning and rollback logic rather than replacing those good controls. Branch protection and deployment-environment configuration were not accessible and are not asserted here.

### Additional data and cross-engine contracts needed in this subaudit

These are implementation recommendations derived from the source defects, not claims that a new paid feed alone solves them.

| Consuming engine | Specific producer/data required | Fields and required semantics | Required display/control |
|---|---|---|---|
| portfolio-admin / portfolio-snapshot | Broker/execution ledger and reference-data corporate actions | Immutable fills/lots; execution time, quantity, price, fees, currency; split/dividend events; realized vs unrealized P&L | Reconcile fills/lots/NAV; show mark source/time and unpriced positions |
| portfolio-snapshot / portfolio-risk | Existing pricing providers through one normalized price adapter | price, quote_time, received_at, market_session, currency, adjusted/unadjusted basis, stale_after, provider, quality | Suppress invented marks and stop evaluations from stale/absent prices |
| backtest-engine | calibration-snapshotter and calibrator | snapshot_id, trained_on_until, calibrated_at, available_at, code/model/feature versions, sample and horizon counts; immutable objects | Show usable point-in-time coverage and invalidate future snapshots |
| backtest-engine | outcome-checker + executable portfolio ledger | Decision timestamp, fill timestamp/price, outcome timestamp, path of daily marks, borrow/funding, costs, overlap/gross/sector capacity | Separate signal score attribution from actual portfolio NAV/risk |
| research-backtest | Historical equity research + historical critic + regime at decision | research_id, critique_id, source snapshot IDs, available_at, regime confidence and unknown state | Display as-of membership and historical coverage; no latest-doc fallback |
| outcome-checker | Calendar/reference engine + canonical price/benchmark adapter | Trading-session horizon endpoint, corporate-action basis, price timestamps, benchmark timestamps, missing-data state | Show unresolved outcomes separately; retry until correctly marked |
| fleet-freshness-monitor | Engine output registry + individual provider adapters | Expected keys and required fields, schema version, source_observed_at, available_at, generated_at, row count, schema-valid count, missing/truncated coverage | Show artifact age and source-data age separately on monitoring and engine pages |
| User workspace / Brain / Journal | Verified identity service and append-only event store | token subject, tenant, actor, object owner, server timestamp, revision, prior event hash | Private per-user reads; attributable immutable decisions; auditable corrections |
| Billing entitlements | Approved Stripe price registry and durable webhook inbox | Allowed price-to-plan mapping, verified owner, event_id, subscription_id, status, period end, processing outcome, event version | Consistent server-authoritative plan; visible failed entitlement processing |

### Verification actually performed

1. `node --test tests/*.test.js`: **42 passed, 0 failed**. Many cases assert source/markup patterns; several execute extracted frontend functions. This result does not imply these security or backtest paths are covered.
2. `python3 tests/deployment/run_tests.py`: **4 deployment static checks and 4 mocked candidate-shell scenarios passed**. The shell suite substitutes fake AWS; no AWS was executed against a real account.
3. `institutional-repro.cjs`: **7 isolated reproductions passed**, including five unauthenticated read/write/enumeration boundary failures against synthetic in-memory records, browser-selected checkout fields against a mocked Stripe service, and a signed fixture webhook acknowledged after a mocked database 503.
4. `institutional-repro.py`: **6 pure-function reproductions passed**: Origin-only authorization, stale basis, stale position side, future calibration selection, zero-byte freshness, and future research attribution.

Reproduction files: `/workspace/scratch/e1cd24b8d426/audit-work/institutional-repro.cjs`, `/workspace/scratch/e1cd24b8d426/audit-work/institutional-repro.py`. Results: `institutional-repro-js-results.json` and `institutional-repro-python-results.txt` in the same directory. These contain synthetic values only and do not contain the discovered credentials.

### Source review coverage and remaining work

Reviewed relevant branches/functions of: data and AI Cloudflare Workers; Supabase profile/RLS setup; shared API auth; portfolio-admin, portfolio-snapshot, portfolio-risk consumer references; API-key admin auth entry; fleet-freshness-monitor; backtest-engine; calibration-snapshotter; outcome-checker and calibrator filtering; research-backtest; signal-backtest aggregation; backtest.html's walk-forward claims; Lambda/Worker/Pages/page-gate/controlled release workflows; deployment helper/test runner and frontend test files. All seven Origin-bypass consumer call sites were located. This is **not full line-by-line certification** of these large files or of every Lambda in the repository.

Further read-only production checks, if available through the owner's access: compare deployed Worker/Lambda versions to audited source; inspect WAF/IAM without testing another user's data; inspect anonymous-route access logs; inspect immutable backup/versioning and customer-impact evidence; inspect price/outcome/calibration data timestamps and source age; inspect real CI history and enforced branch/deployment gates. Any incident-containment deployments, credential rotation or data restoration would be separate authorized implementation work, not a completed action of this audit.


---

## B — Risk, sizing and governed engine fusion

Audit date: 2026-09-08. Repository: `/workspace/scratch/e1cd24b8d426/justhodl-source`. Read-only source audit and offline deterministic reproductions; no AWS commands, deployments, brokerage actions or source mutations. No AGENTS.md found. Live deployment equivalence must be established separately by the overall audit.

### Scope and interpretation

Inspected `justhodl-engine-fusion` (entire core, wrapper, registry, subscriptions and tests); `justhodl-khalid-risk` (core, wrapper, input registry and tests); `justhodl-risk-gate` (calculation, replay, freshness and renderer contracts); `justhodl-risk-sizer` (entire core); `justhodl-katlin` (feed loading, risk war room, cap, basket and page rendering); `justhodl-khalid` (authority ingestion, status/stance semantics and existing tests); dedicated `khalidrisk.js`, `katlin.html`, `risk-gate.html`, `risk.html`; selected `home.js`. Katlin's entire 3,618-line signal/backtest engine was not exhaustively audited in this subtask. `fusion.html` uses a separate `jh-fusion` entity/horizon system, not the governed `engine-fusion` bus; assigned separately to avoid confusing the two.

The reported homepage `INVEST SELECTIVELY / 50%` and embedded Katlin `FULL RISK / 75%` are a real authority integration problem: both define maximum portfolio capital today. They are not simply different forecast horizons. The latest Khalid orchestrator itself **does** consume authoritative `data/khalid-risk.json` and preserves its cap. Its `WAIT_FOR_CONFIRMATION` means no actionable candidates, `risk_score=78` is a worst-domain display measure, and `status=OK` means required sources passed contracts. Those three fields are not inherently contradictory; the UI should call them *entry readiness*, *observed risk severity* and *data health*.

### Reproduced/source-confirmed findings

#### FR-01 — P1: Katlin bypasses the authoritative risk artifact

**Evidence:** `aws/lambdas/justhodl-katlin/source/lambda_function.py:1653-1661` loads raw risk gate and other war-room feeds but never loads `data/khalid-risk.json`. `:1783-1789` averages RISK_OFF as a 72-point weighted leg and hard-vetoes only SEVERE. `:1951-1970` builds its own cap and applies only the raw gate sizing multiplier. `katlin.html:350-359` displays the resulting posture/cap as current deployment guidance and says “No engine is forcing the desk to cash today” when its local veto list is empty.

**Impact:** credit/funding/settlement vetoes or tighter caps applied by Khalid Risk need not constrain the buy desk or its basket. Its `FULL_RISK` green label is retained after its cap is reduced, worsening the apparent contradiction.

**Fix:** Katlin consumes the same validated authoritative artifact as Khalid and homepage. Effective cap = minimum of authority cap and local cap; `allows_new_entries=false` blocks new executable entries regardless of local rank. Distinguish local research posture from effective capital permission. Preserve the authority version, generated time and explicit veto reasons in every basket. Avoid a feedback cycle: Katlin may consume Khalid Risk, but Khalid Risk must not then depend on Katlin's downstream recommendations.

#### FR-02 — P1: Katlin can permit full capital with stale/missing risk evidence; ignores an explicit zero multiplier

**Evidence:** `katlin/source/lambda_function.py:1742-1747` records `asof` but does not validate freshness or require a minimum coverage. `:1951-1965` renormalizes only available legs; even no legs gives UNKNOWN and a 25% cap. `:1968` requires `0 < sz`, excluding a valid zero cap. `katlin.html:430` accepts any successful JSON without an engine/schema/freshness contract.

**Offline reproduction:** one raw RISK_ON gate dated **2000-01-01**, all other war-room feeds missing, produces **FULL_RISK, 100%, no vetoes**. SEVERE with `sizing_multiplier=0` produces a **10%** cap, not zero.

**Fix:** critical missing/stale/invalid risk inputs must produce a data hold. Require observation timestamps/SLAs and freshness at render time. Permit `0 <= multiplier <= 1`. Unknown must not become permission to allocate. Add fixtures for zero, stale, future timestamps and missing critical sources.

#### FR-03 — P1: Risk Sizer violates the single-position limit it publishes

**Evidence:** `aws/lambdas/justhodl-risk-sizer/source/lambda_function.py:225-241` caps Kelly at 8%; `:370-380` then multiplies it by a quality weight up to 1.6; `:392-420` applies only cluster/gross caps. `:488-491` still declares an 8% single-name constraint.

**Offline reproduction:** two valid synthetic candidates, quality 95 and 70, different sectors, produce **HIGH 9.21%, LOW 6.79%** while `constraints_applied.max_single_position_pct=8`. The theoretical per-name upper bound before other caps is 12.8%.

**Fix:** impose the single-name cap after all upward modifiers and before feasible constrained allocation; assert final single-name, cluster and gross limits after rounding. This is a published-constraint violation, not a matter of investment opinion.

#### FR-04 — P1: Risk Sizer has a separate risk authority and ignores current portfolio state

**Evidence:** `risk-sizer/source/lambda_function.py:249-254` loads six inputs including `portfolio/state.json`; `:265-267` defaults missing regime to NEUTRAL / 75% gross. It never consumes `risk-gate.json` or `khalid-risk.json`. Loaded `state` is not used in the sizing math. `:330-335` returns early when no ideas exist without replacing the previous recommendations artifact. `risk.html:268` uses `risk/recommendations.json` directly.

**Impact:** recommendations can conflict with a capital veto, assume available capital already committed elsewhere, and leave an old actionable book in place when the current run has no candidates. Output is recommendation-only per the engine docstring; this audit does not assert automated trades occurred.

**Fix:** authoritative cap/permission required; validate current positions, NAV, cash and timestamp; identify whether recommendations are *target weights* or *additional orders*. Produce explicit empty/DATA_HOLD output when no usable ideas or required state exists. Reconcile incremental exposure against existing holdings and open orders.

#### FR-05 — P1: A zero NAV is discarded, disabling the drawdown brake at total loss

**Evidence:** `risk-sizer/source/lambda_function.py:190-194` filters snapshots by truthiness of `khalid_strategy_value_usd`.

**Offline reproduction:** NAV history `[100, 0]` returns drawdown **0.0**, peak date null, rather than -100%. Empty/missing history also returns zero rather than unknown.

**Fix:** test `is not None`, validate finite nonnegative NAV, preserve zero, report unknown separately and hold when the risk brake lacks sufficient trusted data. A separate UI bug at `:477-478` selects the *first* triggered threshold; at -15% it describes the -5% / 0.75 rule even though sizing multiplier is zero. Render the actually binding trigger.

#### FR-06 — P1: Risk Gate historical replay is contaminated by today's feeds

**Evidence:** `aws/lambdas/justhodl-risk-gate/source/lambda_function.py:342-344` calls itself pure/replayable. But `:522-524` reads current `data/treasury-rehypo.json`, and `:552-554` reads current `data/official-pulse.json`. Their current states modify the historic composite at `:540-574`. Handler `:974-976` calls this for every historical date; `:1010` produces the event study from those postures. Labels `replay_*_fred_only` at `:1032-1033` are inaccurate.

**Offline reproduction:** identical historical F/calendar/date **2025-04-11** gives NEUTRAL / 0 with fake current rehypo CALM, then RISK_OFF / -0.35 when only fake current rehypo changes to SEIZING. It also makes two S3 reads per historic date, adding thousands of serial object reads per run.

**Fix:** pure computation with immutable as-of inputs; live overlays belong outside the historical loop. Historical overlays require stored vintages available at each evaluation time. Recompute and mark existing replay/event-study results invalid until regenerated. Fetch each live artifact once per run.

#### FR-07 — P1: Risk Gate claims collateral/foreign-official deductions but drops them from the live result

**Evidence:** the deductions in `risk-gate/source/lambda_function.py:540-574` alter `replay_comp` and mark `legs.collateral.applied` / `legs.foreign_official.applied`. Handler `:985-994` resets `live_comp=0` and recalculates only funding/credit/dollar/carry/growth/structure, discarding the two deductions. It publishes these annotated legs alongside the inconsistent composite at `:1030-1035`.

**Impact:** an engine can say a stress deduction was applied while the authoritative live composite excludes that specific deduction. Other fleet inputs may still have their own overlapping effects; this does not mean all collateral data has zero effect.

**Fix:** explicit live overlay contributions table, including their numeric effect, source timestamp and eligibility. Reconciliation invariant: final composite must equal sum of published weighted legs and named overlays. Test individual overlays in isolation.

#### FR-08 — P1: Risk Gate calculates monthly Sahm/truck indicators from daily forward-filled rows

**Evidence:** handler `risk-gate/source/lambda_function.py:966-969` forward-fills *every* series onto the union calendar. `compute_indicators :293-297` treats the last 15 daily rows of UNRATE as months, computes a three-row mean and twelve-row raw minimum; the Sahm denominator should also be the trailing low of three-month averages. `:314-318` treats the thirteenth last daily TRUCKD11 row as one year ago.

**Offline reproduction:** unemployment rises from 4% to 5% and stays there for three months: report shows **Sahm 0 / CLEAR**, synthetic proper monthly result **1 percentage point**. Truck series down from 100 last year to 90 this year: report says **0% YoY / EXPANDING**, actual **-10%**.

**Fix:** retain native monthly data separately, use official Sahm realtime series or the exact monthly formula, and compare truck observation with 12 months prior. Preserve observation date and publication date. Do not calculate monthly measures from repeated daily samples.

#### FR-09 — P1/P2: Risk Gate dedicated page does not match its producer schema

**Evidence:** `risk-gate.html:538-542` expects `legs.*.engine_score`, `fleet_fused_score`, `state`, `indicators`, whereas producer emits `score`, `score_fused`, `why`, `fleet_inputs` (`risk-gate/source/lambda_function.py:395-399,989-993`). `risk-gate.html:582-584` expects `fleet_context.inputs`, but producer `:1005-1008` only sets yen/crisis scalar context; actual fleet arrays are under each leg. Producer top-level `indicators.indicators` at `:1038` has no corresponding page consumption.

**Impact:** real scores/driver text/fleet evidence can be omitted or displayed as missing even though the dedicated engine published them. A live sample is needed to distinguish a deployed alternate producer from repo drift. Source-to-source mismatch is definite.

**Fix:** one typed versioned contract; render the producer fields or update both together. Canonical fixture from the production-shaped output must assert values visible, not just DOM IDs. Add the nine indicator panel and explicit unsupported-source rows.

#### FR-10 — P1: Governed fusion rejects the actual credit-composite contract

**Evidence:** production `fusion-registry.v1.json` specifies `required_any=[composite,composite_score]`, but `fusion_engine.py:63-65` range checks both absent alternatives as mandatory. Credit producer `aws/lambdas/justhodl-credit-composite/source/lambda_function.py:149-151` emits `composite` only.

**Offline reproduction:** `{composite:20}` rejected “composite_score must be finite…”; `{composite_score:20}` rejected “composite must be finite…”; supplying both passes. Existing tests use a synthetic one-field contract, so miss the production-registry bug.

**Fix:** validate ranges on present optional fields; enforce alternative presence separately; add a production producer fixture to the contract suite. Restore credit packets/subscriber context/veto behavior once fixed.

#### FR-11 — P2: Governed fusion always reports DEGRADED when healthy evidence exists

**Evidence:** production registry deliberately excludes `best_setups_view` and `signal_fabric_view`. `fusion_engine.py:156-157` always makes these inactive. `:218` defines every output with any inactive packet as DEGRADED.

**Offline reproduction:** every included source fresh and valid, coverage ratio **1.0**, no stale/missing/invalid sources, plus one healthy deliberately excluded view => **DEGRADED**.

**Fix:** separate policy-excluded/observed-only sources from unavailable/invalid scoring inputs. Health should be DEGRADED only for actual required coverage problems. Display exclusions separately so the alarm remains meaningful.

#### FR-12 — P2: Khalid Risk page hides important fields of its dedicated engine

**Evidence:** `khalidrisk.js:341-359` renders named metrics when present, but then omits a domain's `score`, `state`, `status`, `as_of`, `age_h`, `artifact` (only severity is used in the header). `:378-394` source table reads coverage/detail fields absent from emitted health rows; actual `error`, `critical`, `max_age_h`, `freshness_basis`, `producer`/artifact are not shown. `risk_score`, `fusion_context`, `methodology`, `critical_failures` and most `policy` fields are validated/available but not rendered. Coverage summary `:306-314` leaves invalid/unknown counts out.

**Impact:** operators see DEGRADED/INVALID without the exact broken field/SLA/root cause; the dedicated page cannot expose the evidence and authority detail needed for an institutional audit trail.

**Fix:** every domain card shows numeric severity, raw state, freshness/as-of and source link; source table has criticality, SLA and reason. Add expandable full methodology/fusion lineage and raw-contract download. This need not mean putting every duplicate/debug field into the headline layout.

### Concrete engine-to-engine additions

These are code-verified donor paths or explicitly proposed new contract fields. Validate donor quality before wiring; a donor's presence does not certify its math.

| Recipient | Donor artifact and exact fields | Required use |
|---|---|---|
| Katlin war room/basket | `data/khalid-risk.json`: `policy.allows_new_entries`, `policy.mode`, `exposure_cap_pct`, `hard_vetoes`, `source_health`, `generated_at`, `schema_version` | Binding upper cap and entry prohibition; show authority and local opinion separately. |
| Risk Sizer | Same Khalid Risk fields; `portfolio/state.json` (currently read but unused) | Authoritative permission plus reconciled existing exposures/cash. Extend state contract with account-scoped NAV/holdings/open-order timestamps if absent. |
| Risk Gate indicator panel | `data/term-premium.json`: `latest.date`, `latest.tp10`, `latest.tp5`, `latest.tp2`, `deltas_bps.d21`, `z_10y`, `decomposition.identity_check_pct`, `source` | Replace the 2s10s slope labeled ACM proxy with already-existing genuine term-premium donor. Code donor verified at term-premium `:142-156`; yield-curve already demonstrates consuming it `:371-385`. |
| Risk Gate funding panel | `data/crisis-plumbing.json`: `xcc_basis_proxy.rate_diff_jpy_3m.z_score_1y`, `.rate_diff_eur_3m.z_score_1y`, `.obfr_iorb_spread.z_score_1y`, each `.signal` | Already used in `fleet_adjust` `:718-736`; expose exact fields instead of leaving top-level XCC indicator “wire join” pending. Label these proxies explicitly; add actual tenor-specific market basis quotes as a separate future licensed data product. |
| Risk Gate settlement lens | `data/settlement-fails.json`: `treasury.ftd_bn`, `ftr_bn`, `gross_bn`, `stats.gross.z`, `stats.gross.pctile`, `regime`, `score`, `as_of`, `complete`, `scope`, `unit` | Replace absent `ofr-stfm.fails_cross` placeholder (`risk-gate :698-702`) with contractually scoped Treasury evidence; do not count both as independent root facts. |
| Risk Gate replay | Snapshot store for `treasury-rehypo`: `band`, `composite`; `official-pulse`: `dollar_leg.legs_firing`, `.status`, `.available`, `.firing` | Existing fields need source observation and publication/vintage timestamps. No backfilled use of current snapshots. |
| Khalid Risk | `data/factor-risk.json`: `firm.net_market_beta`, `firm.es_95_1d_pct`, `factor_exposures`, `scenarios`, `coverage.n_direct_loadings`, `coverage.n_proxy_loadings`, `firm_book_asof` | Separate *market permission* from *model-book/portfolio suitability*. Add a portfolio overlay, not a worldwide risk verdict; propagate model-book identity. |
| Risk Sizer / portfolio overlay | `portfolio/risk.json`: `position_metrics`, `correlation_matrix`, `correlation_clusters`, `sector_concentration`, `var_1d_99_pct`, `stops_hit`, `alerts_summary` | Check incremental concentration, correlated exposure and stops on the SAME reconciled book; do not combine different notional model portfolios. |
| Risk Sizer / Katlin basket | `data/liquidity-capacity.json`: `least_liquid_names[].symbol`, `.dollar_volume_usd`, `.comfortable_position_usd`, `.days_to_liquidate`, `.size_vs_comfortable_x`; `firm.n_unknown_volume`, `parameters.participation_cap_pct` | Capacity constraint per symbol and residual liquidity. Current donor exports limited lists; add a complete `positions[]` table to serve all candidates. Recalculate for the actual portfolio NAV, not donor notional AUM. |
| Governed Engine Fusion | Credit `composite`; all donor source observation IDs, original series IDs, `observed_at`, `available_at`, `unit`, horizon (new canonical fields) | Repair credit contract, then construct actual dependency ancestry and overlap; current synthesized source `depends_on` arrays mostly empty, so the DAG only verifies declared edges. |
| Khalid page/homepage | Khalid `stance`, `decision.capital_decision`, `decision.exposure_cap_pct`, `risk_board.risk_score_method`, `source_health` | Label entry-readiness separately from permission, market risk and source health; make contradictory local opinions drillable. |

### Further data/model work needed by these engines

- **Risk Gate:** preserve FRED/other observation frequency, first-publication dates and revised-data vintages; source calendar/service-level policy; raw-to-normalized value audit. Real executable cross-currency basis, repo specialness/haircuts, bond liquidity and auction surprises are useful additions, but the existing proxy/data-frequency bugs take precedence.
- **Khalid Risk:** add explicit exposure definition (gross/net/risky sleeve, leverage, hedges), effective/expiry time, rule version, as-of input-set hash, binding constraint and prior-state transition. A generic “50% exposure” cannot govern books with different leverage or derivatives treatment.
- **Katlin:** preserve source observation timestamp per signal and freshness per branch, point-in-time corporate actions/universe membership, reliable catalyst publication dates, earnings blackout, trading spread/slippage/borrow assumptions, and cost-adjusted out-of-sample calibration for its probabilities and expected-return claims.
- **Risk Sizer:** estimates of win probability and win/loss distribution calibrated out of sample. Current `:285-290` converts dims/composite heuristically into probabilities and `:305-308` converts a debate conviction into probability; `kelly_size(edge_pct=0.05)` never uses edge and assumes symmetric win/loss. Call these heuristic scores until calibrated. Add account identity/NAV, existing positions/orders, transaction costs, volume capacity, factor/dollar limits and dated aligned return series (current correlation aligns by array position, not actual date).
- **Fusion:** original observable identity and real ancestry, common forecast horizons, source-specific observation age, calibration sample count, reliability/error intervals and a common unit dictionary. Preserve separate source disagreements; avoid interpreting agreement of overlapping composites as independent confirmation.

### Verification evidence

`audit-work/fusion-risk-repro.py` parses and executes selected source function ASTs, uses synthetic fixtures and a fake S3 object, and intercepts all recommendation writes into memory. It imports no production AWS clients and makes no network calls. Full output: `audit-work/fusion-risk-repro.json`.

- Governed Engine Fusion existing tests: **11 passed**.
- Khalid Risk existing tests: **9 passed**.
- Khalid existing scoring suite: **50 passed**, with a fail-on-any-operation in-memory boto3 client stub (unmodified runner otherwise fails because local boto3 is not installed). No AWS methods were called.
- Independent reproduction fixtures: alternative-field contract rejection; false DEGRADED; stale Katlin full-risk; zero-cap ignored; single-name cap breached; zero NAV ignored; incorrect monthly Sahm/truck calculations; live-feed historical replay contamination. All produced the results documented above.
- No production changes, no deployment, no claims of actual investment loss or proof of production/backend parity. Production reviewer should join these findings with captured live payloads, screenshot evidence and deployment lineage.

### Release acceptance for this slice

1. Every executable recommendation and displayed basket obeys validated authority cap/permission on the same input generation.
2. Missing/stale/invalid required risk data never increases allowed exposure.
3. All final allocations obey single-name/cluster/gross/capacity limits after modifiers and rounding.
4. Historical output is invariant to current feed changes and reproducible from immutable as-of snapshots.
5. Monthly-frequency indicator fixtures match native-month calculations; no forward-filled duplicates enter observation counts.
6. Risk Gate producer fixture renders actual leg scores, fleet fields and indicators. Khalid Risk exposes every critical failure reason and rule affecting permission.
7. Fusion health excludes intentional exclusions; each real producer passes the exact production contract before publication.


---

## C — Page ownership, wiring and complete field display

The current directory cannot certify the user's requirement that every dedicated page shows every relevant output and field of its engine. It reports source string associations; several associations are wrong, and the generic renderer deliberately truncates data.

### Scope and honest counts

- Inventoried all **510 public HTML source candidates**: 464 root HTML, 44 first-level directory index pages, `tools/ice-recover.html`, and `web/intel/index.html`. Included the union of all 231 site-catalog entries, 458 navigation entries, 331 page-AI registry entries, and 458 section-registry entries. None of those catalog routes lacks local source. The 510 candidates include 17 redirect pages; they are not 510 independent engine dashboards and their deployed accessibility was not verified by this subaudit.
- **373** pages contain exact data-path literals in their HTML; that is static reference evidence only. Literal matches may be comments, links, or unused strings. Shared linked-script references and bare-filename inferred matches are recorded separately.
- **42 pages / 167 cards** use the generic `jh-wire.js` component. **71 pages** differ between `data/engine-wiring.json` and actual source declarations. **28 candidate producer/output mismatches** are flagged; some are scanner limitations, not proven wrong mappings.
- Live directory snapshot supplied by the parent: **860 engines**, **710 WIRED**, **40 fresh orphans**, **12 stale orphans**, **10 dead-feed orphans**, **88 NO OUTPUT DECLARED**, observed 2026-09-08T14:00:14.694Z. These are observed directory labels, not independently certified output/display states.
- Runtime fetch, runtime rendering, all-output display, and all-field display are **UNVERIFIED for every inventory row** in this source subaudit. Browser evidence is handled separately by the parent.

`page-inventory.csv` and `.json` have one row per source/catalog page with exact references, source file/line evidence, linked JS, actual and registry-declared wires, page-AI declarations, candidate producers, live directory claims, and per-declared-engine output-reference checks. References inherited from a shared script must not be read as verified page-specific consumption.

### Confirmed defects

#### P1: Pages label upstream inputs as engine outputs

| Page and line | Labeled engine | Current displayed feed | Correct declared engine output | Source evidence |
|---|---|---|---|---|
| `sectors.html:637` | `justhodl-volatility-squeeze-hunter` | `data/universe.json` | `data/volatility-squeeze.json` | Engine `lambda_function.py:56` selects output, `:68` reads universe, `:417` writes output |
| `global-macro.html:304` | `justhodl-hiring-velocity` | `data/bagger-engine.json` | `data/hiring-velocity.json` | Engine `lambda_function.py:37–39` distinguishes output/input; `:328` writes output |
| `resilience.html:163` | `justhodl-stocktwits` | `data/ai-infra-stack.json` | `data/stocktwits.json` | Engine `lambda_function.py:22`, `:76`, `:108` |
| `resilience.html:163` | `justhodl-spinoff-desk` | `data/insider-aggregate.json` | `data/spinoff-desk.json` | Engine `lambda_function.py:56–57`, `:613` |
| `crypto-liquidity.html:94` | `justhodl-index-inclusion` | `data/finviz-index-membership.json` | `data/index-inclusion.json` | Engine `lambda_function.py:20`, `:50`, `:139` |
| `lce.html:841` | `justhodl-fed-pivot-factor-router` | `data/cb-stance.json` | `data/fed-pivot-factor-trades.json` | Engine `lambda_function.py:92`, `:246`, `:332–333` |
| `signal-intelligence.html:53` | `justhodl-alpha-calibrator` | `data/calibration-fleet.json` | `data/calibration-latest.json`, `data/calibration-history.json`, `screener/alpha-weights.json` | Engine `lambda_function.py:107–109`, `:730`, `:751`, `:773` |

All engine paths above are under `aws/lambdas/<engine>/source/`. These are confirmed source mappings; current runtime payloads were not fetched here. Showing a producer's input does not display its computed result. Either point to the named engine's correct output with a typed view or relabel the card to the actual producer. Preserve upstream lineage separately. Choosing a semantically appropriate dedicated page is also necessary: equity index membership currently appears on the crypto liquidity desk.

#### P1: Generic cards cannot display every field or row

`jh-wire.js:32–45` chooses only the first object-array found within two nested levels, at most eight top-level scalar fields, eight rows, and five columns selected from the first row. Long strings, later arrays, nested objects, later-row-only fields, and all remaining rows disappear. `:50` says there are more rows but provides no pagination or raw-data link. The source fixture with 12 rows and six columns renders only eight rows and five columns; a separate risk-warning array and long methodology are absent. This affects all 167 declared cards when the source payload exceeds these limits. Summary cards are reasonable previews but must not be accepted as complete coverage. Add full typed drilldowns, pagination/filtering, schema-aware field mapping and a safe full JSON/CSV export.

#### P1: WIRED status has false positives and suppresses failed-feed status

`scripts/bake_engine_directory.py:95–104` treats an output's basename or any quoted stem anywhere in HTML as reference evidence. The pure reproduced cases count `data/_cycle/pv.json` as present because `live-pulse.html:107` has `class="pv"`, and `data/_council/log.json` because `horizons-gsi.html:193` has `type:"log"` for an axis. A full scan found 322 stem-only output/page pairs; that figure is candidate matches, not 322 proven defects.

At `:109–115`, ANY matched output makes the whole engine WIRED before freshness or feed availability is considered. Thus the status cannot distinguish a broken wired feed from a healthy one or partial from complete output coverage. `:111` takes the freshest output and `:122` stores only the first three outputs and pages; `engines.html:128` displays only the first output. Change to per-output/per-page state with `declared → fetch success → schema valid → rendered → contract complete`, separately tracking freshness, failure, and intended internal feeds.

#### P1: Directory omits directory routes and linked JavaScript

`scripts/bake_engine_directory.py:70` only globs root `*.html`. It misses all directory routes including `intel/index.html`, whose source loads 14 distinct feeds at `:318–521`, and `funding/index.html:103–113`, which loads crypto funding. It does not follow script `src` either. Static dependency-map generation similarly scans only root HTML/JS and merges only same-stem JS (`scripts/build_dependency_map.py:94–110`); that heuristic both misses differently named imported assets and assumes an unlinked same-stem file is used. Use a recursive public-route manifest and actual script import graph, then verify real network/DOM behavior.

#### P1: Engine output manifest has confirmed false negative and false positive modes

`scripts/gen_engine_manifest.py:78–85` inspects a 300-character window after a put call, treats any matching data literal/variable there as written, and truncates results to 16. The reproduction passes a real output write followed by an input read; the scanner falsely lists both as outputs.

Real false negative: `justhodl-etf-fund-flows` has `keys:[]` in the engine manifest and NO OUTPUT DECLARED in the supplied live directory. Its actual code defines `OUTPUT_PREFIX="etf-flows/"` at line 84 and writes `daily.json`, `composite.json`, `event-study.json`, `rotation.json`, and `per-ticker-context.json` through constant-prefix f-strings at lines 1296–1313. The scanner returns `[]` on that real source. Do not classify these engines as dead based on this registry. Use explicit versioned producer contracts plus AST/call-site verification; do not use broad regex matches as authoritative ownership.

#### P1: 'Canonical' wiring generator can overwrite newer page assignments

`scripts/gen_engine_wiring.py:124–131` removes every existing `jh-wire.js` script on an assigned page and replaces it with its hardcoded assignment. The generator is not a round-trip of current source: actual wires and stored registry differ on 71 pages. Example: current `attention.html:199–200` declares `data/attention-signals.json`; generator `:64–67` would replace that with Google trends, ticker trends, and a wrongly labeled `data/ai-infra-stack.json` Stocktwits feed. Its manifest is rewritten before `sys.exit(1)` on problems (`:143–146`), allowing partial mutation on failure. Do not rerun blindly. Reconcile into one maintained ownership contract, generate in staging, validate all pages first, and apply atomically.

#### P2: Null-first-row payload can break the generic renderer

`jh-wire.js:33` accepts `[null]` as an object array because `typeof null === "object"`. `tableOf` then calls `Object.keys(null)` at `:44`, throwing `TypeError`. The local fixture reproduces this. This is a robustness bug; no current production feed with a null first row was observed here. Validate array elements and show an explicit schema/failure state rather than leave a loading card.

### Coverage contract required for every page

Define a single versioned contract with page route, primary engine, supporting producers, each output key and schema version, required/optional fields, user-facing section/column identifiers, units, market/observation timestamps, refresh cadence, expected universe, entitlement, and acceptable degradation. Record intentional internal-only outputs with reasons. 'Every field' should mean every relevant field is inspectable through typed sections or drilldown/export; internal cache fields need not clutter the dashboard. Each excluded field needs an explicit classification, not a silent drop.

Tests should compare actual real-payload JSON paths and row counts to the contract, then browser assertions should verify required fields are visible or available through deterministic drilldown/pagination. Include empty, stale, partial, schema-change, null, and critical-dependency-failure fixtures. A missing or stale critical risk field must disable actionable capital interpretation. Owner identity and upstream lineage must come from the producer contract rather than whichever filename makes a source scanner turn green.

### Additional data/display priorities for the confirmed miswired engines

| Engine | First display the existing engine data | Add or fuse next |
|---|---|---|
| Hiring velocity | `n_scanned`, `n_scored`, `n_errors`, expansion counts, `top_50`, `expansion_inflections`, `double_confirmed`, methodology and generated time (source `:302–325`) | Publication-time headcount history; acquisition-adjusted headcount; compensation/SBC; revenue-per-employee trend; earnings/revision feed context; source coverage per name |
| Stocktwits | `trending_equities`, `top_bullish_buzz`, complete symbol `sentiment`, source, caveats and generated time (source `:94–106`) | Unique author/bot-adjusted activity, sample size, historical attention z-score; combine insider/13F/options confirmation and price/volume, avoiding double-counted retail channels |
| Volatility squeeze hunter | Actual squeeze score and component flags, universe scanned, qualified names, timestamps, failure coverage | Tradable liquidity/spread, free float/borrow, corporate events, independent volume/relative-strength confirmation and historical post-breakout outcome distributions |
| Spinoff desk | Actual spinoff situations and computed assessment, rather than generic insider aggregate | Distribution/ex dates, Form 10 and amendments, parent/child cost basis, forced-selling indices, debt allocation, pro forma cash flow, insider alignment and borrow availability |
| Index inclusion | Inclusion/exclusion signals and effective-date evidence, rather than membership cache | Benchmark-tracking AUM, free-float change, rebalance calendar, expected passive shares to trade, liquidity/auction impact, event confirmation and cancellation |
| Fed pivot factor router | Factor trade routing output, reasons, eligible assets, weights and constraints | Rate surprise/priced path, real yields, dollar and credit/liquidity regime; independently validated risk-gate/veto status and cadence-aware freshness |
| Alpha calibrator | Current calibration, historical calibration and published weights | Sample size, calibration curve, confidence intervals, leakage controls, out-of-sample splits, family/cluster dependence and degraded-input effects |

These are improvement proposals, not claims that the extra datasets are currently absent everywhere in the wider fleet. Verify producer coverage before adding new paid sources.

### Reproducibility

- `build_inventory.py`: read-only source inventory, no engine imports or network.
- `reproduce_scanner_gaps.py`: pure scanner tests and exact real-source counterexamples.
- `reproduce_wire_limits.js`: actual renderer helper functions against controlled fixtures.
- `scanner-reproductions.json`, `wire-renderer-reproduction.json`, `wire-owner-mismatches.json`, and `wiring-registry-drift.json`: evidence and candidate queues.
- No production source, configuration, AWS resource or deployment was changed.


---

## D — Entity/horizon fusion read API

Scope: inspected `cloudflare/workers/justhodl-data-proxy/src/fusion_api.js` and executed its exported handler in an isolated Node VM with mocked `caches.default` and prohibited network fetch. No production requests, deployments, engine invocations, or order execution occurred. Synthetic values below are test fixtures, not observations of current production data.

### Confirmed findings

1. **P1 — Health reports success for unusable, stale, or incoherent snapshots.** At lines 118–124, health requires only truthy parsed JSON documents. Two empty objects produce HTTP 200, `ok:true`, `fusion:{}`, `state:{}`. A fusion document dated 2000 and state document dated 2001, mismatching `snapshot_run_id`/`run_id`, and 100 stale signals still produce HTTP 200 and `ok:true`. Generated timestamps are returned but never evaluated. A cache TTL limits how often S3 is fetched; it does not make the underlying source snapshot fresh. This is a readiness/monitoring integrity defect even in shadow research mode.

2. **P1 — The same snapshot that passes health can crash the fusion list route.** With both cached documents `{}`, health passes, then `/api/v1/fusion` throws `TypeError: Cannot read properties of undefined (reading 'label')` at line 139 (`f.regime.label`). Missing schema validation is therefore not merely a reporting issue. Catching it globally could turn this into HTTP 500 rather than a thrown exception; that would still be inconsistent with healthy readiness.

3. **P1 before any trading integration — Stale capital labels and opportunities remain eligible.** Lines 176–199 only test document truthiness and numeric filters. The ancient fixture returns `capital_decision:"ALLOWED"`, `opportunity_rank_score:0.81`, HTTP 200, public caching for 60 seconds. Repeating with `shadow_mode:false` changes the returned flag but does not change freshness enforcement. No recency, schema, completeness, veto expiry, or coherent snapshot gate is imposed by this read API.

4. **P2 research UX/API contract concern — BLOCKED rows remain in opportunities at rank zero.** Lines 189–193 intentionally multiply rank by zero but still append the row, so it can appear among returned opportunities when few valid candidates exist. The response note explicitly discloses this behavior and the capital label remains BLOCKED. `fusion.html:371–374` duplicates the behavior and displays the blocked label. This is not evidence of bypassed execution controls. Prefer `actionable_only=true` by default, or clearly separate blocked research candidates; return `eligible:false` and veto reasons. Zero-rank rows can remain useful in a research/explanation endpoint.

### Research versus production severity

The source header explicitly calls the read model shadow mode. `fusion.html:278–282` labels shadow mode advisory; `docs/fusion/release_1_plan.md:43–46` describes deliberate promotion after a graded record. `docs/fusion/gap_analysis.md:43` places sizing integration in a later release. No inspected code here places orders. Therefore this audit does **not** claim live capital was allocated from these responses or that a trade kill switch was bypassed. The defects do undermine research trust, operational monitoring, and any downstream consumer that treats a 200 health response or ALLOWED label as current authorization. Promotion to execution must be blocked until fixed and verified.

### Required correction and acceptance criteria

- Define read-model schemas and reject empty/wrong-type documents with `503`, explicit reason codes, and `Cache-Control:no-store`.
- Separate liveness from readiness: `available`, `schema_valid`, `fresh`, `coherent`, `critical_inputs_ready`, `execution_eligible`, with per-document and per-critical-input ages.
- Derive maximum age from the engine cadence, market calendar, and horizon; validate timestamp parsing, impossible future time, observation time versus generated time, and required snapshot completeness.
- Verify `fusion.snapshot_run_id` identifies the state used to compute fusion. If readers advance independently, pin/read the referenced immutable state; do not blindly require equality to an independently moving latest pointer. Return stale-but-valid research snapshots only with explicit stale/ineligible status.
- Expire capital labels at the API boundary; execution must independently enforce its own live risk gate. Never let API caching renew authorization age.
- Cover the exact reproduced cases in meaningful tests: empty objects, ancient timestamps, incoherent snapshot IDs, all-critical inputs stale, zero signals, missing regime, blocked rows, and `shadow_mode:false` with expired data.

Artifacts: `page-coverage/reproduce_fusion_api.js` and `fusion-api-reproduction.json`. Reproduction passed with **0 network calls**.


---

## E — Specific data additions and donor-to-recipient contracts

Prepared 2026-09-08 from public source at `justhodl-source` and official data documentation. These recommendations distinguish actual producer fields from proposed new fields. They are not claims of verified runtime consumption. A literal JSON reference is only static evidence; adapters and shared code can add indirect dependencies. Before adding a collector, prove the fields are absent from the existing warehouses and designated producer.

### Source findings that change the priorities

1. `justhodl-liquidity-profile` currently computes `spread_proxy_bps` from the last daily high-low range divided by close (source lines 123–145) and weights this input at 35% in the tradability score (line 156). This is not an observed bid-ask spread; the module introduction's NBBO description is misleading. Add timestamped quotes and sizes, rename the existing field as daily range, and do not use it as execution-cost evidence.
2. `justhodl-bis-crossborder` explicitly produces BIS CBS consolidated foreign claims, with raw quarterly/yearly changes. It does not provide FX-adjusted LBS flows, currency-specific dollar liabilities, or a complete offshore funding estimate. Keep CBS identity and add the distinct LBS view from the BIS warehouse (S17).
3. `justhodl-china-liquidity` documents money-growth acceleration as a credit-impulse proxy. Add PBOC Aggregate Financing to the Real Economy flow and stock, component breakdown and GDP-normalized credit-flow impulse; keep the proxy and its performance separate. The PBOC publishes distinct stock/flow reports (S19).
4. `justhodl-repo-market`, `justhodl-nyfed-repo-deep`, `justhodl-vintage-fred`, `justhodl-etf-true-flows`, `justhodl-engine-trust` and `justhodl-signal-orthogonality` already implement useful primitives. Improving their contracts, timestamps and actual consumption is more useful than duplicating these engines.
5. `justhodl-firm-book` aggregates desk outputs into a modeled book. A production hedge-fund system additionally needs reconciliation to actual cash, positions, fills, borrow and financing. Do not present modeled target exposure as broker-confirmed holdings.

### Curated integration contracts

For every link below: preserve observation time, release time, source/model version, unit, horizon, quality, and exclusion reasons; provide a donor-to-receiver-to-page trace; prove field coverage and an ablation/change test. The exact field list is a review target, not authorization to create circular dependencies. Existing dependencies require quality verification, not a second edge. Never send downstream decisions back into upstream evidence in the same decision cycle.

| ID | Donor → recipient | Fields | Required improvement/check |
|---|---|---|---|
| D01 | `justhodl-repo-market` → `justhodl-liquidity-credit-engine` | distribution.tail_bps, tail_z_1y, tail_pctile_since_2018, p1/p25/p75/p99, volume_usd_bn; spreads; facilities; reserves | Make funding tails and facility use first-class LCE contributors alongside credit spreads; horizon/date-aware aggregation; keep existing feeds if already semantically consumed. |
| D02 | `justhodl-repo-market` → `justhodl-bond-warroom` | repo_stress_score; distribution; spreads; facilities; calendar | Show funding stress beside sovereign moves so price pressure and financing pressure can be distinguished; display raw contributors and as-of dates. |
| D03 | `justhodl-repo-market` → `justhodl-liquidity-capacity` | repo_stress_score, regime, distribution.tail_bps, spreads | Apply a separately backtested funding-stress scenario to participation/depth assumptions; do not equate systemic repo stress with observed stock tradability. |
| D04 | `justhodl-nyfed-pd` → `justhodl-bond-warroom` | by_tenor_usd_b; net_positions_usd_b; wow_usd_b; z_52w; financing; transactions | Display dealer inventory, financing and transaction volumes by maturity rather than headline yields alone; normalize fails by relevant transaction volume. |
| D05 | `justhodl-settlement-fails` → `justhodl-repo` | treasury; classes; totals; as_of; per-series latest/z/pctile/avg_52w/n_obs/start | Ensure all collateral classes and receive/deliver sides are inspectable; use actual weekly observation dates and avoid relabeling as daily DTCC fails. |
| D06 | `justhodl-settlement-fails` → `justhodl-bond-warroom` | treasury; classes; totals; signal; as_of | Pair rising fails with dealer financing/positions and repo tails to identify settlement pressure; maintain one underlying observation across composites. |
| D07 | `justhodl-term-premium` → `justhodl-bond-warroom` | latest; deltas_bps; z_10y; pctile_full_history; decomposition; curve; source | Separate expected policy-path repricing from duration-premium repricing; carry official ACM provenance and model uncertainty to the page. |
| D08 | `justhodl-auction-grader` → `justhodl-bond-warroom` | graded_auctions[].cusip/auction_date/issue_date/tenor_bucket/accepted_billions/dimensions; by_tenor | Use comparable-tenor auction evidence with financing and dealer inventories; add timestamped when-issued quote to compute a genuine auction tail. |
| D09 | `justhodl-tic-flows` → `justhodl-bond-warroom` | total_foreign_holdings; net_purchases; top_holders; individual; composite_tic_stress | Add separate SLT transactions and valuation-change fields and publication timestamps; do not call declining market-value holdings a sale or indirect bids foreign demand. |
| D10 | `justhodl-bis-crossborder` → `justhodl-eurodollar-plumbing` | source; by_counterparty[].period/latest_bn/qoq_pct/yoy_pct; offshore_centres; em_asia; errors | Current donor is CBS consolidated foreign claims. Add a distinct LBS currency/sector and FX-break-adjusted flow panel for USD funding analysis; retain CBS identity. |
| D11 | `justhodl-ciss-stress` → `justhodl-euro-fragmentation` | ea_composite; ea_composite_date; categories; series; frequency_note; provenance | Display country and sovereign-stress series against matched-maturity BTP-Bund and other spreads; distinguish new versus legacy CISS definitions. |
| D12 | `justhodl-ciss-stress` → `justhodl-liquidity-credit-engine` | ea_composite; ea_regime; series; provenance; frequency_note | Add euro-area/country systemic stress as context with transparent overlap exclusions; do not count the same financial-market inputs as multiple independent votes. |
| D13 | `justhodl-vintage-fred` → `justhodl-backtest-engine` | data/vintage/<series>.json: vintages[].date/value/known_on; vintage index coverage | Require a feature-time adapter using only known_on <= decision timestamp; date-only vintages also need actual release time where intraday decisions are evaluated. |
| D14 | `justhodl-vintage-fred` → `justhodl-liquidity-inflection` | vintages[].date/value/known_on | Re-run advertised liquidity flip/lead-lag event studies using historical available information; respect weekly source publication dates before daily forward-fill. |
| D15 | `justhodl-liquidity-profile` → `justhodl-sizing-engine` | all_tickers.{ticker}.ticker/adv_usd/atr_pct/spread_proxy_bps/consistency/liquidity_score/n_bars | Use ADV/coverage; replace daily high-low spread proxy with timestamped NBBO and depth before claiming execution-aware sizes. Keep volatility and spread distinct. |
| D16 | `justhodl-liquidity-capacity` → `justhodl-sizing-engine` | firm.pct_liquidatable_1d/3d/5d, n_unknown_volume, trapped_book_pct; least_liquid_names[].days_to_liquidate/comfortable_position_usd/size_vs_comfortable_x | Cap order and position size using proposed book plus existing exposures; surface unknown volume as a constraint instead of neutral liquidity. |
| D17 | `justhodl-factor-risk` → `justhodl-sizing-engine` | firm.var_95_1d_pct/var_99_1d_pct/es_95_1d_pct/net_market_beta; factor_exposures; risk_contributors; coverage.n_direct_loadings/n_proxy_loadings/failed_history | Size against marginal portfolio risk and scenario loss, not only standalone signal Kelly; distinguish directly estimated factor loadings from sector proxies. |
| D18 | `justhodl-firm-book` → `justhodl-conviction-engine` | equity_book[].symbol/side/net_pct/gross_pct/desks/desk_conflict; macro_book; sector_exposure; conviction_overlap | Annotate an attractive signal with existing firm exposure, correlated holdings and opposite desk positions; do not turn an already-owned theme into another independent opportunity. |
| D19 | `justhodl-engine-trust` → `justhodl-conviction-engine` | engines[].n_scored/hit_rate/wilson_lb/regime_wilson_lb/regime_n/alpha_status/net_alpha_t_stat/effective_trust; current_regime; caveats | Use sample-aware, regime-conditioned evidence in ranking; make trust lineage visible and test that demotion changes rank; handle insufficient history explicitly. |
| D20 | `justhodl-signal-orthogonality` → `justhodl-conviction-engine` | per_engine; clusters_high_redundancy; engine_ic; effective_information_rank; correlation_matrix; snapshots_total | Combine empirical dependence with structural source lineage; highly correlated/co-derived signals contribute limited independent evidence. Match signal horizons before correlation. |
| D21 | `justhodl-engine-trust` → `justhodl-sizing-engine` | engines[].effective_trust/regime_n/regime_wilson_lb/alpha_status/net_alpha_t_stat; current_regime | Use trust degradation as a bounded size factor, with positive out-of-sample net edge and effective sample size required; document compounded penalties and avoid duplicate haircut. |
| D22 | `justhodl-credit-before-equity` → `justhodl-stock-screener` | names[].distance_to_default/synthetic_cds_bp/default_prob_5y_pct/d_distance_to_default/d_synthetic_cds_bp/d_price_pct/prior_obs_date/hist_n; degraded; gaps | Display credit versus equity divergence, refinancing context and history coverage on each stock; synthetic CDS must stay labeled as a model estimate. |
| D23 | `justhodl-estimate-revisions` → `justhodl-stock-screener` | upward_revisions/downward_revisions[].current_eps_est/baseline_eps_est/eps_rev_pct/eps_rev_recent_pct/rev_rev_pct/baseline_date/n_obs/fiscal_period/fiscal_year/n_analysts/dispersion_pct/earnings_date/session | Add dated revisions, analyst breadth/dispersion and matched fiscal-period comparisons; show forward-growth strength separately from observed revisions. |
| D24 | `justhodl-earnings-quality` → `justhodl-stock-screener` | all_ranked; top_20_high_quality; top_10_low_quality_avoid; methodology; sources; as_of | Expose cash-backed earnings/quality alongside growth and valuation; retain accounting-period/as-filed lineage and sector-valid ratio rules. |
| D25 | `justhodl-etf-true-flows` → `justhodl-etf-constituents` | by_etf; net_flow_1d_usd/net_flow_5d_usd/net_flow_20d_usd; nav_source_counts; n_price_fallback_degraded; anomalies; by_stock; impact_map | Prefer NAV/share-based flow ground truth to AUM changes or volume proxies; trace each implied stock pressure to dated ETF holdings and avoid top-50 truncation presented as full coverage. |
| D26 | `justhodl-etf-true-flows` → `justhodl-sector-rotation` | category_rotation; complexes[].gross_flow_5d_usd/net_flow_5d_usd/share_class_rotation; by_stock[].implied_usd_5d/bps_adv_day | Separate real capital movement, share-class switching and mark-to-market returns; normalize pressure by underlying ADV and cross-check sector breadth/earnings. |
| D27 | `justhodl-short-interest` → `justhodl-squeeze-fuel` | by_ticker.{ticker}.short_interest/settlement_date/days_to_cover/si_change_pct/latest_short_pct/n_days_volume_data; data_sources | Keep delayed short-interest positioning separate from daily off-exchange short-volume flags; add dated float, locate/borrow fees and actual borrow availability for tradability. |
| D28 | `justhodl-short-interest` → `justhodl-trade-tickets` | by_ticker.{ticker}.settlement_date/short_interest/days_to_cover/si_change_pct; short source | Surface crowding and covering risk; live short orders need broker locate/borrow rate and recall status, which public short interest cannot provide. |
| D29 | `justhodl-options-analytics` → `justhodl-trade-tickets` | board[].atm_iv_front/term_slope/term_structure/skew_25d/hv20/vrp/iv_rank/gamma_regime/gamma_flip_strike/call_wall/put_wall/expiries | Add option cost and event-vol context to ticket rationale; execution requires bid/ask, quotes timestamps, size/depth, multiplier and expiry. Dealer gamma remains assumption-sensitive. |
| D30 | `justhodl-crypto-funding` → `justhodl-crypto-basis` | by_coin.{coin}.instId/current_funding_rate/annualized_pct/oi_usd/funding_z_score/n_history_periods; market_composite | Combine dated cash-futures basis with perp financing/crowding context without treating a past annualized funding rate as locked carry; parameterize each venue funding interval. |
| D31 | `justhodl-crypto-basis` → `justhodl-sizing-engine` | per-asset index/perp_premium_pct/funding_annualized_pct/basis_30d_ann_pct/cash_and_carry_yield_3m_pct/curve[].instrument/days/basis_pct/annualized_basis_pct/open_interest/volume_usd | Apply only to relevant crypto carry exposures; require executable bid/ask, borrow/cash yield/fees, margin and exchange collateral risk before interpreting mark-based annualized basis as obtainable yield. |
| D32 | `justhodl-firm-book` → `justhodl-liquidity-capacity` | equity_book[].symbol/side/net_pct/gross_pct/desks; firm.gross_exposure_pct/net_exposure_pct; desk_conflicts | Show tradability on both net and gross desk exposure. Real order management must reconcile actual positions/fills; modeled strategy allocation is not a broker position ledger. |
| D33 | `justhodl-factor-risk` → `justhodl-firm-risk-board` | firm; scenarios; risk_contributors; hedges; coverage; firm_book_asof | Expose portfolio risk components/scenario results with factor coverage; reject stale book linkage; show the risk effect of scenario and hedge assumptions. |
| D34 | `justhodl-liquidity-capacity` → `justhodl-firm-risk-board` | liquidity_posture; firm.n_unknown_volume/trapped_book_pct/pct_liquidatable_1d/3d/5d; trapped_names; by_desk; by_sector | Put inability-to-exit and unknown tradability beside the exposure cap and vetoes; drill through every constrained name and show a stressed exit horizon. |


## JustHodl data enhancement and dependency notes

Research date: 2026-09-08. These are proposed requirements, not verified backend deficiencies or existing dependency wiring. Confirm each field against source/output schemas before adding a collector. The public provider inventory already describes broad NY Fed, OFR, CFTC, Treasury, BIS, ECB and SEC coverage, so consumption/completeness is the first task.

### High-value enhancement list

| Priority | Recipient engine / page | Proposed donor | Exact fields / data requirement | Reason and acceptance check |
|---|---|---|---|---|
| P0 | Every predictive engine; conviction; sizing | Canonical indicator bus / FRED archive / signal ledger | `observation_date`, `source_released_at`, `ingested_at`, `available_at`, `vintage_id`, revision flag, timezone; FRED `realtime_start`, `realtime_end`, `vintage_dates` | Reconstruct what was knowable at each decision time. A revised latest-history backtest must not be called point-in-time. Reject future releases in as-of joins. S01. |
| P0 | liquidity-agent / global-liquidity / liquidity-pulse | FRED / NY Fed / Treasury warehouse | WALCL, WTREGEN, RRPONTSYD, WRESBAL; matched point-in-time dates and unit normalization; actual reserve balances alongside Fed-minus-TGA-minus-RRP proxy | WALCL is Wednesday level, WTREGEN is week average, RRP is daily; WALCL/TGA/reserves use millions, RRP billions. Use a clearly defined common convention rather than silent forward-fill. Show each input's observation and release date. S02. |
| P1 | repo-market / liquidity-pulse / liquidity-credit-engine | NY Fed reference rates and research | SOFR, TGCR, BGCR, EFFR, OBFR; p1/p25/p75/p99; transaction volume; rate revision footnote; IORB; Reserve Demand Elasticity estimate and confidence interval | Produce SOFR-IORB, EFFR-IORB, SOFR-TGCR, p99-p50, p75-p25, volume anomalies, with methodology-version flags. Rate tails/actual reserve demand measure funding pressure better than raw balance-sheet size alone. RDE published monthly, not a daily independent new observation. S03/S04. |
| P1 | repo-market / canary-macro / dollar-radar | NY Fed operation outputs | Repo/SRF operation date, settlement/maturity, total submitted/accepted, collateral class; ON RRP total and counterparty count; central-bank USD swap usage | Distinguish facility uptake and month-end effects from structural private-market stress; retain zero activity separately from missing release. Operations are aggregate, not realtime borrower-specific funding exposures. S05. |
| P1 | repo / repo-market / bond-warroom | OFR STFM | `REPO-DVP_TV_TOT-P`, `REPO-GCF_TV_TOT-P`, `REPO-TRIV1_TV_TOT-P`; venue rates, tenor, collateral composition, preliminary/final flag | Show venue shares and maturity/refinancing concentration. OFR centrally cleared data generally T+1, tri-party T+2, 3 p.m. releases on applicable business days. Do not mark expected lags as outages or claim full bilateral coverage without dataset proof. S06. |
| P1 | repo / repo-lending / sizing | NY Fed Tri-Party/GCF Repo research | Collateral class; median and haircut range; collateral value/share; top-three-dealer share; observation and publication dates | Haircut shock and concentration scenarios; monthly representative-day snapshot, not daily live haircuts. Handle November 2025 reporting break explicitly. Haircuts cannot be substituted for every participant's prime-broker margin. S07. |
| P1 | bond-warroom / canary-macro / liquidity-credit-engine | NY Fed primary-dealer statistics | Treasury fails to deliver/receive; financing and net positions by maturity/collateral; transaction volumes | Build maturity-aware dealer inventory pressure and fails/volume ratios. Weekly previous-week release on Thursday; do not present these as daily DTCC fails. S08. |
| P1 | bond-warroom / conviction / repo-lending | CFTC TFF + OFR Hedge Fund Monitor | Leveraged-fund Treasury futures longs, shorts, spreading and open interest; asset-manager positioning; Form PF leverage and liquidity aggregates; SCOOS dealer financing terms; FICC sponsored repo volume | Crowding and funding vulnerability context. COT generally Tuesday positions released Friday, not same-day flow. Leveraged-fund shorts alone do not measure the cash-futures basis trade; no exact basis exposure without cash-side data. OFR monitor contains related CFTC data, so deduplicate donor lineage. S09/S10. |
| P1 | bond-warroom / liquidity-flow / liquidity-pulse | Treasury auction warehouse + NY Fed SOMA + Treasury cash data | CUSIP, maturity, auction/issue dates, offering/accepted amounts, direct/indirect/primary-dealer accepted, bid-to-cover, high yield, reopen flag, SOMA accepted, scheduled maturities | Settlement-calendar cash/duration supply shocks; segregate bills/coupons, auction date/settlement date, competitive/noncompetitive denominators. Auction tail needs an independently timestamped pre-auction when-issued yield, not the average accepted yield. Indirect bidders are not synonymous with foreign buyers. S11. |
| P1 | liquidity-credit-engine / canary-macro / conviction | NY Fed CMDI + existing ICE credit store | CMDI broad/IG/HY; ICE OAS by rating and maturity; historical sample counts and licensed lineage | Add market-functioning distress beyond spread level; require full licensed histories to support 5-year z-scores/GFC calibration. Never silently compute a 5-year statistic from 3 years. S12/S13. |
| P1 | liquidity-credit-engine / global-cycle / screener / forensic | FRB SLOOS + H.8 bank data | Net tightening and loan demand separately for C&I, CRE and consumer credit; bank loan/deposit levels and growth by relevant bank groups | Separate supply contraction from weak demand; map credit tightening to debt maturity, floating-rate burden and refinancing need in equities. Quarterly survey is slow regime context; release-aware freshness mandatory. S14. |
| P1 | liquidity-credit-engine / bond-warroom / sizing | FINRA aggregate data; licensed TRACE if entitled | Treasury volume by maturity/type; corporate volume/trade counts; security-level pricing and liquidity only under appropriate license | Add execution capacity and price corroboration. Public aggregates are not equivalent to real-time transaction feeds or executable liquidity. Verify vendor/redistribution agreements before public display. S15. |
| P1 | liquidity-flow / dollar-radar / bond-warroom / global-tide | Treasury TIC / existing TIC warehouse | SLT holdings, net purchases/sales and valuation changes by country and security type; banking claims/liabilities; foreign official/private distinctions | Identify external funding and duration demand. Keep stocks, transactions and valuation separate; monthly ~6-week lag; annotate February 2023 transactions/valuation series break and custody-country bias. Do not label a fall in market-value holdings as sales. S16. |
| P1 | bis-crossborder / global-liquidity / dollar-radar / china-liquidity | BIS LBS / GLI | USD claims/liabilities by bank location, counterparty country, sector and instrument; break- and exchange-rate-adjusted quarterly changes; foreign-currency credit to nonbanks | Map funding dependence, separate exchange-rate translation from credit creation. Keep BIS residence and nationality views distinct. Quarterly slow vulnerability context, not realtime flows; do not count same loans twice across BIS, TIC and global-M2 composites. S17. |
| P1 | ciss-stress / ECB hub / bond-warroom / liquidity-credit-engine / global-tide | ECB CISS/CLIFS/SovCISS + sovereign-yield and bank-lending outputs | `CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX`, comparable US CISS; country stress, sovereign stress and subindices; matching maturity sovereign yields; ECB lending standards | Expose systemic-vs-country stress and fragmentation contributors. Distinguish new/legacy CISS variants; don't sum composites with overlapping inputs as independent confirmations. Spread calculation requires same maturity, observation convention and currency. S18. |

### Explicit integration contract

Proposed donor-to-recipient links above must carry `source_engine_id`, `source_output_version`, canonical indicator ID, raw value/unit, transformation, observation/release times, missing-data state, freshness policy, confidence and evidence URL. For every link, the receiving output and page must display the actual values used and exclusion reasons. A string reference to a JSON file establishes neither semantic use nor field-level display coverage.

To avoid circular self-confirmation, use raw-data/derived-feature/model/signal/risk/decision stages; lag any intended feedback. `conviction` should receive dated model probabilities, expected return/horizon, measured out-of-sample skill and independent-source groups. `sizing` should receive conviction plus current portfolio exposures, covariance, funding/counterparty limits, executable liquidity and actual cost assumptions. These are requirements, not proof the existing engines omit them.

### Unverified methodology checks

- Public liquidity page describes a 3–5-day SPY leading signal; require a dated walk-forward track record with realistic publication lags before presenting a fixed lead as established.
- Public bond page includes strong deterministic bond-regime-to-BTC claims and selected historical examples. Require all signal instances, false positives, out-of-sample results and uncertainty; selected event anecdotes do not establish tradable timing.
- Public LCE methodology advertises 5-year z-scores and GFC/COVID threshold calibration. FRED's current ICE OAS notes restrict downloadable history to three years starting April 2026; verify stored/licensed older history, source rights, sample counts and honest unavailable states.
- Public provider playbook says no observation in 90 days leads to metadata-only import. If implemented literally, this can discard valid annual/lagged quarterly data and discontinued historical crisis series. Freshness must be source-calendar/frequency aware; preserve historical series with retired status.
- Public provider inventory contains broad feeds already. Treat proposed additions as field-level exposure/integration checks first; avoid duplicate provider ingestion and counting mirrors as independent evidence.

### Primary sources

- S01 FRED API real-time/vintage semantics: https://fred.stlouisfed.org/docs/api/fred/realtime_period.html and https://fred.stlouisfed.org/docs/api/fred/series_observations.html . Defaults return information available today; ALFRED real-time dates retrieve past-known data.
- S02 Official FRED definitions: https://fred.stlouisfed.org/series/WALCL ; https://fred.stlouisfed.org/series/WTREGEN ; https://fred.stlouisfed.org/series/RRPONTSYD ; https://fred.stlouisfed.org/series/WRESBAL .
- S03 NY Fed reference rate methodology, percentiles, volumes, publication and revisions: https://www.newyorkfed.org/markets/reference-rates/additional-information-about-reference-rates .
- S04 RDE: https://www.newyorkfed.org/research/reserve-demand-elasticity and https://www.newyorkfed.org/newsevents/news/research/2024/20241017 .
- S05 NY Fed repos: https://www.newyorkfed.org/markets/desk-operations/repo . Associated reverse-repo and swap operations linked in NY Fed navigation.
- S06 OFR repo dataset: https://www.financialresearch.gov/short-term-funding-monitor/datasets/repo/ ; lag policy https://www.financialresearch.gov/short-term-funding-monitor/documentation/ ; exact venue mnemonics https://www.financialresearch.gov/short-term-funding-monitor/market-digests/volume/chart-26/ .
- S07 NY Fed monthly haircuts/concentration and 2025 break: https://www.newyorkfed.org/research/tri-party-repo .
- S08 NY Fed dealer release schedule and dataset scope: https://www.newyorkfed.org/markets/data-hub?form=MG0AV3 .
- S09 CFTC report definitions and timing: https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm . General classifications/history also https://www.cftc.gov/es/node/128971 ,
- S10 OFR HF monitor datasets/frequencies: https://www.financialresearch.gov/hedge-fund-monitor/ . Form PF/SCOOS quarterly, FICC/CFTC monitor extracts monthly; direct CFTC release is weekly.
- S11 Treasury auction fields: https://www.treasurydirect.gov/auctions/auction-query/ ; verified field-bearing query https://www.treasurydirect.gov/auctions/auction-query/?cusip=912810UL0 . Indirect-bidder definition: https://treasurydirect.gov/help-center/faqs/auction-faqs/ ,
- S12 CMDI official product: https://www.newyorkfed.org/research/policy/cmdi ; research https://www.newyorkfed.org/research/staff_reports/sr957 .
- S13 ICE restrictions and 3-year history change: https://fred.stlouisfed.org/series/BAMLH0A0HYM2 . Verify JustHodl's separate licensing; public access is not evidence rights are missing.
- S14 FRB SLOOS: https://www.federalreserve.gov/data/sloos/about.htm ; analytical explanation https://www.federalreserve.gov/econres/notes/feds-notes/an-aggregate-view-of-bank-lending-standards-and-demand-20200504.html .
- S15 FINRA aggregate data: https://www.finra.org/finra-data/fixed-income ,finra.org/filing-reporting/trace/content-licensing/real-time-end-day-market-data-agreements ,
- S16 TIC interpretation and reporting lags: https://home.treasury.gov/data/treasury-international-capital-tic-system-home-page/frequently-asked-questions-regarding/ticfaq2 ,treasury.gov/data/treasury-international-capital-tic-system-home-page/tic-forms-instructions/securities-b-portfolio-holdings-of-us-and-foreign-securities ,
- S17 BIS LBS definitions and adjusted changes: https://data.bis.org/topics/LBS ,bis.org/topics/GLI (redirect initially observed from https://www.bis.org/statistics/gli.htm,
- S18 ECB CISS metadata: https://data.ecb.europa.eu/data/datasets/ciss/data-information ,ecb.europa.eu/data/datasets/CISS/CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX ,ecb.europa.eu/data/datasets/CISS/CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX ,

Public JustHodl observations: https://justhodl.ai/data.html; https://justhodl.ai/lce.html; https://justhodl.ai/liquidity.html; https://justhodl.ai/bonds.html. Search-rendered empty placeholders alone are not proof of browser runtime failures.


### Additional official source checks for equities and China

- S19 PBOC Aggregate Financing stock and flow publication index: https://www.pbc.gov.cn/en/3688247/3688978/3709140/48b09237-8.html Historical index verifies product existence; integration must find the current official series and preserve component/methodology changes.
- S20 SEC Form 13F FAQ: https://www.sec.gov/rules-regulations/staff-guidance/division-investment-management-frequently-asked-questions/frequently-asked-questions-about-form-13f Use filing availability, not quarter end, for knowledge time. 13F is incomplete for net positions because short equity/options positions are not reported. Need amendments and identifier joins.
- S21 FINRA short volume versus short interest: https://www.finra.org/investors/insights/short-interest and https://www.finra.org/finra-data/browse-catalog/short-sale-volume. These are different metrics; retain both and their separate observation/publication times.

### Truly additional production inputs, after existing coverage audit

1. Broker/PB position, cash, fills, accrued financing and margin reconciliation → `firm-book`, `portfolio-analytics`, `pnl-attribution`, `firm-risk-board`.
2. Timestamped NBBO, quoted size, depth/auction liquidity and actual fill slippage → `liquidity-profile`, `liquidity-capacity`, `sizing-engine`, `trade-tickets`. Entitlement and venue-coverage checks required; daily range is not spread.
3. Locate availability, borrow fee, recall/closeout conditions and short-sale constraints → `short-book`, `merger-arb-risk`, `sizing-engine`, `trade-tickets`.
4. Pre-auction when-issued yield with exact timestamp and security convention → `auction-tail`, `auction-grader`, `auction-crisis-detector`, `bond-warroom`.
5. Licensed historical credit indices and issuer-level debt/credit data where absent → `credit-stress`, `liquidity-credit-engine`, `cds-monitor`, `credit-before-equity`; preserve structural-model labels if no observed CDS exists.
6. Point-in-time estimates, historical constituents/delistings and corporate actions → `estimate-revisions`, `stock-screener`, `backtest-engine`, `signal-scorecard`. Present-day values are not adequate substitutes for historical knowledge.
7. Source release calendars/timezones, immutable model/signal registries and scheduled-vs-observed provenance → every predictor and its page; these are data inputs to auditability, not decorative metadata.

These are design requirements; no brokerage connectivity, licensing entitlement, private production state or complete engine-to-page field rendering has been verified by this subtask.


---

## F — Complete engine inventory methodology

This deliverable covers all 860 engine names observed on the live engine directory on 2026-09-08. Every name has source code in the supplied repository. It is a reproducible static inventory, not a claim that all engine calculations, runtime behaviors, deployments or page fields were individually validated.

### Findings from the screening

- All 1,182 Python files under `aws/lambdas` parsed successfully with `ast.parse`; no application imports or engine execution occurred. This includes nested vendored modules and tests for syntax only.
- First-party modules immediately under each engine's `source/` directory were analyzed for storage read/write paths. The extractor found candidate JSON reads in 629 engines and writes in 778 engines. No recognized static writes in 82 engines does not mean they are broken: request/response APIs, dynamic writes, shared helpers and internal services can be valid.
- There are 2,704 candidate engine-to-engine dependency edges from static write/read intersections. This is evidence that a substantial amount of inter-engine consumption already exists. It does not prove that all relevant fields are used correctly or that the signals are economically independent.
- Nineteen paths have multiple candidate writers. Inspect ownership, merge rules and last-writer behavior before treating those feeds as a canonical source.
- Fifty-two engines have 54 keys that the **repository manifest** declares as outputs but the AST extractor sees only as reads. Ninety-nine engines have 227 static write paths absent from that manifest. These counts are investigation candidates, not proven production failures; lexical analysis can miss dynamic writes.
- Forty-five engines' **live table primary paths** are observed only as reads in source. The live table and repository manifest are different snapshots and are recorded separately.

### Concrete ownership examples

| Engine | Evidence | Why inspect it |
|---|---|---|
| `justhodl-allocator` | `lambda_function.py:861` reads `data/indicator-bus.json` with `get_object`; its detected output is `data/allocator.json`. | Repository manifest includes the indicator bus among outputs; consumers should not infer ownership from that declaration. |
| `justhodl-activity-nowcast` | `lambda_function.py:294` reads `data/indicator-bus.json`; its detected output is `data/activity-nowcast.json`. | Same read-versus-write manifest ambiguity. |
| `justhodl-ai-chat` | `lambda_function.py:827` calls the AST-resolved read helper `get_s3("data/13f-positions.json")`. | Live directory displays that 13F feed as its primary path; source consumption does not establish feed ownership. The local manifest has no keys for this engine, demonstrating snapshot drift. |
| `justhodl-air-cargo` | `lambda_function.py:202` writes `air/hkia-cargo-levels.json`. | The repository manifest misses this sibling-prefix output; validate whether consumers need its history and define the output explicitly. |

All source paths above are under `aws/lambdas/<engine>/source/`.

The repository's `scripts/gen_engine_manifest.py:75-85` looks in a 300-character window after each detected write call, matches known key literals/variables in that window, and returns only the first 16 sorted keys. That mechanism does not establish actual argument binding or exhaustive output ownership. Replace it with explicit producer manifests/schema contracts and retain source analysis as a consistency check.

### Files

- `engine_inventory.csv` and `engine_inventory.json`: all 860 engines; source paths; repository output declarations; live primary declaration and observed page links; separate AST reads, writes, dynamic patterns and unclassified literal candidates; per-engine review action; proposed additional data; explicit coverage limitations.
- `static_path_evidence.csv` and `.json`: direction, JSON path, source file, line, extraction method and confidence for each observation.
- `static_dependency_edges.csv`: recipient/donor/path intersections. A path with several candidate writers yields several candidates; this does not select a canonical producer.
- `multiple_writer_candidates.csv`: paths requiring canonical-writer review.
- `syntax_errors.csv` and `.json`: empty error table/list after all 1,182 Python files passed parsing.
- `summary.json`: machine-readable counts and provenance.
- `build_inventory.py`: reproducible extractor; read-only with respect to application source and services.

### Interpretation limits

Literal calls have high static confidence; resolved names/collections have medium static confidence. The extractor unions possible assignments and uses bounded propagation through locally defined wrappers. It does not prove reachability, environment configuration, branch selection or deployment equivalence. It does not fully resolve dynamic paths, shared imported modules, external provider schemas or arbitrary runtime dataflow.

Per-engine data proposals are category-informed review prompts grounded in the engine identifier and repository description. They are not evidence that those fields are currently missing. Confirm exact fields, economic value, licensing and availability before implementing. Infrastructure and collector entries emphasize observability, timestamps, lineage and reliable contracts rather than unnecessary signal inputs.

The live directory's observed page links do not prove dedicated ownership or an exhaustive consumer list. Displaying every field remains a separate producer/schema-to-DOM validation requirement. Meaningful public outputs should be visible in dedicated views or labeled drilldowns; private credentials, internal state and infrastructure-only artifacts require appropriate access controls rather than indiscriminate public display.


---

## G — Frontend syntax screening

{
  "html_candidates": 510,
  "html_sources_parsed": 510,
  "html_parser_failures": 0,
  "inline_javascript_blocks_checked": 821,
  "root_javascript_files_checked": 46,
  "total_checks": 867,
  "passes": 867,
  "failures": 0,
  "affected_sources": 0,
  "node_version": "v24.19.0",
  "method": "HTMLParser extracts literal inline script content. Non-JS types, external-src inline content and empty blocks skipped. node --check uses .mjs for type=module, .js otherwise. Root *.js checked without vendor recursion. No JS/import execution.",
  "limitations": "Syntax-only screening does not establish runtime correctness, undefined identifiers, failed data fetches, field display, calculation correctness or deployed artifact parity. Node may differ from browser grammar."
}

| Source | Source line | Inline script | Parse error | Qualification |
|---|---:|---:|---|---|

No application JavaScript, imports or page code was executed. Node diagnostic source snippets were omitted to avoid exposing embedded secrets.
