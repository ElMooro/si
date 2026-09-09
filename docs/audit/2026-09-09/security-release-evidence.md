# Security release handoff — 9 September 2026

Status: implemented and tested in source. Production progress and live receipts are maintained by the release owner; this subteam does not independently attest deployment or confidentiality closure. Ops5230 must validate the complete matching 44-function source set before migration, including all earlier core producers.

No private customer bodies, billing mutations, producer invocations, messages, credentials or deployment operations were used by this security implementation team. Test data are synthetic. The release owner runs the joint release gate and live verification separately.

## Implemented boundaries

| Audit finding | Source result and meaningful verification |
|---|---|
| INST-01 personal data | Private corpus, owner account outputs, historical archives and alternate aliases intercept before cache. Owner/service authorization gates 26 exact mirror kinds; only services publish. Raw archives remain IAM-only. Full private originals are preserved. Signed-out, unrelated-account, stale-cache, Range, version and encoded-path fixtures cannot disclose private bodies. |
| INST-01 locked journal | The existing SQLite Durable Object stores versioned, chunked journals and immutable revision records. Legacy source is retained. Locked entries cannot be edited/deleted; corrections append. Concurrent submissions conflict explicitly; restart and multi-megabyte migration fixtures preserve history. |
| INST-02 identity isolation | Verified user namespaces and anonymous namespaces remain separate. Existing regression tests still pass. |
| INST-04 checkout | Verified buyer, server price mapping, pinned return origin and independently proven Stripe customer ownership. Browser profile or request metadata cannot select another customer or grant a plan. |
| INST-05 billing | Serialized durable customer state, event markers, current customer-wide Stripe reconciliation and versioned entitlement projection replace KV read-before-write deduplication. No stale event fallback. Tests cover duplicates, reorder, replacement subscriptions, mirror failures, restart and cross-account binding. Supabase/KV mirrors cannot elevate authorization. |
| Fusion D1–D4 | Real producer schema, nested rows, finite values, coherent run/timestamps, per-signal freshness and critical readiness govern every projection. Execution and explicit research eligibility are separate. Fusion page consumes the validated API snapshot and expires it. |
| Personal watchlist/trades | Read/write Function URLs verify identity before inputs or scheduled flags are processed. Owner pages use fixed authenticated Worker routes, server-held service credentials and no browser admin-token forms. Watchlist S3 revisions use conditional writes and explicit conflicts. Personal trades are distinct from the public simulated trade-evaluator ledger. |
| Request-specific calculators | Wealth/tax HTTP calculations remain usable anonymously, return private/no-store responses and cannot persist caller financial scenarios. Browser requests use JSON POST bodies instead of URL parameters. Only explicitly marked, input-free default model runs may publish public snapshots. Old current snapshots are withheld until regenerated; old versions remain IAM-only. |

The configured portfolio-manager context is now an authenticated owner artifact. Its actual router handler reads canonical portfolio risk, portfolio snapshot and PM history, publishes the private mirror before its IAM original, rejects anonymous HTTP envelopes before reads, and never replaces the dedicated schema with a public deterministic fallback. Inspection of all 55 checked-in router contexts found only this context consumes account bodies; its downstream uptime reader projects timestamps only. Raw legacy holdings/history and discovered notification-recipient metadata are retained behind exact source/version/archive denies.

Three health reports now expose fixed diagnostic categories and typed metrics only: `_health/fleet.json`, `data/_fleet-monitor.json`, and `data/_freshness-monitor.json`. Existing CloudWatch/provider/SDK error strings are removed by deterministic legacy projections. Freshness reports require an explicit public-health schema marker and exclude private source keys; unmarked legacy reports become an unavailable placeholder. Migration never invokes normal notification handlers.

Ops5238 is a permission-only early containment operation. Its policy verification accepts documented IAM array ordering and scalar/list normalization while preserving exact permissions, conditions, principals, unrelated statements and unknown fields. Receipts distinguish write attempted, write acknowledged, and observed temporary-deny presence (`true`, `false`, or unknown). A failed readback is never evidence that the write did not happen. Safe policy diagnostics contain only hashes, counts and structural differences; object checks use HEAD without bodies. The full migration alone may remove temporary current-object containment after its complete checks.

INST-03's original Enterprise/unmetered bypass was already corrected in the previous source review. Its degraded quota fallback remains per process, not a global cross-container quota guarantee. Provider-literal cleanup and actual provider revocation are tracked by the release owner; source removal alone is not evidence of revocation.

## Reviewed source map and migration

`aws/shared/private_artifact.py` is the canonical map: 26 full private mirror kinds plus raw private exact keys and archive prefixes. `aws/ops/checks/audit_20260909_security.py` exports the 21 deterministic current keys and three JSON families requiring sanitization and their historical-version deny statement. The policy denies both anonymous and unrelated signed AWS accounts while preserving same-account IAM access.

The 44 exact-source readiness targets in `audit_20260909_privacy_migration.py` comprise:

| Boundary | Engines, all with the `justhodl-` prefix |
|---|---|
| Private producers and service readers (20 service-token targets) | brain-sync, journal-grader, my-brief, devils-advocate, notes-intel, playbook-engine, ask, portfolio-snapshot, portfolio-risk, portfolio-sizer, portfolio-catalysts, risk-sizer, pm-decision, behavior-mirror, ai-brief, history-api, watchlist, vol-regime, trade-journal, ai-brief-router |
| Public projection producers | brain-compiler, tv-workbench, canary-warroom, tradingview, domain-barometers, sizing-engine, best-setups, master-allocator, position-sizer, engine-conflicts, equity-research, provider-catalog, fleet-monitor, fleet-error-monitor, fleet-freshness-monitor, source-map, etf-fund-flows, macro-regime |
| Downstream public readers/caches | ask-desk, symdir, ai-chat, page-ai-commentary |
| Stateless personal scenario calculators | wealth-plan, tax-plan |

The direct-key closure trace is documented in `privacy-graph-closure.md`: 24 private paths then present, their legacy aliases, and 36 matching consumer files. It found the additional manual personal trade API. Watchlist and volatility were separately traced; the calculator request-persistence paths were found during the page/API classification. This is an explicit bounded source review, not a claim that every computed runtime path or live endpoint was tested.

The four owner-only history outputs (portfolio risk, sizing and catalyst alert dictionaries; behavior history) are now mirrored in full for authenticated inspection. Existing histories are retained; absent histories use only the corresponding producers’ exact empty schemas under conditional creation. History source errors never become empty successful reads. `PRIVATE_ARTIFACT_ALIASES` explicitly maps the risk recommendations duplicate to its canonical private mirror without double-seeding it. No private S3 resource becomes public through this coverage change.

Public model portfolios, simulated performance, public market intelligence, normalized PM macro posture, permitted structured note counts/scores and the public simulated trade-evaluation ledger remain public. Personal holdings, quantities, cash, manual notes/theses, watchlist membership, trade records, and caller financial scenario inputs do not.

Migration preserves originals and historical versions; installs permanent private/history denies and temporary current-derivative containment; validates exact deployed ZIP/shared-source membership; grants only the named service environment targets and exact SSM parameter; seeds authenticated mirrors; conditionally sanitizes current objects; rebuilds the provider index; resets warm caches; purges affected caches; and verifies metadata/HEAD access before lifting temporary containment. The full volatility original is bootstrapped before its public projection is reduced to core model rows. No calculator invocation is needed to remove old caller data. Any failed containment, parity or purge gate must leave temporary protection in place.

No new Durable Object binding, migration tag or Supabase DDL is required. Exact-source readiness applies to every earlier and later companion Lambda change. Same-account IAM readers need their existing S3 grants; services need the configured private publication token. Previously downloaded browser copies cannot be recalled.

## Local evidence and handoff commits

- 117 security/Fusion/browser/workspace JavaScript tests passed before the final calculator transport change. Its three new browser tests and all 30 Worker tests then passed together. They use actual Worker modules and extracted page functions, not source-string assertions alone.
- AI Brief: six actual-handler checks. Personal trade API: six. Private portfolio producers: 30. Downstream AI/history: 20. Watchlist: ten; volatility: two. These are offline handler/source tests with external services mocked.
- The final migration has 21 offline checks; the public Brain boundary suite has 14. Earlier private Brain/grader/Ask/Ask Desk handler checks also remain available through their committed runners.
- Owned Python preflight and JavaScript/inline-page parsing passed. Root's joint release gate is the authority for the assembled release; no duplicate broad gate was run after the root took that work.

Primary security foundation: `eede659`, cache follow-up `75d2d04`, Fusion `4ca378a`, note derivative protection `3facbdf` and `eceb239`. Account foundation/UI: `b7cd7e3`, `98e4776`. Companion account producers: `7090342`, `233495f`, `8d1b7ba`, `9e690fb`, `c1b90b3`, `b99ff75`. Downstream AI/history: `12e9041`. Migration and later extensions: `58d2b32`, `5cef5c2`, `2a6fc87`, `dd2fbe5`. Calculator producer and final browser/cache commits are recorded by the release owner when assembling this handoff.

Rollback must retain permanent privacy policies and durable journal/billing state. Do not restore public original fallbacks or advisory profile/KV authorization. Production closure requires recorded exact-code deployment, temporary-containment removal only after successful scrub/purge, anonymous denial and authenticated no-store access checks. No such closure is asserted here.

The TradingView attribution landing key `data/tv-sources.json` contains authenticated browser source labels, descriptions and diagnostics. It remains IAM-only, including its legacy aliases and historical versions. The separate `data/source-map.json` publishes only fixed family identifiers, qualified market symbols and typed counts/timestamps; source-map no longer rewrites the ingest landing file. Its page rejects legacy unmarked data. The migration replaces unmarked legacy rollups with a fixed unavailable projection and validates every marked field again.

ETF flows and macro regime preserve real successful market metrics while replacing provider error response text with fixed diagnostic categories. The same pure projection applies to HTTP responses, current publications and historical writes. Migration sanitizes `etf-flows/daily.json`, `macro/regime.json` and every JSON object in `etf-flows/history/` and `macro/history/`; historical object versions remain IAM-only. Worker requests for both current and historical families bypass old cache generations. Temporary external-read containment remains until the entire family scrub and verification succeed; receipts count each reviewed prefix separately.
