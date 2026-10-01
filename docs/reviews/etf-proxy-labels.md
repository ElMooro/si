# Legacy ETF trading-activity proxy labels

Review baseline: main `0035bf47da741a2916a0e34df173934e5ae297aa`; synthetic predecessor fixture generated from `39064c633`. Original pre-release review evidence follows; see the release handoff below for deployment status.

## Problem and repair

`justhodl-etf-flows` computes daily close times traded shares, rolling activity statistics and price returns from Polygon bars. It does not observe creations, redemptions, changes in shares outstanding or net fund cash flows. Its old introductory documentation claimed otherwise. Current ticker reference data supports the existing AUM calculation, not a historical issuance measurement.

This change adds `measurement_type`, source and `actual_fund_flows_measured: false` to the packet, plus per-row `price_volume_measurement`. The latter supplies the provider bar-start `as_of`, explicit units, a close-times-volume approximation, missing-field names and null `net_fund_flow_usd`. A zero traded volume is retained as zero; missing, boolean, nonnumeric, negative volume and nonfinite measurements remain unavailable. Missing bar timestamps are never replaced with publication time. These clocks make no freshness claim.

Livermore and Wyckoff render the legacy enums as trading-activity proxies and show a reported daily-bar clock or unavailable label. They link to the existing `/etf.html` evidence page. The Livermore preset's visible label changes while its stored token remains compatible. Table rendering escapes labels and preserves a real zero change. IDs, navigation, scan persistence and ranking functions remain intact. The dormant `jh-etf-engine.js` receives additive display/measurement/clock fields; existing `flow`, `flowRaw`, dollar-flow fields and rankings remain untouched. Legacy paid-flow values are explicitly unverified, not upgraded into qualified flow evidence.

## Consumer inventory and unresolved semantics

| Surface | Actual role | Treatment |
|---|---|---|
| `aws/lambdas/justhodl-etf-flows/source/lambda_function.py` | Actual exact-key writer of `data/etf-flows.json` | Additive provenance; corrected descriptions only; enums, thresholds, calculations and output version remain compatible |
| `livermore.html`, `wyckoff.html` | Active exact-feed scans | Honest display labels; score functions unchanged |
| `jh-etf-engine.js` | Legacy reader; no current HTML script reference found | Additive annotations; existing decisions and aggregate behavior unchanged |
| `jh-strong-engine.js` | Legacy reader with native fuse route | Unchanged; native fuse clears legacy flow inputs |
| `aws/shared/brief_compiler.py:compile_market_tape` | Counts HEAVY/ROTATION enums into `heavy_inflow_n`, `heavy_outflow_n`, `other_flow_n`, breadth; summary says heavy in/out/other | **Unresolved legacy naming/interpretation.** No compiler edits: shared surface has a separate active official-stats claim and deployment affects importers. Required input, TTL, counts, votes and readiness remain unchanged |
| `aws/shared/jh_brief_adapters.py`, `aws/shared/jh_adapters.py:EtfFlowsAdapter` | Decision consumers of legacy counts, z-scores and direction | Unchanged; proxy inputs are not converted into actual fund-flow qualification |
| `jh-khalid-sniper.js:228–231,325–337` | Truthy `meta.flows` passes flow checkbox; text assembled from `Major fund inflow`, `Institutional ownership flow`, `Persistent flow score` | **Unresolved decision/display mismatch:** nonempty accepted label text is not proof of verified net fund flow or 13F net buying. This PR neither fixes nor validates that gate. Requires separate coordinated decision review; no Khalid/Katlin files touched |
| `justhodl-alert-router:check_etf_flows` | Legacy alert reader expects category lists/signal fields inconsistent with this writer's current category object | Separate existing schema mismatch; not repaired or activated |
| `justhodl-wave-signal-logger:log_etf_flows` | Directional event logging from legacy heavy/rotation enums | Unchanged; historical events are not relabeled as verified fund flows |
| `justhodl-capital-flow` | Legacy handler reads feed; current handler uses research store | Current `capital_research.py` explicitly retires price/volume-derived flow scoring; don't confuse legacy function with current handler |
| `sector_fusion_store.py`, history snapshotter | Context roots / archival read list | Unchanged |
| ETF census, portfolio analytics | Source documentation mentions feed | References inventoried; not asserted to be active execution paths merely from documentation |
| Engine registry, page-AI source maps, discovery indexes and historical ops | Routing/documentation/test/old brief construction references | Unchanged; not additional current writers |

Generated `config/artifact-producers.json` lists crypto/Finviz writers as well: actual writer paths are `data/crypto-etf-flows.json` and `data/finviz-etf-flows.json`, not this exact key. Basename-based catalog matches must not be treated as proof of shared ownership. Catalog correction is separate.

No matching recent 72-hour edits or active claim conflicted with the four production targets at the checked base. Our claim is `S-shopiz#proxy1001a`. Shared compiler and separate Khalid/Katlin work are preserved.

## Regression evidence

- Five dependency-free producer tests AST-load real functions without constructing AWS clients. Synthetic bars, fixed clock and in-memory publication stub exercise the actual handler. The complete legacy packet, rankings, enums, category summaries and handler result match the predecessor fixture after removing only new metadata and intentionally changed display definitions.
- Metadata assertion fails on predecessor output (key absent) and passes after repair. Analyzer/classifier AST comparison, excluding only docstrings and the new return field, is identical. Both page scoring function sources are identical to the predecessor.
- Four Node regressions exercise mapped/unknown/hostile enums, unavailable and epoch timestamps, actual inline scan rendering on both pages, escaped content, zero display, retained enums and dormant desk zero/missing annotations.
- Local Chromium synthetic click-through on both pages at 1440 and 390px: scan output and Livermore preset work; no horizontal overflow. Every external request is blocked and brief persistence disabled. No private Brain calls, provider calls or live acceptance claimed.
- Validation: 2,238 frontend tests, 5 producer tests, 59 bridge tests, 1,003 deployment static tests and 15 shell tests passed; 599 page graphs had zero syntax errors; wiring checked 36 pages / 143 assets with no missing or stale entries. Final command results are also recorded in the draft PR. Source/config validation, preflight, secret scan including staged additions, complete frontend suite, page-script/wiring and deployment safety tests are required before commit.

## Cost, limitations and rollback

No extra provider calls, Lambda invocation, S3 request, schedule, resource setting or data source. New metadata enlarges the existing publication. The three-fund invented fixture serialized with identical JSON settings grows from 4,396 to 7,540 bytes. A typical per-row metadata object is about 539 bytes before its key; 69 configured unique funds plus at most 50 duplicate notable-list rows imply roughly 66 KB additional metadata at that configuration, not a measured live packet size. Two pages load one small additional same-origin script. No production capacity or data availability claim is made.

The additive fields do not repair pre-existing missing-to-zero behavior inside legacy calculations or legacy empty `netFlow=0`; the separate measurement describes unavailable values honestly. Old packets render without a clock, never with invented freshness. Display labels do not qualify stale or future data for investment use. Actual provider flow evidence, decisions, votes, scoring, vetoes, sizing and capital behavior are outside this change.

Rollback is a normal revert of this single commit, restoring the previous writer and display files together. Readers remain compatible with either packet because legacy fields/enums are unchanged; the new script can be removed with its two references. No retained-policy/replay migration, provider rollback or AWS operation is required. Reverting would restore misleading wording, so use only if this change causes a regression. This draft must receive review before merge/deployment; live producer and page acceptance remain post-deployment conditions.


## Release handoff — 2026-10-01

PR21 reviewed head `e457eba9675e4d66f67c7b1535482d7c5e8ed0df` merged as `ca5462aebca056c1d097363a9068032886a0f0db` at 03:41:25 UTC. Its tree `824a50b443b27f9147a976c44d6c05ecbf83dd6d` exactly matched the final combined-tree staged inventory on main `51eff1e2045bbb8d7fef2f5a5d1e5e944a5435e8`. All nine reviewed files were unchanged. Final combined-tree checks: 2,243 frontend, 5 producer, 59 bridge, 73 brief-contract, 1,008 deployment and 15 shell tests; source/config, preflight, secrets, page syntax/wiring and inventory passed.

- Lambda workflow [36811670782](https://github.com/ElMooro/si/actions/runs/36811670782): success.
- Pages workflow [36811670778](https://github.com/ElMooro/si/actions/runs/36811670778): success, including actual deploy step.
- Exact-commit page gate 36811670807 and stub guard 36811670753: success.
- Factory exam [36811753630](https://github.com/ElMooro/si/actions/runs/36811753630): success, completed 03:49:16 UTC. This was a **scheduled** run on the merge SHA, not a push-triggered consequence. Both release-audit workflow runs (36811937932, 36812019730) also completed successfully. No run in that checked SHA set remains pending.

**Release status: deployment workflows and edge assets verified; Lambda receipt and new natural output pending.** The canonical receipt request returned HTTP 403 and was not retried or redirected to another host. GitHub artifact/log download endpoints were also denied; no bypass attempted. Expected producer source SHA-256 is `e104280f4b42eafe6d5240b732348f589c823eef75cd46fe4c7a67caf499ee8a`. A green workflow alone is not described as independently verified Lambda receipt contents.

At 03:46 UTC, the public build manifest named the exact merge SHA. `jh-etf-proxy-labels.js` and `jh-etf-engine.js` matched both manifest and reviewed source byte-for-byte. Both public HTML pages contained the exact commit marker, proxy disclaimer, helper reference, existing ETF link and scan IDs. Their raw edge bytes differ from build bytes solely by an injected Cloudflare analytics script (366 bytes) plus its following newline: removing exactly that identified injection yields each expected manifest hash. This is an explicitly normalized HTML comparison, not a raw-byte equality claim. No Cloudflare setting changed.

### Parent managed-browser handoff

Public URLs: https://justhodl.ai/livermore.html and https://justhodl.ai/wyckoff.html . Check both at 1440px and 390px with normal TLS/browser settings.

Common visible marker: “ETF classifications below are daily price/volume trading-activity proxies, not measured fund inflows or outflows. Legacy ranking rules are unchanged.” Link: “Open dated ETF flow evidence” to `/etf.html`.

- Livermore: visible preset “ETF price/volume proxy”; stored DOM selector remains `[data-p="liquid ETF inflow"]`. Brief textarea `#q`; “Save brief & scan tape” button `#go`; results `#out`; status `#meta`.
- Wyckoff: existing accumulation/spring/etc. chips `#chips`; campaign textarea `#q`; “Save campaign & scan range” button `#go`; results `#out`; status `#meta`.
- After an isolated public scan, result heading is “ETF trading-activity proxies”; rows use trading-activity/price-direction wording rather than raw HEAVY_INFLOW/OUTFLOW labels. Old packets without the new row clock must show “Bar time unavailable”; do not infer freshness or measured fund flows.

**Read-only QA caveat:** both existing save-and-scan buttons call `persist()`, which writes local history and attempts a Brain PUT. Do not activate that path unmodified for this public read-only acceptance. In an isolated browser, disable persistence (`persist = async () => {}`) and block mutation requests before a scan, as done in synthetic tests; otherwise limit QA to initial visible controls/layout and leave scan acceptance pending. No private Brain contents or saved user history are needed. The local normal-sandbox Chromium launch was blocked before requesting a site; no sandbox/TLS override was used for live QA. Parent owns managed-browser acceptance.

### Natural publication handoff

The latest observed public feed was still `2026-09-30T11:35:42.721302+00:00`, with 69 rows and no new measurement metadata. Code deployment is not a fresh-output proof.

Cadence evidence: `config/schedule-manifest.json` records enabled rule `justhodl-etf-flows-6h` with expression `cron(35 11 * * ? *)`; `aws/ops/reports/latest/ops_5213_fusion_r1_diag.md:71` independently records the same enabled rule/expression and an 11:35 publication. Despite the historical name, the recorded expression is **daily 11:35 UTC**, not six-hourly. No new live AWS schedule probe was performed.

Parent's next public readback: **2026-10-01 11:37 UTC**, two minutes after the expected natural start. Check publication advances, root proxy metadata appears, and row source/bar clocks and null/zero distinctions are retained. Two minutes is a first check, not a completion guarantee (configured timeout 600 seconds). If unchanged, report it as pending; do not invoke the producer or alter schedules. Parent owns this check; no automation created.

Costs remain additional existing-publication bytes plus one small same-origin script on each page; no extra provider request or schedule. Rollback remains a normal scoped revert of PR21, preserving unrelated releases and legacy decision contracts.
