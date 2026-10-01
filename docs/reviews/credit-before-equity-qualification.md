# Credit-before-equity evidence qualification (draft)

The existing page could render missing counts as zero, a missing payload as “No lead firing,” and a legitimate zero flat-band threshold as 3%. An engine-declared missing credit leg appeared below the tables while the summary still implied a no-signal result.

This frontend-only change adds a prominent evidence-status panel, preserves numeric zero, displays unavailable/invalid numeric fields without coercion, distinguishes degraded/incomplete, no-issuer, awaiting-history and engine-reported no-lead states, and retains the supplied populated rows and signals. Backend decisions, thresholds, feeds, navigation and schedules are unchanged. The backend caps its lead preview at 20; the page labels that preview and retains every supplied issuer row.

Source and consumer:

- Consumer: `credit-before-equity.html`, original inline loader and renderer.
- Existing output: `data/credit-before-equity.json` (original fetch URL unchanged).
- Producer: `aws/lambdas/justhodl-credit-before-equity/source/lambda_function.py`, `OUT_KEY`, output dictionary and `leads[:20]`.
- Qualification uses published `degraded`, `gaps`, counts, arrays and `thresholds.equity_flat_band_pct`. It checks transport types/count consistency, not investment validity; it never recomputes a signal.
- `generated_at` is engine packet generation time. The output supplies no credit/equity input clocks. Even a parseable recent or future timestamp is not called fresh. No TTL is guessed.
- Payload text is escaped before HTML insertion; original table gains a named, focusable horizontal-scroll region. Existing navigation URLs are preserved.

## Validation and limits

All observations below are local source or synthetic/intercepted tests, not deployed acceptance:

- Eight executable inline-consumer tests cover missing/null/wrong types, genuine zeros, zero threshold, degraded-empty/no-issuer/awaiting-history/valid-no-lead states, populated rows, the 20-lead cap, count inconsistencies, XSS, generation-time semantics and HTTP/network/JSON failures.
- `node --test tests/*.test.js`: 2,251 tests passed.
- `node tests/credit-before-equity-browser.cjs`: Chromium at 1440 and 390 pixels, populated and empty cases, escaped hostile text, zero threshold, no page overflow, Tab access to the table, mobile ArrowRight scrolling, and following cross-link focus. Every browser request is intercepted and fulfilled or aborted; no AWS/provider/private access. Screenshots are local `/tmp/credit-qualification-{1440,390}.png`.
- Required source gates passed: secrets scan, Brain public boundaries (15), page-script regression (6), 599-page syntax scan, sovereign site assets (3), engine wiring check (143 attachments/36 pages), forbidden literals, offline build boundary (6), and whitespace check.
- Local offline site assembly exercised workspace generation, sections, guarded offline bakers, reskin, asset stamping, SEO, compiled/embedded access contracts, final 599-page syntax validation and build-manifest stamping. No deploy or upload. The dash budget file is absent in this checkout; the workflow's bootstrap behavior permits the observed 818 placeholders. The temporary build manifest names the then-current claim commit and is only local build evidence, not a release receipt.
- Additional deployment contract tests are invoked directly because this environment lacks pytest; the selected functions use no pytest fixtures: page data contracts 29, offline build 1, navigation publication 10, all passed.
- `build_page_data_contracts.py --check` reports existing stored-registry drift in `justhodl-symdir` and five unrelated consumers (`chart-pro.html`, `index.html`, `workspace.html`, `chart.html`, `archive/exponential-search-dashboard.html`). The compiled credit-before-equity contract and fleet classification counts are unchanged. The local build uses freshly compiled contracts; this draft does not regenerate unrelated checked-in inventories.

Before merge: independent review and normal required checks. After an independently approved merge/deploy: verify exact public page/build-manifest bytes, compare the public packet with the rendered counts/status/threshold, and exercise the page at desktop/mobile widths with keyboard navigation. No production parity claim is made here.

## Bounded fleet inventory context

The initial read-only inventory at public main `ca5462aebc` covered the complete **599-route / 898-engine access-contract registry**, not complete rendered-field parity. Local compilation for this slice retains those counts:

| Contract classification | Routes |
|---|---:|
| Primary valid access contract | 264 |
| Primary partial | 276 |
| Support-only | 14 |
| No association | 3 |
| Not applicable | 42 |

There are 598 embedded inspection routes and one standalone route. Overlapping static gap flags: 230 unresolved writes, 150 unindexed families, 50 unresolved primary references, 33 withheld outputs. Withheld/private outputs are not automatically public render gaps. The 69 reviewed internal-storage exclusions stay excluded; private Brain, secrets and restricted/account records must remain private. Technical metadata may be inspection-only or intentionally unrendered.

The source navigation manifest lists 543 routes. Its 56-route difference comprises 52 nested paths and four intentional redirects; it does not prove 56 unreachable pages. The authoritative served `/nav-manifest.json` and `/build-manifest.json` returned HTTP 403 from the initial audit environment; the web tool could not open navigation either. No live outage or live rendering bug was inferred from that access limitation.

Correlation interpretation/units and shared `jh-wire.js` preview/freshness findings remain separate future work. Khalid scorer/backtest, heatmap navigation, ETF desk extras, proxy labels and other active claims are excluded from this change.
