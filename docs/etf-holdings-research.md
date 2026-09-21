# Dated ETF holdings research

ETF Constituents now reconstructs complete current and earlier provider snapshots for all 300 configured funds. Flow Lookthrough is an inspectable projection of the same canonical evidence. Neither creates an independent investment vote or infers purchases from ETF flows.

## What each observation means

Every holding keeps its exact original response hash, page and row position. Effective dates describe the reported positions; processing dates describe the provider snapshot; acquisition time describes our source check. A new compilation changes none of those clocks. The earlier query cutoff is 30 calendar days before the current cutoff, not a guaranteed 30-day measurement interval.

Missing tickers do not delete cash, bond or derivative rows. Exact provider identifier tuples include listing and classification attributes. Ticker equality alone never joins instruments. Missing or ambiguous identities remain visible as individual fund rows. Dated cross-fund membership does not establish common-date ownership, an allocation weight or independent evidence.

Raw provider weights are retained without multiplying, dividing or forcing their sum to 100%. The provider documentation calls them percentages, while examples and reviewed original snapshots present an unresolved scale convention. Trading currency does not certify market-value currency. Position units can describe bonds or derivatives rather than equity shares. These qualifications remain explicitly false until separately reconciled to authoritative issuer definitions.

Unadjusted position differences require two complete snapshots on distinct, single effective dates and an exact unique identity. Missing positions never become zero. Corporate actions, trades, accumulation, fund creations and index rebalances cannot be inferred from the resulting difference alone.

## Reproducibility and publication

Original responses and entire predecessor packets are protected content-addressed objects. Legacy current and v2 holding caches are inventoried and retained whole. Complete predecessor handlers and the predecessor page remain byte-identical archives and are not imported. Collection uses only the existing provider subscription, bounded hosts, pagination, bytes, request rate and runtime. Credentials are headers only and never persisted in source evidence.

The compiler emits immutable row, index, comparison, directory and membership pages. Inputs, compiler bytes, output and run manifests are retained. Publication reconstructs all calculations from originals and compares every emitted page before replacing the current packet. Conditional writes reject publication and source-processing regressions. Durable request claims prevent blind duplicate collection. An unavailable collection cannot replace the current canonical publication; an individual failed snapshot may retain a separately labelled prior good snapshot with its original clocks.

Canonical outputs are `data/etf-holdings-research.json` and `data/flow-lookthrough.json`. Four old pressure/lookup paths become explicit research-only compatibility references, preserving their complete old bytes privately. The downstream research guard rejects their use as legacy trading evidence. No private account, signal, notification, portfolio mutation or paid AI is used.

## Page behavior

`etf-holdings.html` and `flow-lookthrough.html` verify retained run/output bytes and each requested content-addressed page. All returned holdings remain searchable and paginated, including late source pages. Global security search exposes exact-identity memberships with separate fund dates. Selection changes invalidate scenarios and asynchronous responses cannot replace a later selection.

A browser-only hypothetical calculation uses the user's signed USD fund position, assumed holding exposure percentage, assumed shock and costs. It never uses unqualified source weights. The result is a linear assumption-driven P&L, not measured portfolio exposure, a recommendation or derivative repricing. Stale, unavailable and wrong-basis snapshots reject calculation.

## Acceptance

The runner must verify exact commit receipts and actual ZIP/import closures for all affected functions, replay all eight preserved preflight snapshots, invoke only the two reviewed public producers once, replay their complete originals, independently check source-row arithmetic and memberships, prove protected originals remain inaccessible anonymously, and verify public assets and aliases. A green workflow alone is insufficient. Runtime memory/time headroom, source dates, large snapshots, stale inputs, signed assumptions and mobile browser behavior are separate acceptance checks.

The remaining qualification work includes issuer/vendor weight reconciliation, derivative exposure mapping, security master history and corporate-action adjustment, compatible valuation dates/currencies, supported user portfolio inputs and out-of-sample predictive evidence. This stage deliberately does not claim those capabilities.
