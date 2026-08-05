# SHIPPED — crisis.html / plumbing.html / liquidity.html

**Author:** perplexity  
**Timestamp:** 2026-08-05T15:07:00Z  
**Reason for drop:** A2A bus returned `budget_exceeded: max turns` (thread `0805045237`).

## Delivered

All three desks rebuilt to the 2026-08-05 institutional bar:

1. Barometer (0-100 gauge) at top with color palette matching state
2. 1996+ regime strip w/ 8-9 crisis bands labeled (Asian, LTCM, Dotcom, GFC, EU-Sov, CNY, Q4-18, COVID, SVB)
3. Historical percentile bar showing where today sits vs 26yr / 104w history
4. Component decomposition with weight + stress + missing-data flag per leg
5. Predictive read (forward SPX by posture on crisis; marginal impulse on liquidity)
6. Provenance footer with age chip per feed

Shared library added: `/jh-institutional.js` + `/jh-institutional.css`. Both served commit-versioned. Every value comes from live producer feed. Playwright QA on live URLs shows zero `undefined%` / `NaN` / `[object Object]`.

## Notes for Claude / backend

- **crisis.html** renders 7-conjunctions slot; awaiting `red-alert-conjunctions.json` engine (F10 queued in directive `0344b3916061a8d6`).
- **global-tide.json** ships `fed: {}` — the Fed cell on liquidity.html renders `—`. Producer-side gap; fix inside `justhodl-global-tide` producer.
- **OFR FSI** 27.2th percentile / -2.57 z reading rendered from `ofr-stfm.json` (working).
- **Forward-returns table** on crisis reads `risk-gate.json event_study.flips` directly; posture aggregation done client-side. When risk-gate F1 joins are fixed, no page changes required — data will flow through automatically.
- **`regime.json`** 365-mo 1996+ strip renders producer's regime-engine version (P0-8 owner call still awaiting Khalid).
- **`plumbing.html` yield-curve panel** auto-hides when `crisis-plumbing.yield_curve` is empty — no cosmetic gap. If a producer ships a `yield_curve` block later, no page change required.

## Live URLs

- https://justhodl.ai/crisis.html
- https://justhodl.ai/plumbing.html
- https://justhodl.ai/liquidity.html

## PR

- https://github.com/ElMooro/si/pull/9 (merged, commit `8d5bc41`)
