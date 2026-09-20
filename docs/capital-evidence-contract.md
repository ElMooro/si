# CapitalFlow research contract

`capital-evidence-research.v1` replaces the legacy stock cash-flow score with three separate measurements. It grants no Calls, sizing or execution authority. Original observations and earlier calculations remain retained.

## Reported holdings-value bridge

Every identity in either of the two selected public manager disclosures is retained. Identity is the exact reported CUSIP, class, SH/PRN and PUT/CALL; no guessed ticker join is permitted. A missing or incomplete public chain is unavailable, not an empty portfolio. Current and historical managers retain their own report periods.

For a matched non-option SH identity, with positive quantities and no unresolved source valuation review, let `Q` be reported quantity and `P = reported USD value / Q`. The exact symmetric accounting identity is:

`ΔV = ΔQ × (P1 + P0) / 2 + ΔP × (Q1 + Q0) / 2`.

The terms are quantity-associated and implied unit-value-associated changes. They are **not** purchases, sales, causal market-price attribution or corporate-action-adjusted ownership. Splits, discretion changes, public omissions and interim trades remain unresolved. Options, principal amounts, unmatched identities and unresolved valuation reviews remain visible without fabricated decompositions.

Per-security amounts retain exact rational numerators and denominators. Decimal displays round half-even. Adding exact rational terms over thousands of unrelated denominators can produce enormous least-common-denominator integers; scope totals instead sum cent-rounded per-security terms and retain an explicit rounding adjustment. The tests include 2,048 unrelated denominators, repeating fractions, very large quantities and split-shaped disclosures.

Each instrument scope reconciles:

- matched value change + newly present value − absent prior value = complete reported table change;
- rounded quantity terms + rounded unit-value terms + rounding adjustment + matched changes not decomposed = matched value change.

No cross-manager dollar total is produced. Unmatched values are disclosures, not assumed executions.

## Fund issuance and Treasury transactions

The pinned Global Flow Desk supplies all configured fund records, original issuer evidence/history links, exact five-observation estimates and coverage. Changes in reported fund shares valued at NAV do not establish constituent-stock purchases, investor identity or national flows. The monthly Treasury context retains its series definition and USD-million unit, distinct from USD-valued fund estimates and holdings values. No sum or independent voting bonus combines these families.

## Retention and publication

`data/capital-flow.json` points to a content-addressed manifest under `data/capital-research/runs/`. That manifest binds reviewed compiler bytes, immutable source manifests/outputs, the complete output, manager artifacts and the complete preceding public packet/history. Both preceding products are also backed up privately and the old history key remains unchanged. Legacy calculations are explicitly unqualified; their missing provider originals are not invented.

Validation and complete calculation precede writes. Immutable writes require full readback. The mutable pointer uses an ETag condition; a concurrent newer writer is not overwritten. Unchanged sources/compiler do not refresh calculation or collection clocks. Expiry can publish stale status while preserving source dates. Source and publication clock regressions are rejected or left unchanged. HTTP reads never invoke the retired collector, secret lookup or calculation.

`python scripts/replay_capital_research.py --run <sha256>` checks original SEC disclosure replay, ETF issuer originals/histories and Treasury originals through the TIC view, then reproduces every manager bridge and the whole output. Derived output hashes alone are insufficient. Use the matching reviewed checkout; a compiler mismatch fails explicitly.

## Consumer boundary

`capital_research_boundary.py` marks the direct CapitalFlow input as excluded from stock score, distress flag, agreement count, denominator, universe and trade narrative. Forged eligibility flags do not reinstate it. Flow Confluence, Deep Value, Equity Confluence, Best Setups, Industry Rotation, Engine Conflicts, Narrative vs Tape and Ask use this boundary. Master Ranker rejects Flow Confluence packets from before this boundary; the scored component list must also exclude CapitalFlow. Best Setups rejects the old flow annotation map, and Equity Confluence rejects the previous Deep Value calculation.

These are bounded direct-path repairs, not certification of all indirect families or the remaining heuristic engines. Compatibility empty stock rankings mean “no qualified stock cash-flow ranking”; they do not mean zero institutional activity. The previous narrative compatibility key remains but no longer calls a dislocation-screen result institutional buying.

The producer retains its existing daily 16:30 UTC EventBridge binding. The page exposes pinned snapshots, all manager records with pagination, exact evidence, source periods, unresolved reviews and distinct fund/Treasury sections. Browser arithmetic uses integers rather than floating-point approximations.
