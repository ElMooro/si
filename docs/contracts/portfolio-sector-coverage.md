# Reported sector exposure and classification coverage

Snapshot accounting and the risk model share `aws/shared/portfolio_sector_exposure.py`. Unknown classifications are a coverage gap, never an economic sector. Every input row, original label, selected reported label, identity, signed mark, gross mark and group reference is retained. Research enrichment takes precedence when present; a source-book fallback is explicitly identified in snapshot rows.

## Measurement

- All sector percentages use unrounded absolute quantity-times-price lot exposure divided by the complete gross marked lot exposure. Long and short lots do not cancel in this denominator. The risk model recomputes from the retained quantity and price instead of treating the snapshot's two-decimal display amount as an operand; the displayed amount remains separately recorded. The snapshot's existing group value remains signed; its added gross value and weight scope identify the revised percentage definition.
- Missing, blank, malformed and conventional unavailable labels remain unclassified. Generic `Other`, `ETF` and `Fund` labels are also not treated as an industry. Original text is never replaced in the evidence. Surrounding whitespace is removed only from the selected grouping label; no taxonomy alias or sector inference is invented.
- Conflicting reported labels for the same instrument make every affected lot unclassified. Matching duplicate lots remain separate contributors. A missing label on a zero-value lot remains an unknown record but cannot prevent otherwise complete nonzero exposure coverage.
- Known-sector shares are never normalized to the classified subset. Unclassified amounts, percentages and counts remain separate. A whole-book HHI and maximum sector share require every nonzero marked exposure to have a reported sector label. Missing marks withhold full-book weights and HHI while retaining available priced amounts.
- A known sector strictly above 40% of complete gross marked exposure supports an exposure alert even with some unclassified exposure. Otherwise incomplete classification returns an unknown threshold result, not a clean bill of health. Threshold comparison uses unrounded values; display rounding cannot change it. Empty and entirely zero books have no meaningful sector percentages or HHI.
- These are binary64 descriptive calculations over reported classifications. Provider taxonomy, vintage, actual instrument classification, currencies and ETF constituent look-through remain unverified. No sizing or trade authority is granted.

## Consumer and replay

Risk version `2.0.2` binds the shared classifier in its full replay compiler identity. The browser reconciles every record, count, amount, group member, weight, HHI and threshold result, and matches each record to its corresponding loaded holding. Legacy or inconsistent sector evidence cannot display an HHI or exposure alert. Existing holdings risk remains subject to its separate contract and whole-snapshot binding.

The page shows classification coverage and unknown exposure separately from the sector bars, with complete metadata in a keyboard-accessible disclosure. Failed loads clear preceding sector content. A coverage gap is not a concentration claim or an absence-of-risk claim.

## Acceptance limits

Tests use whole invented books and complete current-source browser frames; retained predecessor sources are inspected as inert bytes/AST only. No actual account, provider or downstream-consumer packet is read, no native producer is invoked, and no schedule is accelerated. Native acceptance requires exact release receipts, complete source packages and unchanged resource/schedule bindings for both snapshot and risk. Actual private publication, source classification validity and investment qualification remain unverified.
