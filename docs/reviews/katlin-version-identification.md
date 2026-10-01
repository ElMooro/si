# Version identification after the Katlin permission repairs

This metadata-only follow-up identifies the reviewed, deployed behavior from PR52 (required funding hold), PR56 (regime/credit/volatility abstention) and PR54 (current permission required for actionable Katlin alerts).

The user supplied an engine-version bump requirement for this follow-up. The checked repository instructions (`CLAUDE.md`, `AUTONOMY.md`, `DEPLOY_LANE.md`) require deployment verification and unique live identification; no blanket engine-version bump text or `.agents/skills` files were found in this checkout. The supplied user requirement is authoritative.

Katlin's existing `VERSION` changes from 2.5.1 to **2.5.2**. Existing daily and permission-refresh publications already carry it in `version`. Output schema stays **1.1**. No decision, weight, threshold, clock, data-source criticality, basket, research field or policy changes.

Alert-router gets its first explicit **ENGINE_VERSION = 1.0.0**, emitted only as additive `engine_version` in its next normal history publication. This starts an engine implementation version series; it does not reinterpret the existing history `version` (default 1.0) or generic webhook-envelope `version` (1.1), which are format identifiers and remain unchanged. Alert selection, permission checks, transport payloads, destinations, timing, dedup state and historical events are unchanged. The existing guarded public-history projection is not modified or bypassed.

Tests exercise Katlin daily/refresh output versions and schema preservation, plus the actual alert handler with offline allowed/blocked/duplicate cases. An AST comparison binds the handler change to its single history-identification assignment; it cannot hide policy changes. No live test sends or manual invokes are used.

## Explicitly excluded policy

The released PR56 removes only regime-composite, credit-stress and vol-regime risk votes. **Auction and raw-gate local fallback votes remain outside PR56, PR54 and this follow-up.** The auction fallback remains 45; an unknown nonempty raw-gate posture can still contribute 50 locally. The separate required raw-gate contract hold remains binding. Neither their eligibility policies nor these fallback values are changed here. The accurate scope is regime/credit/volatility abstention, not all possible unqualified inputs.

Volatility's Katlin mapping remains unresolved; every volatility state abstains rather than receiving an invented calibrated risk score. Producer-native measurements remain research context.
