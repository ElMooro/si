# Risk remediation and deployment checks — 9 September 2026

Implemented against the audit of `34ddd51`, starting from `125a68a`. No cloud action was performed by the risk implementation agents.

## Changes and evidence

- **FR-02 / RV-01:** Katlin and Risk Sizer share `aws/shared/capital_contract.py`. Capital permission requires the expected engine/schema, finite coherent cap, boolean entry decision, supported mode/status, all five canonical critical sources with bounded SLAs, no inconsistent vetoes/failures, valid clock skew and an expiry no later than the earliest critical-input deadline. Khalid Risk publishes and validates this expiry. Katlin filters stale/future local evidence and requires a usable raw Risk Gate.
- **FR-04:** Sizer requires the complete `portfolio/snapshot.json.capital_book` v1.0 contract. Equity NAV is the denominator; signed positions and absolute exposure remain separate. Same-symbol lots aggregate; remaining orders reserve name/cluster/gross capacity; existing correlated holdings count toward cluster limits. NAV history must belong to the same account/book and end at the reconciled current equity. Missing, unpriced, stale, inconsistent or incomplete books hold all new sizing.
- **RV-03:** Allocations round down to the published precision. Failed final constraints block every row. Katlin's separate basket redistributor was also corrected so excess budget remains cash instead of violating its 10% name limit.
- **FR-09 / FR-12:** Risk Gate, Khalid Risk, Katlin and Risk Sizer expose nested complete evidence and JSON. Invalid/expired evidence remains inspectable; timers and visibility changes remove expired capital permission without waiting for a network refresh.
- **FR-10 / FR-11:** Governed fusion validates only present optional alternatives, with production-registry tests; deliberate scoring exclusions no longer force DEGRADED. Exclusions remain visible separately. Homepage validation accepts the corrected policy.
- Risk Gate consumes real ACM term premium, including tenor values/decomposition, and scoped Treasury settlement fails with units, completeness, dates and arithmetic checks. These are live inputs; historical replay remains separate.
- JPLG's generic IMF loan-level route is prevented from overriding curated BOJ YoY. The versioned `% YoY` cache contract invalidates incompatible cached rows automatically. Risk Gate also checks the explicit unit/version contract.

`python aws/ops/checks/audit_20260909_risk.py` runs the offline regression gate: 67 Python tests and 20 JavaScript tests passed at this handoff. The checker can independently validate captured artifacts with `--skip-tests --artifact ENGINE=PATH`; it has no cloud/network access. It does not certify deployment parity or execution performance.

## Deployment order and prerequisites

1. Bundle `aws/shared/capital_contract.py` with Khalid Risk, Katlin and Risk Sizer; retain existing shared module packaging. Risk Gate also needs its local `donor_contracts.py`.
2. Deploy governed fusion and the corrected TradingView adapter; refresh JPLG through the approved producer path. Existing cached `family:LG` or unversioned rows must not remain eligible. No manual deletion of unrelated cache entries is required.
3. Deploy Risk Gate; candidate `{"mode":"validate_only"}` computes the real artifact without S3 publication. After promotion, publish its updated artifact and check ACM/fails source health, JPLG units and composite reconciliation.
4. Deploy Khalid Risk and publish an artifact containing `expires_at` before expecting new consumers/pages to allow capital. Existing governed validation mode remains read-only.
5. Deploy the accounting agent's portfolio snapshot contract and Risk Sizer. The current source has no reconciled cash/NAV/order ledger: its `capital_book.status=BLOCKED` is intentional. **Do not infer NAV from positions, suppress this hold, or substitute a hypothetical model book.** A real reconciliation producer must supply the READY contract before incremental account sizing can unlock.
6. Deploy Katlin and run the full daily research path. Add the separate `eventbridge_schedulers_extra` list entry `justhodl-katlin-permission-refresh`, `rate(15 minutes)`, input `{"mode":"permission_refresh"}`, qualified target alias `live`. Preserve the existing daily schedule and separate weekly backtest configuration.
7. Publish the pages/scripts and validate actual displayed permission against the updated artifacts, including automatic expiry and nested evidence access.

Katlin permission refresh uses conditional S3 `IfMatch`; a concurrent full ranking wins instead of being overwritten. It preserves `research_generated_at` and market `session`. Research older than 36 hours, or a market session older than 96 hours, holds even if permission refresh succeeds. These calendar-hour limits are explicit conservative policy; refreshing a cache never renews research age.

Katlin and Sizer accept either `{"mode":"validate_only"}` or `{"validate_only":true}`. Katlin's full dry run suppresses cache/history/output writes and restores invocation state in `finally`. `{"mode":"permission_refresh","validate_only":true}` validates cached research plus current risk without publishing. Candidate responses expose only `{ok,validation_only,schema_version,status,artifact_size_bytes}`, not account data.

Post-release checks should use exact deployed code/package hashes, qualified alias targets, actual payload times/contracts, displayed caps and blocker states. Passing source tests alone is not live sign-off.
