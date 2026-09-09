# Dedicated metric schedules

Ops 5281 confirmed both declared legacy EventBridge rule names were absent. Ops 5282 paginated all Scheduler configurations and checked EventBridge targets for both function ARNs and aliases: neither producer had an active schedule. No existing cadence or target is replaced.

The two source configurations now declare distinct hourly Scheduler identities, UTC, empty event input, and the existing justhodl-scheduler-role. The deployment helper refuses unrelated target replacement and verifies the complete readback. The final release observer requires the declared expression, enabled state, matching function and exact input. Legacy shared-rule collision tests remain in temporary legacy fixtures.

Both regular metric handlers refresh public metrics and descriptive LLM analysis. They contain no order or notification dispatch. Their default refresh is included in the reviewed quiet publication allowlist after exact package/configuration verification. Anonymous HTTP writes and paid refresh routes remain service-authenticated.

Failed analysis publishes an owned macro-analysis.v1 unavailable snapshot with the exact input artifact and UTC collection time. It does not re-date old narrative or report raw provider errors. All successful analysis remains explicitly uncalibrated descriptive output with execution_eligible=false.

Local validation: 246 deployment checks, 12 candidate shell scenarios, and 12 Katlin capital-permission fixtures passed. Live schedule readback remains contingent on the final ops5231 receipt.
