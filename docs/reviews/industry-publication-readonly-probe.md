# Industry-case publication probe 6412 (draft, not dispatched)

The ordinary public inspector still showed the August 18 v1.1.0 industry-case packet after the declared October 1 13:00 cadence. Repository config uses uppercase `Schedule`, which the lowercase deployment schedule guard does not check; the historical September 2 inventory recorded no bindings. Neither historical evidence nor a deployment receipt proves today's invocation or binding state. This staged script collects only bounded current metadata to distinguish those possibilities. It does not repair or invoke anything.

## Fixed read scope and bounds

- Region us-east-1; exact function `justhodl-industry-case`.
- One S3 HeadObject, bucket `justhodl-dashboard-live`, key `data/industry-case.json`. Only modification time, length, ETag and encoding are projected; no object body, custom metadata, semantic freshness or content claims.
- One CloudWatch GetMetricData with four AWS/Lambda FunctionName series: Invocations/Errors/Throttles Sum and Duration Maximum. Period 60 seconds, MaxDatapoints 3000, no pagination followup. Start inclusive 2026-10-01T12:55:00Z; end exclusive is the dispatch clock floored to a minute. Other dates or an empty window fail closed. Reported-point sums/maxima do not fill missing minutes or attribute execution to a trigger or code version. Empty results remain null. Partial responses and read errors cannot establish complete evidence.
- Default-bus ListRuleNamesByTarget for the unqualified, `$LATEST`, and `live` function ARNs: at most two pages of 100 each. At most five deduplicated rules get DescribeRule and one ListTargetsByRule page of 100. A missing target after lookup is explicitly unconfirmed. Other qualifiers, buses and indirect triggers remain unexamined.
- Regional Scheduler ListSchedules: at most ten pages of 100, no group restriction. Filter exact direct function ARNs, including qualified variants, before GetSchedule for at most five deduplicated name/group pairs. Only name/group/state/expression/timezone/qualifier are emitted. Universal SDK targets and indirect triggers remain unexamined.
- Maximum 33 SDK calls: 1 + 1 + 6 + 5 + 5 + 10 + 5. Retries disabled (`total_max_attempts=1`), connect timeout 3 seconds and read timeout 5 seconds. New reads stop after a 90-second monotonic deadline; an already started request may finish after that deadline.
- Every inventory has `inventory_complete=false`: this bounded direct-target probe cannot certify global absence. Page/detail limits, repeated tokens, malformed responses, races and AWS failures are explicit incomplete evidence. Exception strings and API error bodies are never emitted.

The script does not read Lambda configuration, environment, code, logs, receipts, object bodies, private Brain, or provider APIs. It makes no AWS writes, native invocation, schedule mutation or provider call. The existing direct runner writes its normal repository report; that report mechanism is not an AWS mutation.

## Runner and report contract

Only `Run ops script (direct)` under workflow_dispatch in `ElMooro/si` passes the runtime guard. The script lives in `aws/ops/staged/`, outside the pending serial queue. Number 6412 is reserved in SESSION_CLAIMS. No workflow changes are included. Independent exact-head review and explicit parent release must precede any merge or dispatch.

A future reviewed direct dispatch would select `staged/ops_6412_industry_publication_readonly.py`. It emits the normal `aws/ops/reports/latest/ops_6412_industry_publication_readonly.md` report with GITHUB_SHA and probe-source SHA-256. Failed or incomplete reads exit nonzero after preserving sanitized results. A successful bounded read still does not assert the existence/absence of all triggers, completed publication, or observation freshness.

## Offline validation

`python3 -m unittest discover -s tests/ops -p test_industry_publication_readonly.py -v`

Invented stubs exercise exact output/function scope; 33-call worst case; pagination/detail limits and repeated tokens; absent/sparse/partial metrics; measured zero; malformed data and timestamps; changed bindings; every allowlisted AWS method failing; deadline/call exhaustion; date/local-run guards; and canaries in inputs, roles, descriptions, metadata and errors that must not reach the report. No local AWS clients are constructed during tests.
