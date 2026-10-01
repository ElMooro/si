# Cost monitor budget alignment

The declared monthly alert reference and the source fallback differed from the user's requested operating budget. This change sets both to 150. It preserves the existing daily schedule, anomaly math, notification destinations and strict above-110-percent forecast condition. It is an alert reference, not a spend-enforcement control. The resulting forecast-only alert boundary is above 165.

Baseline: reviewed technical probe 6423, run 36905807442 at source 83c05b2b4dc54d617b8ca121773988e593d7ce0a, verified the live budget matched the prior repository value 300. It also verified the fleet monitor's five declared threshold values matched their configuration. No financial output, application body, metric or log was read. Baseline report is retained in commit 94874761a.

Rollback: restore the two changed budget literals to 300 through a reviewed source/config commit and the existing targeted cost-anomaly deployment lane. Preserve every other configuration member and environment value. Re-run probe 6423 against that source to verify the budget match; check the exact code receipt. No schedule, IAM, security or secret change is part of this repair.

Validation uses only invented Cost Explorer results and fake transports. It checks default/config agreement, explicit environment precedence, equality of all non-budget-derived financial calculations, the unchanged strict alert boundary, the output contract and the original daily rule. The existing handler is never manually invoked. Its existing financial output/logging behavior is not expanded or qualified by this patch, and must not be used as a private billing retrieval path.

The fleet-error five-minute rule remains unchanged pending a separate cadence decision. GetMetricData batching is not assumed to reduce billed metric volume. No reduction in actual spending is claimed by this alert-reference repair.
