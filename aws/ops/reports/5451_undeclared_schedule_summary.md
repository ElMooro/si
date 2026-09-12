# Undeclared schedules — one-page breakdown

Op **5451** · inspected **2026-09-12T16:19:13.227816+00:00** · inventory only; **zero schedule mutations**.

The existing drift object was last modified **2026-09-12T14:54:32+00:00** and contains **303** total findings. This report covers all **226 undeclared schedule names**; the remaining drift classes are outside this inventory.

| Breakdown | Count |
| --- | ---: |
| Transport: events | 112 |
| Transport: scheduler | 114 |
| State: DISABLED | 2 |
| State: ENABLED | 224 |
| Distinct Lambda targets | 185 |
| Lambda targets with an observed invocation in 48h | 177 |

**Complete name + target ARN + invocation-age inventory:** [5451_undeclared_schedules.csv](5451_undeclared_schedules.csv). Every undeclared name has one CSV row; multiple target ARNs remain listed in that row. [Structured evidence](5451_catalyst_drift_inventory.json) preserves each target's timestamp and measurement basis.

Invocation ages use the latest nonzero **AWS/Lambda Invocations** five-minute bucket in the preceding 48 hours. They are **function-wide**, including other triggers, not evidence that this particular schedule fired. No observation means no metric in that window, not that a function has never run. Non-Lambda targets are labeled separately.

**Plan:**

1. Match each listed name and target to its producer's current output contract and intended owner. Undeclared does not by itself mean broken or obsolete.
2. Review enabled entries and shared targets for intended payloads, cadence and duplicate triggers; keep input bodies private and preserve current wiring during review.
3. Propose exact manifest additions or retirement candidates with output evidence and rollback. No disable, delete, creation, reattachment or enforcement is authorized by this inventory.

No CATALYST adapter or score was written. CATALYST warehouse search evidence appears in the structured report. The six verified compiler schedules and the other residual drift classes were left alone.
