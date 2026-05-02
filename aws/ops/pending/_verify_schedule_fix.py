"""
Verify the schedule-aware fix resolved the 5 RED items.

Invokes health-monitor synchronously and checks the result for the
specific 5 items. They should all be 'green' now (it's Saturday).
"""
import json

from ops_report import report
import boto3

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)


TARGETS = [
    "s3:repo-data.json",
    "s3:intelligence-report.json",
    "lambda:justhodl-intelligence",
    "lambda:justhodl-nyfed-dealer-survey",
    "lambda:justhodl-oecd-cli",
]


def main():
    with report("verify_schedule_aware_fix") as r:
        r.heading("Verify schedule-aware fix on 5 RED items")

        r.section("Invoke health-monitor synchronously")
        try:
            resp = lam.invoke(
                FunctionName="justhodl-health-monitor",
                InvocationType="RequestResponse",
                Payload=b'{"source":"verify-schedule-fix"}',
            )
            body = resp["Payload"].read().decode("utf-8")
            data = json.loads(body)
            inner = json.loads(data.get("body", "{}"))
        except Exception as e:
            r.fail(f"  invoke failed: {e}")
            return

        components = inner.get("components", [])
        r.log(f"  total components checked: {len(components)}")
        counts = inner.get("counts", {})
        r.log(f"  status counts: {counts}")

        r.section("Status of the 5 previously-RED items")
        by_id = {c.get("id"): c for c in components}
        all_green = True
        for tid in TARGETS:
            c = by_id.get(tid, {})
            status = c.get("status", "missing")
            reason = c.get("reason") or c.get("note", "")[:60]
            r.log(f"  [{status:8s}] {tid}")
            if reason:
                r.log(f"             reason: {reason[:120]}")
            if status != "green":
                all_green = False

        r.section("Summary")
        if all_green:
            r.ok(f"  ✅ All 5 previously-RED items now GREEN")
        else:
            r.log(f"  Some still not green — see above")

        r.log(f"\n  Total RED across all 63 components: {counts.get('red', 0)}")
        if counts.get("red", 0) > 0:
            r.log(f"  Remaining REDs:")
            for c in components:
                if c.get("status") == "red":
                    r.log(f"    - {c.get('id')}: {c.get('reason', c.get('note',''))[:120]}")


if __name__ == "__main__":
    main()
