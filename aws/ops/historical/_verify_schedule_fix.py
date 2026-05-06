"""
Verify the schedule-aware fix resolved the 5 RED items.

Invokes health-monitor synchronously to trigger a fresh check, then
reads the full dashboard.json from S3 (Lambda's response is truncated
to 500 chars by design).
"""
import json
import time

from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
DASHBOARD_KEY = "_health/dashboard.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


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

        r.section("Invoke health-monitor synchronously to trigger fresh check")
        try:
            resp = lam.invoke(
                FunctionName="justhodl-health-monitor",
                InvocationType="RequestResponse",
                Payload=b'{"source":"verify-schedule-fix"}',
            )
            r.ok(f"  invocation status: {resp['StatusCode']}")
            if "FunctionError" in resp:
                body = resp["Payload"].read().decode("utf-8")
                r.fail(f"  FunctionError: {body[:300]}")
                return
        except Exception as e:
            r.fail(f"  invoke failed: {e}")
            return

        # Lambda finishes writing dashboard.json before returning, so we can read immediately
        time.sleep(1)

        r.section("Read full dashboard.json from S3")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=DASHBOARD_KEY)
            dashboard = json.loads(obj["Body"].read())
        except Exception as e:
            r.fail(f"  read S3 failed: {e}")
            return

        components = dashboard.get("components", [])
        counts = dashboard.get("counts", {})
        r.log(f"  total components: {len(components)}")
        r.log(f"  status counts: green={counts.get('green',0)} yellow={counts.get('yellow',0)} red={counts.get('red',0)} info={counts.get('info',0)} unknown={counts.get('unknown',0)}")

        r.section("Status of the 5 previously-RED items")
        by_id = {c.get("id"): c for c in components}
        all_green = True
        for tid in TARGETS:
            c = by_id.get(tid, {})
            status = c.get("status", "missing")
            reason = c.get("reason") or c.get("note", "")[:80]
            r.log(f"  [{status:8s}] {tid}")
            if reason:
                r.log(f"             {reason[:140]}")
            if status not in ("green", "info"):
                all_green = False

        r.section("Summary")
        if all_green:
            r.ok(f"  ✅ All 5 previously-RED items now GREEN/INFO")
        else:
            r.log(f"  Some still not green — see above")

        red_count = counts.get("red", 0)
        r.log(f"\n  Total RED across all {len(components)} components: {red_count}")
        if red_count > 0:
            r.log(f"  Remaining REDs:")
            for c in components:
                if c.get("status") == "red":
                    r.log(f"    - {c.get('id')}: {c.get('reason', c.get('note',''))[:140]}")


if __name__ == "__main__":
    main()

