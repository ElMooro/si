"""Re-verify after the followup fix."""
import json, time
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
DASHBOARD_KEY = "_health/dashboard.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("verify_after_2nd_fix") as r:
        r.heading("Re-verify after followup fix")
        try:
            resp = lam.invoke(
                FunctionName="justhodl-health-monitor",
                InvocationType="RequestResponse",
                Payload=b'{"source":"verify-2"}',
            )
            r.ok(f"  invoke status: {resp['StatusCode']}")
            if "FunctionError" in resp:
                r.fail(f"  FunctionError detected")
                return
        except Exception as e:
            r.fail(f"  invoke failed: {e}")
            return
        time.sleep(1)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=DASHBOARD_KEY)
            dash = json.loads(obj["Body"].read())
        except Exception as e:
            r.fail(f"  S3 read failed: {e}")
            return
        counts = dash.get("counts", {})
        r.log(f"  counts: green={counts.get('green',0)} yellow={counts.get('yellow',0)} red={counts.get('red',0)} info={counts.get('info',0)} unknown={counts.get('unknown',0)}")
        red = counts.get("red", 0)
        if red == 0:
            r.ok(f"  ✅ ZERO RED")
        else:
            r.log(f"  Still {red} RED:")
            for c in dash.get("components", []):
                if c.get("status") == "red":
                    r.log(f"    - {c.get('id')}: {c.get('reason', c.get('note',''))[:140]}")


if __name__ == "__main__":
    main()
