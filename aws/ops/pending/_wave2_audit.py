"""Final audit of Wave 2: 3 new Lambdas, 2 new pages, 1 new alert system."""
import json
import boto3
from ops_report import report

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("wave2_audit") as r:
        r.heading("Wave 2 final audit")

        # Lambdas
        r.heading("Lambdas live")
        wave2_lambdas = [
            "justhodl-calibration-snapshot",
            "justhodl-sector-rotation",
            "justhodl-alert-router",
        ]
        for n in wave2_lambdas:
            try:
                cfg = lam.get_function_configuration(FunctionName=n)
                r.ok(f"  ✓ {n:38s} state={cfg['State']:8s} mem={cfg['MemorySize']:>4}MB  timeout={cfg['Timeout']}s")
            except Exception as e:
                r.log(f"  ✗ {n}: {e}")

        # Schedules
        r.heading("Schedules wired")
        for rule in ["justhodl-calibration-snapshot-30min", "justhodl-sector-rotation-6h", "justhodl-alert-router-30min"]:
            try:
                d = events.describe_rule(Name=rule)
                r.ok(f"  ✓ {rule:42s} {d.get('ScheduleExpression', '?'):20s} state={d.get('State')}")
            except Exception as e:
                r.log(f"  ✗ {rule}: {e}")

        # S3 outputs
        r.heading("S3 outputs producing data")
        wave2_keys = [
            "data/calibration-snapshot.json",
            "data/sector-rotation.json",
            "data/alert-history.json",
            "alerts-state.json",
        ]
        for key in wave2_keys:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
                size = obj["ContentLength"]
                last = obj["LastModified"]
                r.ok(f"  ✓ {key:42s} {size:>8,}b  modified={last.isoformat()}")
            except Exception as e:
                r.log(f"  ✗ {key}: {e}")

        # Frontend pages
        r.heading("Frontend pages live (via GH Pages)")
        import urllib.request
        for p in ["accuracy.html", "sectors.html"]:
            try:
                req = urllib.request.Request(f"https://justhodl.ai/{p}", headers={"User-Agent": "audit/1.0"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    body_size = len(resp.read())
                    r.ok(f"  ✓ {p:30s} status={resp.status}  size={body_size:,}b")
            except Exception as e:
                r.log(f"  ✗ {p}: {e}")

        # Alert summary
        r.heading("Alert router today")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/alert-history.json")
            d = json.loads(obj["Body"].read())
            alerts = d.get("alerts", [])
            r.log(f"  total alerts in history: {len(alerts)}")
            r.log(f"  last run: {d.get('last_run')}")
            r.log(f"  by severity:")
            sev_counts = {}
            for a in alerts:
                sev = a.get("severity", "?")
                sev_counts[sev] = sev_counts.get(sev, 0) + 1
            for sev, n in sorted(sev_counts.items()):
                r.log(f"    {sev}: {n}")
            r.log(f"  by category:")
            cat_counts = {}
            for a in alerts:
                c = a.get("category", "?")
                cat_counts[c] = cat_counts.get(c, 0) + 1
            for c, n in sorted(cat_counts.items()):
                r.log(f"    {c}: {n}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
