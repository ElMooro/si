"""Inspect calibration weights, outcomes table, signal sources before building accuracy.html."""
import json
import boto3
from collections import Counter
from ops_report import report

ssm = boto3.client("ssm", region_name="us-east-1")
ddb = boto3.client("dynamodb", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_calibration_state") as r:
        r.heading("Inspect calibration state (weights + outcomes + signals)")

        # SSM weights
        for path in ["/justhodl/calibration/weights", "/justhodl/calibration/accuracy"]:
            try:
                v = ssm.get_parameter(Name=path)["Parameter"]["Value"]
                r.ok(f"  ✓ {path}  len={len(v)}")
                try:
                    parsed = json.loads(v)
                    r.log(f"    parsed keys: {list(parsed.keys())[:10]}")
                    if isinstance(parsed, dict):
                        for k, val in list(parsed.items())[:5]:
                            r.log(f"      {k}: {str(val)[:80]}")
                except Exception:
                    r.log(f"    raw: {v[:200]}")
            except Exception as e:
                r.log(f"  ✗ {path}: {e}")

        # DDB tables
        for tbl in ["justhodl-signals", "justhodl-outcomes", "justhodl-feedback"]:
            try:
                d = ddb.describe_table(TableName=tbl)["Table"]
                r.ok(f"  ✓ {tbl}: items≈{d.get('ItemCount', '?')} sizeBytes={d.get('TableSizeBytes', '?')}")
            except Exception as e:
                r.log(f"  ✗ {tbl}: {e}")

        # Sample outcomes (recent labels)
        try:
            resp = ddb.scan(TableName="justhodl-outcomes", Limit=20)
            items = resp.get("Items", [])
            r.log(f"  outcomes sample (n={len(items)}):")
            types = Counter()
            for it in items:
                t = it.get("signal_type", {}).get("S", "?")
                types[t] += 1
            for t, n in types.most_common(10):
                r.log(f"    {t:30s} n={n}")
        except Exception as e:
            r.log(f"  ✗ outcomes scan: {e}")

        # signal-portfolio-state (paper portfolio from #8)
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="portfolio/signal-portfolio-state.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  paper portfolio: keys={list(d.keys())[:10]}")
            if "by_signal_type" in d:
                for st, info in list(d["by_signal_type"].items())[:5]:
                    r.log(f"    {st}: {str(info)[:120]}")
        except Exception as e:
            r.log(f"  ✗ paper portfolio: {e}")

        # ab-test results
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ab-test-results.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  ab-test: keys={list(d.keys())[:10]}")
            r.log(f"    {json.dumps(d, default=str)[:400]}")
        except Exception as e:
            r.log(f"  ✗ ab-test: {e}")


if __name__ == "__main__":
    main()
