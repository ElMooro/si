"""Retire the unused justhodl-skew-engine Lambda — Polygon plan doesn't have options data."""
import boto3
from ops_report import report

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")


def main():
    with report("retire_skew_engine") as r:
        r.heading("Retire justhodl-skew-engine + its rule")
        try:
            events.remove_targets(Rule="justhodl-skew-engine-hourly", Ids=["1"])
            r.ok("  ✓ removed targets")
        except Exception as e:
            r.log(f"  remove_targets: {e}")
        try:
            events.delete_rule(Name="justhodl-skew-engine-hourly")
            r.ok("  ✓ deleted rule")
        except Exception as e:
            r.log(f"  delete_rule: {e}")
        try:
            lam.delete_function(FunctionName="justhodl-skew-engine")
            r.ok("  ✓ deleted Lambda")
        except Exception as e:
            r.log(f"  delete_function: {e}")


if __name__ == "__main__":
    main()
