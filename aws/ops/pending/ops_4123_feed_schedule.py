"""ops_4123 — arm the families-feed daily schedule (permanent asset)."""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

sch = boto3.client("scheduler", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def main():
    with report("4123_feed_schedule") as rep:
        rep.heading("ops 4123 — families-feed daily schedule")
        arn = lam.get_function_configuration(
            FunctionName="justhodl-families-feed")["FunctionArn"]
        donor = sch.get_schedule(Name="tv-workbench-daily")
        role = donor["Target"]["RoleArn"]
        try:
            sch.create_schedule(
                Name="families-feed-daily",
                ScheduleExpression="cron(15 11 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": arn, "RoleArn": role,
                        "Input": json.dumps({"source": "schedule"})})
            rep.ok("  created cron(15 11) daily")
        except sch.exceptions.ConflictException:
            rep.ok("  already exists")
        st = sch.get_schedule(Name="families-feed-daily")
        rep.kv(state=st.get("State"), expr=st.get("ScheduleExpression"))
        if st.get("State") != "ENABLED":
            rep.fail("schedule not ENABLED")
            sys.exit(1)
        rep.ok("PASS_ALL — feed refreshes daily at 11:15 UTC")


if __name__ == "__main__":
    main()
