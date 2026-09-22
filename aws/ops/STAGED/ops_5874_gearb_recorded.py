"""ops 5874 -- Gear B since the wrapper fix (Claude, 2026-09-22). READ-ONLY: jobs since 2026-09-21 18:00 + last_tick.json."""
import json, sys
from datetime import datetime, timezone
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
PRI = "justhodl-ai-857687956942"


def main():
    sm = boto3.client("sagemaker", region_name="us-east-1"); s3 = boto3.client("s3", region_name="us-east-1")
    with report("5874_gearb_recorded") as r:
        r.heading("ops 5874 -- Gear B since the doctrine-wrapper fix")
        jobs = sm.list_training_jobs(NameContains="jh-", CreationTimeAfter=datetime(2026, 9, 21, 18, 0, tzinfo=timezone.utc), MaxResults=20, SortBy="CreationTime", SortOrder="Descending").get("TrainingJobSummaries", [])
        for j in jobs:
            r.log("%s %s %s" % (j["CreationTime"].strftime("%m-%d %H:%M"), j["TrainingJobName"], j["TrainingJobStatus"]))
        if not jobs:
            r.warn("no jobs since 09-21 18:00")
        try:
            last = json.loads(s3.get_object(Bucket=PRI, Key="factory/gearb/last_tick.json")["Body"].read())
            r.log("last_tick: " + json.dumps(last, default=str)[:1200])
        except Exception as e:  # noqa: BLE001
            r.warn("last_tick.json: %s" % str(e)[:120])
        r.ok("done")


if __name__ == "__main__":
    main()
