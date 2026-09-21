"""ops 5861 -- supply run yield (Claude, 2026-09-20). READ-ONLY: the newest factory/curriculum/code/runs/<run_id>.json summaries."""
import json, sys
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
PRI = "justhodl-ai-857687956942"


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("5861_supply_yield") as r:
        r.heading("ops 5861 -- supply run yields")
        keys = sorted((o["Key"], o["LastModified"]) for o in s3.list_objects_v2(Bucket=PRI, Prefix="factory/curriculum/code/runs/").get("Contents", []) if o["Key"].endswith(".json"))
        for k, lm in keys[-3:]:
            d = json.loads(s3.get_object(Bucket=PRI, Key=k)["Body"].read())
            r.log("%s %s judged=%s passed=%s passed_by_family=%s failed_by_family=%s written=%s exists=%s fetch=%s report=%s" % (
                lm.strftime("%m-%d %H:%M"), d.get("run_id"), d.get("rows"), d.get("passed"), json.dumps(d.get("passed_by_family")), json.dumps(d.get("failed_by_family"))[:200], d.get("written"), d.get("exists"), json.dumps(d.get("fetch"))[:200], json.dumps(d.get("report"))[:300]))
        r.ok("done" if keys else "no run summaries yet")


if __name__ == "__main__":
    main()
