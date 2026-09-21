"""ops 5862 -- read the supply fetch log tail from S3 (Claude, 2026-09-21). READ-ONLY."""
import sys
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
PRI = "justhodl-ai-857687956942"


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("5862_fetch_log") as r:
        r.heading("ops 5862 -- newest supply fetch log tails")
        objs = sorted((o["LastModified"], o["Key"]) for o in s3.list_objects_v2(Bucket=PRI, Prefix="factory/curriculum/code/runs/").get("Contents", []) if o["Key"].endswith("-fetch-tail.log"))
        for lm, key in objs[-2:]:
            text = s3.get_object(Bucket=PRI, Key=key)["Body"].read().decode("utf-8", "replace")
            r.log("== %s (%s)" % (key.rsplit("/", 1)[-1], lm.strftime("%m-%d %H:%M")))
            for line in text.strip().splitlines()[-30:]:
                r.log("   " + line[:300])
        r.ok("done" if objs else "no fetch logs on S3")


if __name__ == "__main__":
    main()
