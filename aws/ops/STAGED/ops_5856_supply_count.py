"""ops 5856 -- coding supply after the APPS widening (Claude, 2026-09-20). READ-ONLY: verified rows by family, newest run ids."""
import json, sys
from collections import Counter
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
PRI = "justhodl-ai-857687956942"; PREFIX = "factory/curriculum/code/verified/"


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("5856_supply_count") as r:
        r.heading("ops 5856 -- verified coding supply by family")
        fam, runs, n = Counter(), Counter(), 0
        token = None
        while True:
            kw = {"Bucket": PRI, "Prefix": PREFIX, "MaxKeys": 1000}
            if token: kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw)
            for o in page.get("Contents", []):
                n += 1
                if n <= 4000:
                    try:
                        d = json.loads(s3.get_object(Bucket=PRI, Key=o["Key"])["Body"].read())
                        fam[d.get("family")] += 1; runs[str(d.get("run_id"))] += 1
                    except Exception:  # noqa: BLE001
                        fam["unreadable"] += 1
            token = page.get("NextContinuationToken")
            if not token: break
        r.kv(verified_rows=n, families=json.dumps(dict(fam)), newest_runs=json.dumps(runs.most_common(4)))
        r.ok("counted")


if __name__ == "__main__":
    main()
