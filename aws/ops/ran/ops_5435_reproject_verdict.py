"""ops_5435 -- refresh verdict from live jh-fusion after FLOW ingest."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
from brief_contract import project_verdict  # noqa: E402

B = "justhodl-dashboard-live"


def main():
    with report("ops_5435_reproject_verdict") as R:
        R.heading("ops 5435 reproject verdict")
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket=B, Key="data/jh-fusion.json")
        fusion = json.loads(obj["Body"].read())
        verdict = project_verdict(fusion)
        s3.put_object(
            Bucket=B, Key="data/verdict.json",
            Body=json.dumps(verdict, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("fusion %s coverage=%s score=%s missing=%s" % (
            fusion.get("run_id"), verdict.get("coverage"),
            verdict.get("score"), verdict.get("missing_families")))


if __name__ == "__main__":
    main()
