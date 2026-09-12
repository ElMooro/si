"""ops_5432 -- refresh verdict projection from live jh-fusion. S3 only."""
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
    with report("ops_5432_reproject_verdict") as R:
        R.heading("ops 5432 reproject verdict from current fusion")
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket=B, Key="data/jh-fusion.json")
        fusion = json.loads(obj["Body"].read())
        lm = obj["LastModified"].isoformat()
        verdict = project_verdict(fusion)
        s3.put_object(
            Bucket=B, Key="data/verdict.json",
            Body=json.dumps(verdict, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("fusion run=%s generated=%s" % (fusion.get("run_id"), fusion.get("generated_at")))
        R.ok("verdict coverage=%s score=%s missing=%s writer=%s as_of=%s" % (
            verdict.get("coverage"), verdict.get("score"),
            verdict.get("missing_families"), verdict.get("writer"),
            verdict.get("as_of")))
        R.ok("fusion LastModified %s" % lm)


if __name__ == "__main__":
    main()
