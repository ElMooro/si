"""ops_5497 -- invoke positioning after 3217; CFTC fields must survive."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"


def main():
    with report("ops_5497_positioning_cftc_compiler_proof") as R:
        R.heading("ops 5497 compiler CFTC persistence")
        lam = boto3.client("lambda", region_name="us-east-1")
        s3 = boto3.client("s3", region_name="us-east-1")
        resp = lam.invoke(
            FunctionName="justhodl-brief-compiler",
            InvocationType="RequestResponse",
            Payload=json.dumps({"mode": "positioning"}).encode("utf-8"),
        )
        R.ok("invoke %s" % resp.get("StatusCode"))
        brief = json.loads(s3.get_object(Bucket=B, Key="data/positioning-brief.json")["Body"].read())
        f = brief.get("fields") or {}
        inp = (brief.get("inputs") or {}).get("data/cftc-join.json") or {}
        R.ok("status=%s acc/dist/flat=%s/%s/%s cftc live=%s rows=%s required=%s" % (
            brief.get("status"), f.get("accumulating"), f.get("distributing"), f.get("flat"),
            f.get("cftc_n_live"), f.get("cftc_rows"), inp.get("required")))
        if brief.get("status") != "LIVE":
            R.fail("brief not LIVE")
            sys.exit(1)
        if f.get("cftc_n_live") in (None, 0):
            R.fail("compiler dropped CFTC")
            sys.exit(1)
        if f.get("accumulating") != 3200:
            R.warn("breadth changed acc=%s" % f.get("accumulating"))


if __name__ == "__main__":
    main()
