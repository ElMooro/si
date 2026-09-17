"""ops 5583 — invoke justhodl-flow-lookthrough after the 2.5.0 harvest-write fix.

f658c6d left the harvest dict dead after return inside _by_ticker_slice, so Event
202 from 5582 never rewrote S3. This op fires Event and polls for version 2.5.0
plus by_ticker.AAPL.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION, BUCKET, KEY = "us-east-1", "justhodl-dashboard-live", "data/flow-lookthrough.json"
FN = "justhodl-flow-lookthrough"


def main() -> int:
    lam = boto3.client(
        "lambda", region_name=REGION,
        config=Config(read_timeout=180, connect_timeout=10, retries={"max_attempts": 0}),
    )
    s3 = boto3.client("s3", region_name=REGION)
    with report("ops_5583_lookthrough_invoke") as R:
        R.heading("ops 5583 — flow-lookthrough 2.5.0 harvest")
        r = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        R.ok("Event status=%s" % r.get("StatusCode"))
        ver = nbt = aapl = None
        for i in range(24):
            time.sleep(20)
            try:
                d = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
            except ClientError:
                R.log("poll %d miss" % i)
                continue
            ver = d.get("version")
            bt = d.get("by_ticker") or {}
            nbt = len(bt) if isinstance(bt, dict) else 0
            aapl = "AAPL" in bt if isinstance(bt, dict) else False
            R.log("poll %d ver=%s by_ticker=%s AAPL=%s gen=%s" % (
                i, ver, nbt, aapl, (d.get("generated_at") or "")[:19]))
            if ver == "2.5.0" and nbt:
                break
        if ver != "2.5.0" or not nbt:
            R.fail("harvest still ver=%s by_ticker=%s" % (ver, nbt))
            sys.exit(1)
        R.ok("flow-lookthrough 2.5.0 by_ticker_n=%s AAPL=%s" % (nbt, aapl))
    return 0


if __name__ == "__main__":
    sys.exit(main())
