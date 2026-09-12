"""ops_5440 -- Claude-style receipt: invoke justhodl-brief-compiler mode=positioning."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
FN = "justhodl-brief-compiler"
KEY = "data/positioning-brief.json"


def main():
    with report("ops_5440_compiler_lambda_positioning_proof") as R:
        R.heading("ops 5440 live compiler positioning")
        lam = boto3.client("lambda", region_name="us-east-1")
        s3 = boto3.client("s3", region_name="us-east-1")
        cfg = lam.get_function(FunctionName=FN)
        R.ok("code_sha256=%s last_modified=%s" % (
            cfg["Configuration"].get("CodeSha256"),
            cfg["Configuration"].get("LastModified")))
        inv = lam.invoke(
            FunctionName=FN,
            InvocationType="RequestResponse",
            Payload=json.dumps({"mode": "positioning"}).encode("utf-8"),
        )
        payload = json.loads(inv["Payload"].read() or b"{}")
        R.ok("invoke: %s" % json.dumps(payload)[:400])
        obj = s3.get_object(Bucket=B, Key=KEY)
        doc = json.loads(obj["Body"].read())
        f = doc.get("fields") or {}
        R.ok("s3 source=%s status=%s acc=%s dist=%s n=%s pct=%s" % (
            doc.get("source"), doc.get("status"),
            f.get("accumulating"), f.get("distributing"),
            f.get("n_with_inst_trans"), f.get("breadth_pct")))
        if (f.get("n_with_inst_trans") or 0) < 500:
            R.warn("live zip still on the rows-parse bug -- runner 5439 remains the good brief")
        if f.get("accumulating") == 100 and f.get("distributing") == 100:
            R.fail("capped 100/100 came back")
            sys.exit(1)


if __name__ == "__main__":
    main()
