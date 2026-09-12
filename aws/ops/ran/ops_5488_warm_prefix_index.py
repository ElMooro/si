"""ops_5488 -- one-level index of data/warm/ so we stop guessing prefix names."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"


def main():
    with report("ops_5488_warm_prefix_index") as R:
        R.heading("ops 5488 warm prefix index")
        s3 = boto3.client("s3", region_name="us-east-1")
        token = None
        prefixes = []
        while True:
            kw = {"Bucket": B, "Prefix": "data/warm/", "Delimiter": "/", "MaxKeys": 200}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for p in resp.get("CommonPrefixes") or []:
                pref = p.get("Prefix")
                inner = s3.list_objects_v2(Bucket=B, Prefix=pref, MaxKeys=5)
                n = inner.get("KeyCount") or 0
                truncated = bool(inner.get("IsTruncated"))
                prefixes.append({"prefix": pref, "sample_n": n, "truncated": truncated})
                R.ok("%s sample_n=%s more=%s" % (pref, n, truncated))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        body = {
            "schema": "warm-prefix-index.v1",
            "source": "ops_5488",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "n_prefixes": len(prefixes),
            "prefixes": prefixes,
        }
        s3.put_object(Bucket=B, Key="data/warm-prefix-index.json", Body=json.dumps(body).encode("utf-8"), ContentType="application/json")
        R.ok("wrote data/warm-prefix-index.json n=%s" % len(prefixes))
        if not prefixes:
            R.fail("no warm prefixes")
            sys.exit(1)


if __name__ == "__main__":
    main()
