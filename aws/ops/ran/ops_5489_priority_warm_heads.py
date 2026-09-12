"""ops_5489 -- newest object per high-value warm prefix from 5488."""
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
PREFS = [
    "data/warm/census-econ/",
    "data/warm/census-us/",
    "data/warm/tic-full/",
    "data/warm/edgar-filings/",
    "data/warm/fiscaldata-full/",
    "data/warm/treasury-auctions/",
    "data/warm/bls-full/",
    "data/warm/real-economy/",
    "data/warm/sec-ftd/",
    "data/warm/dtcc-fails/",
]


def main():
    with report("ops_5489_priority_warm_heads") as R:
        R.heading("ops 5489 priority warm heads")
        s3 = boto3.client("s3", region_name="us-east-1")
        rows = []
        for pref in PREFS:
            token = None
            n = 0
            newest = None
            while True:
                kw = {"Bucket": B, "Prefix": pref, "MaxKeys": 200}
                if token:
                    kw["ContinuationToken"] = token
                resp = s3.list_objects_v2(**kw)
                for obj in resp.get("Contents") or []:
                    n += 1
                    if newest is None or obj["LastModified"] > newest["LastModified"]:
                        newest = obj
                if not resp.get("IsTruncated"):
                    break
                token = resp.get("NextContinuationToken")
                if n > 30000:
                    break
            rows.append({
                "prefix": pref, "n": n,
                "newest_key": newest["Key"] if newest else None,
                "newest_lm": newest["LastModified"].astimezone(timezone.utc).isoformat() if newest else None,
                "newest_bytes": newest["Size"] if newest else None,
            })
            R.ok("%s n=%s %s" % (pref, n, rows[-1]["newest_key"]))
        body = {
            "schema": "priority-warm-heads.v1",
            "source": "ops_5489",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "prefixes": rows,
            "n_total": sum(r["n"] for r in rows),
        }
        s3.put_object(Bucket=B, Key="data/priority-warm-heads.json", Body=json.dumps(body).encode("utf-8"), ContentType="application/json")
        R.ok("wrote data/priority-warm-heads.json n_total=%s" % body["n_total"])


if __name__ == "__main__":
    main()
