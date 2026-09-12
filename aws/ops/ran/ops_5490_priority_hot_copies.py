"""ops_5490 -- copy small priority warm heads to data/* hot JSON."""
from __future__ import annotations

import gzip
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
COPIES = [
    ("data/warm/real-economy/_summary.json", "data/real-economy-summary.json"),
    ("data/warm/treasury-auctions/composite-history.json", "data/treasury-auctions-composite.json"),
    ("data/warm/dtcc-fails/agency.json", "data/dtcc-fails-agency.json"),
    ("data/warm/tic-full/_state/state.json", "data/tic-state.json"),
    ("data/warm/fiscaldata-full/_state/state.json", "data/fiscaldata-state.json"),
    ("data/warm/census-us/_state/state.json", "data/census-us-state.json"),
    ("data/warm/bls-full/_state/state.json", "data/bls-full-state.json"),
]


def _load(s3, key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if key.endswith(".gz"):
        raw = gzip.decompress(raw)
    return raw, json.loads(raw)


def main():
    with report("ops_5490_priority_hot_copies") as R:
        R.heading("ops 5490 hot copies")
        s3 = boto3.client("s3", region_name="us-east-1")
        out = []
        for src, dest in COPIES:
            try:
                raw, doc = _load(s3, src)
                s3.put_object(Bucket=B, Key=dest, Body=raw, ContentType="application/json")
                meta = {"src": src, "dest": dest, "bytes": len(raw), "status": "LIVE"}
                if isinstance(doc, dict):
                    meta["top_keys"] = list(doc)[:20]
                out.append(meta)
                R.ok("%s -> %s bytes=%s" % (src, dest, len(raw)))
            except Exception as e:
                out.append({"src": src, "dest": dest, "status": "FAIL", "error": str(e)[:180]})
                R.warn("%s %s" % (src, e))
        body = {
            "schema": "priority-hot-copies.v1",
            "source": "ops_5490",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "copies": out,
        }
        s3.put_object(Bucket=B, Key="data/priority-hot-copies.json", Body=json.dumps(body).encode("utf-8"), ContentType="application/json")
        live = sum(1 for x in out if x.get("status") == "LIVE")
        R.ok("live copies=%s" % live)
        if live < 4:
            R.fail("too few copies")
            sys.exit(1)


if __name__ == "__main__":
    main()
