"""ops_5415 -- Finviz insider surface from existing warehouse.

No Elite HTTP. Reads data/finviz-signals.json, writes the insider
screens to data/finviz-insider.json so data.html / ticker can bind.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"
SRC = "data/finviz-signals.json"
HINTS = ("insider", "it_latest", "buy", "sale", "cluster")


def main():
    with report("ops_5415_finviz_insider_split") as R:
        R.heading("ops 5415 -- Finviz insider split")
        s3 = boto3.client("s3", region_name=REGION)
        raw = json.loads(s3.get_object(Bucket=B, Key=SRC)["Body"].read())
        signals = raw.get("signals") or {}
        counts = raw.get("counts") or {}
        picked = {}
        for k, v in signals.items():
            lk = str(k).lower()
            if any(h in lk for h in HINTS):
                if isinstance(v, list):
                    picked[k] = v[:80]
                else:
                    picked[k] = v
        R.ok("signal keys=%s insider_keys=%s" % (list(signals)[:20], list(picked)))
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source": "ops_5415",
            "from": SRC,
            "src_generated_at": raw.get("generated_at"),
            "n_screens": len(picked),
            "counts_subset": {k: counts.get(k) for k in picked},
            "status": "LIVE" if picked else "DATA_HOLD",
            "screens": picked,
        }
        s3.put_object(
            Bucket=B,
            Key="data/finviz-insider.json",
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/finviz-insider.json")["Body"].read())
        if back.get("schema_version") != 1:
            R.fail("read-back")
            sys.exit(1)
        R.ok("GREEN -- insider screens=%s status=%s" % (back.get("n_screens"), back.get("status")))


if __name__ == "__main__":
    main()
