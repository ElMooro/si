"""ops_5499 -- ALFRED hot index from the real 5482 prefix."""
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
PREFIXES = ["data/warm/inst-public/alfred/", "data/warm/alfred/", "data/alfred/"]


def _load(s3, key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if key.endswith(".gz"):
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops_5499_alfred_hot_index") as R:
        R.heading("ops 5499 ALFRED real prefix")
        s3 = boto3.client("s3", region_name="us-east-1")
        series = []
        found = []
        for prefix in PREFIXES:
            page = s3.list_objects_v2(Bucket=B, Prefix=prefix, MaxKeys=100)
            objs = page.get("Contents") or []
            R.ok("%s n=%s" % (prefix, len(objs)))
            for o in objs:
                key = o["Key"]
                if not (key.endswith(".json") or key.endswith(".json.gz")):
                    continue
                found.append(key)
                try:
                    doc = _load(s3, key)
                except Exception as e:
                    R.warn("read %s %s" % (key, e))
                    continue
                vint = doc.get("vintages") if isinstance(doc.get("vintages"), list) else []
                series.append({
                    "key": key,
                    "lm": o["LastModified"].astimezone(timezone.utc).isoformat(),
                    "series_id": doc.get("series_id") or key.split("/")[-1].split(".")[0],
                    "n_vintages_available": doc.get("n_vintages_available"),
                    "n_vintages_banked": doc.get("n_vintages_banked") or len(vint),
                    "first_vintage": (vint[0].get("vintage_date") if vint and isinstance(vint[0], dict) else None),
                    "last_vintage": (vint[-1].get("vintage_date") if vint and isinstance(vint[-1], dict) else None),
                    "source": doc.get("source") or "alfred",
                })
        hot = {
            "schema": "alfred-vintages.v1",
            "source": "ops_5499",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "prefixes": PREFIXES,
            "n_objects": len(found),
            "n_indexed": len(series),
            "series": series,
        }
        s3.put_object(Bucket=B, Key="data/alfred-vintages.json", Body=json.dumps(hot).encode("utf-8"), ContentType="application/json")
        R.ok("indexed %s -> data/alfred-vintages.json" % len(series))
        if not series:
            R.fail("still no ALFRED objects")
            sys.exit(1)


if __name__ == "__main__":
    main()
