"""ops_5498 -- surface data/warm/alfred/* as data/alfred-vintages.json. Append-only."""
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
PREFIX = "data/warm/alfred/"


def _load(s3, key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if key.endswith(".gz"):
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops_5498_alfred_hot_index") as R:
        R.heading("ops 5498 ALFRED vintage cube index")
        s3 = boto3.client("s3", region_name="us-east-1")
        keys = []
        token = None
        while True:
            kw = {"Bucket": B, "Prefix": PREFIX, "MaxKeys": 200}
            if token:
                kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw)
            for o in page.get("Contents") or []:
                keys.append({"key": o["Key"], "lm": o["LastModified"].astimezone(timezone.utc).isoformat(), "bytes": o["Size"]})
            if not page.get("IsTruncated"):
                break
            token = page.get("NextContinuationToken")
        R.ok("warm objects %s" % len(keys))
        series = []
        for row in keys[:80]:
            if not (row["key"].endswith(".json") or row["key"].endswith(".json.gz")):
                continue
            try:
                doc = _load(s3, row["key"])
            except Exception as e:
                R.warn("read %s %s" % (row["key"], e))
                continue
            series.append({
                "key": row["key"],
                "lm": row["lm"],
                "series_id": doc.get("series_id") or row["key"].split("/")[-1].split(".")[0],
                "n_vintages_available": doc.get("n_vintages_available"),
                "n_vintages_banked": doc.get("n_vintages_banked") or (len(doc.get("vintages") or []) if isinstance(doc.get("vintages"), list) else None),
                "first_vintage": doc.get("first_vintage"),
                "last_vintage": doc.get("last_vintage"),
                "source": doc.get("source") or "alfred",
            })
        hot = {
            "schema": "alfred-vintages.v1",
            "source": "ops_5498",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "prefix": PREFIX,
            "n_objects": len(keys),
            "n_indexed": len(series),
            "series": series,
        }
        s3.put_object(Bucket=B, Key="data/alfred-vintages.json", Body=json.dumps(hot).encode("utf-8"), ContentType="application/json")
        R.ok("indexed %s series -> data/alfred-vintages.json" % len(series))
        if not keys:
            R.warn("warm/alfred empty -- 5482 may not have landed objects")


if __name__ == "__main__":
    main()
