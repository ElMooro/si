"""ops_5485 -- read CFTC warm objects into data/cftc-join.json."""
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
PREF = "data/warm/cftc/"


def _load(s3, key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if key.endswith(".gz"):
        raw = gzip.decompress(raw)
    try:
        return json.loads(raw)
    except Exception:
        return {"_raw_bytes": len(raw)}


def main():
    with report("ops_5485_cftc_join") as R:
        R.heading("ops 5485 CFTC join")
        s3 = boto3.client("s3", region_name="us-east-1")
        resp = s3.list_objects_v2(Bucket=B, Prefix=PREF)
        files = []
        for obj in resp.get("Contents") or []:
            key = obj["Key"]
            if key.endswith("/"):
                continue
            try:
                doc = _load(s3, key)
            except Exception as e:
                files.append({"key": key, "status": "FAIL", "error": str(e)[:160]})
                R.warn("%s %s" % (key, e))
                continue
            meta = {"key": key, "status": "LIVE", "lm": obj["LastModified"].astimezone(timezone.utc).isoformat()}
            if isinstance(doc, dict):
                meta["keys"] = list(doc)[:24]
                for cand in ("as_of", "date", "report_date", "week_ending"):
                    if doc.get(cand):
                        meta["as_of"] = doc.get(cand)
                        break
                if isinstance(doc.get("rows"), list):
                    meta["n"] = len(doc["rows"])
                elif isinstance(doc.get("contracts"), list):
                    meta["n"] = len(doc["contracts"])
            elif isinstance(doc, list):
                meta["n"] = len(doc)
            files.append(meta)
            R.ok("%s n=%s" % (key, meta.get("n")))
        body = {
            "schema": "cftc-join.v1",
            "source": "ops_5485",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "n_files": len(files),
            "files": files,
        }
        s3.put_object(Bucket=B, Key="data/cftc-join.json", Body=json.dumps(body, default=str).encode("utf-8"), ContentType="application/json")
        R.ok("wrote data/cftc-join.json files=%s" % len(files))
        if not files:
            R.fail("no cftc files")
            sys.exit(1)


if __name__ == "__main__":
    main()
