"""ops_5401 -- join orphan regional-Fed nowcasts onto the activity desk.

Atlanta Fed GDPNow and Cleveland Fed yield-curve are warehoused but
activity-nowcast.html never reads them. This op copies live S3 objects
into data/fed-nowcast-join.json (nulls stay null; no synthetic fills)
and proves read-back. Pages already fetch warehouse JSON.
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

CANDIDATES = [
    "data/providers/atlantafed.json",
    "data/providers/clevelandfed.json",
    "data/atlantafed.json",
    "data/clevelandfed.json",
    "data/gdpnow.json",
    "data/cleveland-fed.json",
    "data/warm/atlantafed/",
    "data/warm/clevelandfed/",
]


def _get(s3, key):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        raw = obj["Body"].read()
        try:
            body = json.loads(raw)
        except Exception:
            body = {"_raw_bytes": len(raw), "_not_json": True}
        return {"ok": True, "key": key, "bytes": len(raw),
                "last_modified": obj["LastModified"].isoformat(),
                "body": body}
    except Exception as e:
        return {"ok": False, "key": key, "error": "%s: %s" % (type(e).__name__, str(e)[:160])}


def _list_prefix(s3, prefix, limit=8):
    keys = []
    token = None
    while True:
        kw = {"Bucket": B, "Prefix": prefix, "Delimiter": "/", "MaxKeys": 50}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for it in resp.get("Contents") or []:
            keys.append({"key": it["Key"], "bytes": it["Size"],
                         "modified": it["LastModified"].isoformat()})
            if len(keys) >= limit:
                return keys
        token = resp.get("NextContinuationToken")
        if not token:
            break
    return keys


def main():
    with report("ops_5401_fed_nowcast_join") as R:
        R.heading("ops 5401 -- Atlanta + Cleveland Fed join")
        s3 = boto3.client("s3", region_name=REGION)
        found = []
        missing = []
        for key in CANDIDATES:
            if key.endswith("/"):
                listed = _list_prefix(s3, key)
                R.log("prefix %s n=%d" % (key, len(listed)))
                for row in listed[:3]:
                    got = _get(s3, row["key"])
                    if got["ok"]:
                        found.append(got)
                    R.log("  %s %s" % (row["key"], row["bytes"]))
                continue
            got = _get(s3, key)
            if got["ok"]:
                found.append(got)
                R.ok("hit %s %sB" % (key, got["bytes"]))
            else:
                missing.append(key)
                R.warn("miss %s" % key)

        def slim(item):
            body = item.get("body")
            if not isinstance(body, dict):
                return {"key": item["key"], "last_modified": item.get("last_modified"),
                        "shape": type(body).__name__}
            keep = {}
            for k in ("generated_at", "as_of", "schema_version", "name", "slug",
                      "latest", "value", "gdpnow", "nowcast", "level", "date",
                      "series", "note", "status", "freshest_h", "datasets"):
                if k in body:
                    keep[k] = body[k]
            # never invent numbers
            return {
                "key": item["key"],
                "last_modified": item.get("last_modified"),
                "bytes": item.get("bytes"),
                "fields": keep,
                "top_keys": list(body.keys())[:24],
            }

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source": "ops_5401",
            "hits": [slim(x) for x in found],
            "missing_keys": missing,
            "n_hits": len(found),
            "status": "LIVE" if found else "DATA_HOLD",
        }
        s3.put_object(
            Bucket=B,
            Key="data/fed-nowcast-join.json",
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = _get(s3, "data/fed-nowcast-join.json")
        if not back["ok"] or not isinstance(back.get("body"), dict):
            R.fail("read-back failed")
            sys.exit(1)
        if back["body"].get("schema_version") != 1:
            R.fail("schema mismatch")
            sys.exit(1)
        R.section("verdict")
        R.log("hits=%d missing=%d status=%s" % (len(found), len(missing), payload["status"]))
        R.ok("GREEN -- data/fed-nowcast-join.json written and read back")


if __name__ == "__main__":
    main()
