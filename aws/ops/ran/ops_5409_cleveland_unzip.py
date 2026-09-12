"""ops_5409 -- Cleveland canary is gzip wrapping a ZIP. Unwrap then CSV.

Writes data/fed-nowcast-join.json schema 5. Fails if Cleveland last
looks like a PK header.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"
CLE = "data/warm/fred-canary/cleveland-model.csv.gz"
ATL = "data/warm/fred-canary/atlanta-gdpnow.csv.gz"


def _unwrap(raw: bytes):
    notes = []
    blob = raw
    if blob[:2] == b"\x1f\x8b":
        blob = gzip.decompress(blob)
        notes.append("gunzip")
    if blob[:2] == b"PK":
        zf = zipfile.ZipFile(io.BytesIO(blob))
        names = zf.namelist()
        pick = next((n for n in names if n.lower().endswith((".csv", ".txt", ".tsv"))), None)
        if pick is None and names:
            pick = names[0]
        blob = zf.read(pick) if pick else b""
        notes.append("unzip:%s" % pick)
    text = blob.decode("utf-8", errors="replace")
    return text, notes, blob[:4].hex()


def _parse(text: str):
    rows = list(csv.reader(io.StringIO(text, newline="")))
    header = [h.strip() for h in rows[0]] if rows else []
    last = None
    for row in reversed(rows[1:] if rows else []):
        if any((c or "").strip() for c in row):
            last = row
            break
    paired = {}
    if last is not None:
        for i, h in enumerate(header):
            if i < len(last) and h:
                paired[h] = last[i]
    return {"n_rows": max(0, len(rows) - 1), "header": header[:24], "last": paired}


def main():
    with report("ops_5409_cleveland_unzip") as R:
        R.heading("ops 5409 -- Cleveland gzip+ZIP unwrap")
        s3 = boto3.client("s3", region_name=REGION)
        series = {}
        for slug, key in (("atlantafed", ATL), ("clevelandfed", CLE)):
            obj = s3.get_object(Bucket=B, Key=key)
            raw = obj["Body"].read()
            text, notes, inner = _unwrap(raw)
            parsed = _parse(text)
            parsed.update({
                "key": key,
                "bytes": len(raw),
                "outer_magic": raw[:4].hex(),
                "inner_magic": inner,
                "unwrap": notes,
                "last_modified": obj["LastModified"].isoformat(),
            })
            series[slug] = parsed
            R.ok("%s unwrap=%s header=%s last=%s" % (
                slug, notes, parsed["header"][:6],
                {k: parsed["last"].get(k) for k in list(parsed["last"])[:4]}))

        hdrs = series["clevelandfed"].get("header") or []
        if hdrs and str(hdrs[0]).startswith("PK"):
            R.fail("Cleveland still PK after unwrap -- object is not a table")
            sys.exit(1)
        if not series["clevelandfed"].get("last"):
            R.fail("Cleveland last empty")
            sys.exit(1)

        join = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 5,
            "source": "ops_5409",
            "status": "LIVE",
            "series": series,
        }
        s3.put_object(
            Bucket=B,
            Key="data/fed-nowcast-join.json",
            Body=json.dumps(join, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/fed-nowcast-join.json")["Body"].read())
        if back.get("schema_version") != 5:
            R.fail("read-back")
            sys.exit(1)
        R.ok("GREEN -- join v5 Cleveland header=%s" % series["clevelandfed"]["header"][:8])


if __name__ == "__main__":
    main()
