"""ops_5410 -- Cleveland ZIP member pick.

5409 unzipped README.txt. List every member, pick csv/tsv/xlsx/txt that
is not README, parse, write join schema 6. DATA_HOLD if no table.
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


def _blob(raw):
    blob = raw
    if blob[:2] == b"\x1f\x8b":
        blob = gzip.decompress(blob)
    return blob


def _parse_text(text):
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
    with report("ops_5410_cleveland_zip_members") as R:
        R.heading("ops 5410 -- Cleveland ZIP members")
        s3 = boto3.client("s3", region_name=REGION)
        atl_raw = s3.get_object(Bucket=B, Key=ATL)["Body"].read()
        atl_text = gzip.decompress(atl_raw).decode("utf-8", errors="replace")
        atlanta = _parse_text(atl_text)
        atlanta.update({"key": ATL, "unwrap": ["gunzip"]})
        R.ok("atlanta %s" % atlanta["last"])

        obj = s3.get_object(Bucket=B, Key=CLE)
        raw = obj["Body"].read()
        blob = _blob(raw)
        members = []
        picked = None
        parsed = {"n_rows": 0, "header": [], "last": None}
        if blob[:2] == b"PK":
            zf = zipfile.ZipFile(io.BytesIO(blob))
            for info in zf.infolist():
                members.append({"name": info.filename, "bytes": info.file_size})
            R.log("members %s" % members)
            ranked = []
            for m in members:
                n = (m["name"] or "").lower()
                base = n.rsplit("/", 1)[-1]
                if base.startswith("readme") or base.endswith(".md"):
                    continue
                score = 0
                if n.endswith(".csv"):
                    score = 3
                elif n.endswith((".tsv", ".txt")):
                    score = 2
                elif n.endswith((".xlsx", ".xls")):
                    score = 1
                if score:
                    ranked.append((score, m["bytes"], m["name"]))
            ranked.sort(reverse=True)
            if ranked:
                picked = ranked[0][2]
                data = zf.read(picked)
                if picked.lower().endswith((".xlsx", ".xls")):
                    parsed = {
                        "n_rows": None,
                        "header": ["xlsx"],
                        "last": None,
                        "note": "xlsx member -- not csv-parsed here",
                    }
                else:
                    parsed = _parse_text(data.decode("utf-8", errors="replace"))
                R.ok("picked %s header=%s" % (picked, parsed.get("header")))
            else:
                R.warn("no data member")
        else:
            parsed = _parse_text(blob.decode("utf-8", errors="replace"))
            picked = "plain"

        hdr = (parsed.get("header") or [""])[0] if parsed.get("header") else ""
        status = "LIVE"
        if not parsed.get("last") or str(hdr).startswith("-") or str(hdr).startswith("PK"):
            status = "DATA_HOLD"
            R.warn("Cleveland not a numeric table; members logged")

        join = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 6,
            "source": "ops_5410",
            "status": status,
            "series": {
                "atlantafed": atlanta,
                "clevelandfed": {
                    **parsed,
                    "key": CLE,
                    "bytes": len(raw),
                    "picked_member": picked,
                    "zip_members": members,
                    "last_modified": obj["LastModified"].isoformat(),
                },
            },
        }
        s3.put_object(
            Bucket=B,
            Key="data/fed-nowcast-join.json",
            Body=json.dumps(join, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("GREEN -- join v6 status=%s members=%s" % (status, [m["name"] for m in members]))


if __name__ == "__main__":
    main()
