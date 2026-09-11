"""ops_5408 -- Cleveland ZIP parse + IMF cadence inspect.

Cleveland canary is PK/ZIP not gzip CSV. IMF 6h schedule was DISABLED
in ops 5111 (850s timeout). This op does not lambda.invoke and does
not re-enable the 6h rule.
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


def _rows_from_bytes(raw):
    if raw[:2] == b"PK":
        zf = zipfile.ZipFile(io.BytesIO(raw))
        names = zf.namelist()
        # pick first csv-like or first small member
        pick = None
        for n in names:
            if n.lower().endswith(".csv") or n.lower().endswith(".txt"):
                pick = n
                break
        if pick is None and names:
            pick = names[0]
        text = zf.read(pick).decode("utf-8", errors="replace") if pick else ""
        kind = "zip:%s" % pick
    else:
        try:
            text = gzip.decompress(raw).decode("utf-8", errors="replace")
            kind = "gzip"
        except Exception:
            text = raw.decode("utf-8", errors="replace")
            kind = "plain"
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
    return kind, {"n_rows": max(0, len(rows) - 1), "header": header[:24], "last": paired}


def main():
    with report("ops_5408_cleveland_zip_imf") as R:
        R.heading("ops 5408 -- Cleveland ZIP + IMF schedule")
        s3 = boto3.client("s3", region_name=REGION)
        now = datetime.now(timezone.utc).isoformat()
        series = {}
        for slug, key in (("atlantafed", ATL), ("clevelandfed", CLE)):
            obj = s3.get_object(Bucket=B, Key=key)
            raw = obj["Body"].read()
            kind, parsed = _rows_from_bytes(raw)
            parsed.update({
                "key": key,
                "bytes": len(raw),
                "magic": raw[:4].hex(),
                "container": kind,
                "last_modified": obj["LastModified"].isoformat(),
            })
            series[slug] = parsed
            R.ok("%s container=%s rows=%s last_keys=%s" % (
                slug, kind, parsed["n_rows"], list((parsed.get("last") or {}).keys())[:8]))

        join = {
            "generated_at": now,
            "schema_version": 4,
            "source": "ops_5408",
            "status": "LIVE" if any(v.get("last") for v in series.values()) else "DATA_HOLD",
            "series": series,
        }
        s3.put_object(Bucket=B, Key="data/fed-nowcast-join.json",
                      Body=json.dumps(join, default=str).encode("utf-8"),
                      ContentType="application/json")

        imf = {"schedules": [], "note": "6h was DISABLED by ops 5111; weekly keeps warehouse"}
        try:
            sch = boto3.client("scheduler", region_name=REGION)
            for name in ("justhodl-imf-full-6h", "justhodl-imf-full-weekly",
                         "justhodl-imf-full", "justhodl-imf-full-24h"):
                try:
                    s = sch.get_schedule(Name=name)
                    imf["schedules"].append({
                        "name": name,
                        "state": s.get("State"),
                        "expr": s.get("ScheduleExpression"),
                        "tz": s.get("ScheduleExpressionTimezone"),
                    })
                    R.log("schedule %s %s %s" % (name, s.get("State"), s.get("ScheduleExpression")))
                except Exception as e:
                    imf["schedules"].append({"name": name, "error": type(e).__name__})
        except Exception as e:
            imf["scheduler_error"] = type(e).__name__
            R.warn("scheduler %s" % type(e).__name__)

        # prefix age only
        listed = s3.list_objects_v2(Bucket=B, Prefix="data/warm/imf-full/", Delimiter="/", MaxKeys=5)
        imf["warm_listed"] = len(listed.get("Contents") or []) + len(listed.get("CommonPrefixes") or [])
        s3.put_object(Bucket=B, Key="data/imf-cadence.json",
                      Body=json.dumps({"generated_at": now, "schema_version": 1,
                                       "source": "ops_5408", **imf}, default=str).encode(),
                      ContentType="application/json")
        R.ok("GREEN -- join v4 + imf-cadence.json (no invoke)")


if __name__ == "__main__":
    main()
