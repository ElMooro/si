"""ops_5402 -- parse live Atlanta GDPNow + Cleveland curve CSVs.

5401 only found provider catalog cards. Real observations live at
data/warm/fred-canary/atlanta-gdpnow.csv.gz and cleveland-model.csv.gz.
Parse last numeric row, write values onto data/fed-nowcast-join.json.
No invented numbers.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"
FILES = {
    "atlantafed": "data/warm/fred-canary/atlanta-gdpnow.csv.gz",
    "clevelandfed": "data/warm/fred-canary/cleveland-model.csv.gz",
}


def _get_bytes(s3, key):
    obj = s3.get_object(Bucket=B, Key=key)
    return obj["Body"].read(), obj["LastModified"].isoformat()


def _parse_csv_gz(raw):
    text = gzip.decompress(raw).decode("utf-8", errors="replace")
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except Exception:
        dialect = csv.excel
    rows = list(csv.reader(io.StringIO(text), dialect))
    if not rows:
        return {"n_rows": 0, "header": [], "last": None}
    header = [h.strip() for h in rows[0]]
    last = None
    for row in reversed(rows[1:]):
        if any(c.strip() for c in row):
            last = row
            break
    paired = {}
    if last is not None:
        for i, h in enumerate(header):
            if i < len(last) and h:
                paired[h] = last[i]
    return {"n_rows": max(0, len(rows) - 1), "header": header[:16], "last": paired}


def main():
    with report("ops_5402_fed_nowcast_values") as R:
        R.heading("ops 5402 -- parse regional Fed canary CSVs")
        s3 = boto3.client("s3", region_name=REGION)
        parsed = {}
        fails = []
        for slug, key in FILES.items():
            try:
                raw, lm = _get_bytes(s3, key)
                info = _parse_csv_gz(raw)
                info["key"] = key
                info["last_modified"] = lm
                info["bytes"] = len(raw)
                parsed[slug] = info
                R.ok("%s rows=%s last_keys=%s" % (
                    slug, info["n_rows"], list((info.get("last") or {}).keys())[:8]))
            except Exception as e:
                fails.append("%s %s" % (slug, type(e).__name__))
                R.warn("%s: %s" % (slug, e))
                parsed[slug] = {"key": key, "error": str(e)[:200], "last": None}

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 2,
            "source": "ops_5402",
            "status": "LIVE" if any((v.get("last") for v in parsed.values())) else "DATA_HOLD",
            "series": parsed,
        }
        s3.put_object(
            Bucket=B,
            Key="data/fed-nowcast-join.json",
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/fed-nowcast-join.json")["Body"].read())
        if back.get("schema_version") != 2:
            R.fail("read-back schema miss")
            sys.exit(1)
        R.section("verdict")
        if fails and not any((v.get("last") for v in parsed.values())):
            R.fail("no series parsed: %s" % fails)
            sys.exit(1)
        R.ok("GREEN -- fed-nowcast-join.json v2 with parsed last rows")


if __name__ == "__main__":
    main()
