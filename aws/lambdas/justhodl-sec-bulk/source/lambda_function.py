"""justhodl-sec-bulk — registry item: SEC bulk zips (ops 4474).

The whole-market fundamentals layer: SEC's official bulk files —
companyfacts.zip (EVERY XBRL fact for EVERY registrant, ~1.2GB) and
submissions.zip (every filer's filing history). One file per run
(alternating cursor), streamed chunked to /tmp (4GB ephemeral) then
uploaded to data/warm/sec-bulk/ with size + sha256. Weekly cadence per
file. Explicit failures; the zip itself IS the raw layer."""
import hashlib
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
FILES = [
    ("companyfacts", "https://www.sec.gov/Archives/edgar/daily-index/"
     "xbrl/companyfacts.zip"),
    ("submissions", "https://www.sec.gov/Archives/edgar/daily-index/"
     "bulkdata/submissions.zip"),
]


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state_key = "data/_state/sec-bulk.json"
    try:
        state = json.loads(s3.get_object(
            Bucket=BUCKET, Key=state_key)["Body"].read())
    except Exception:
        state = {"next": 0, "history": []}
    idx = state.get("next", 0) % len(FILES)
    name, url = FILES[idx]
    tmp = f"/tmp/{name}.zip"
    rec = {"file": name, "url": url, "started": now}
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "JustHodl research admin@justhodl.ai"})
        h = hashlib.sha256()
        size = 0
        with urllib.request.urlopen(req, timeout=120) as r, \
                open(tmp, "wb") as f:
            while True:
                chunk = r.read(8 * 1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                h.update(chunk)
                size += len(chunk)
        if size < 50_000_000:
            raise ValueError(f"suspiciously small: {size}b")
        s3.upload_file(tmp, BUCKET, f"data/warm/sec-bulk/{name}.zip")
        rec.update({"ok": True, "bytes": size,
                    "gb": round(size / 1e9, 2),
                    "sha256": h.hexdigest()[:16]})
    except Exception as e:
        rec.update({"data_unavailable": True,
                    "reason": f"{type(e).__name__}: {str(e)[:80]}"})
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass
    state["next"] = idx + 1
    state["history"] = (state.get("history", []) + [rec])[-10:]
    state["as_of"] = now
    s3.put_object(Bucket=BUCKET, Key=state_key,
                  Body=json.dumps(state, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(json.dumps(rec, default=str))
    return {"statusCode": 200, "body": json.dumps(rec, default=str)}
