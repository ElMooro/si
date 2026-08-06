"""justhodl-ecb-full-catalog — E5 v1 (ops 4454).

Nightly ECB SDMX dataflow catalog: the official registry of every ECB
dataset (EXR, BSI, MIR, ICP, YC, ...) from data-api.ecb.europa.eu ->
id + name + version -> data/warm/ecb/catalog.json.gz + summary. This is
the map E10's backfill walks; per-dataset series pulls are its job, stated
not silent. F4 snapshot of the raw XML."""
import gzip
import json
import os
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    url = "https://data-api.ecb.europa.eu/service/dataflow/ECB"
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai",
        "Accept": "application/xml"})
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
    raw_key = snapshot("ecb", url, raw) if snapshot else None
    root = ET.fromstring(raw)
    flows = []
    for df in root.iter():
        if df.tag.endswith("}Dataflow"):
            fid = df.attrib.get("id")
            ver = df.attrib.get("version")
            name = None
            for ch in df:
                if ch.tag.endswith("}Name"):
                    name = (ch.text or "").strip()
                    break
            if fid:
                flows.append({"id": fid, "name": name, "version": ver})
    flows.sort(key=lambda x: x["id"])
    s3.put_object(Bucket=BUCKET, Key="data/warm/ecb/catalog.json.gz",
                  Body=gzip.compress(json.dumps(
                      {"as_of": now.isoformat(timespec="seconds"),
                       "source_url": url, "raw_snapshot_key": raw_key,
                       "n_dataflows": len(flows),
                       "note": "dataflow registry; per-dataset series "
                               "pulls = E10 backfill",
                       "dataflows": flows}).encode()),
                  ContentType="application/gzip")
    key_ids = [f["id"] for f in flows
               if f["id"] in ("EXR", "BSI", "MIR", "ICP", "YC", "FM",
                              "STS", "GFS")]
    s3.put_object(Bucket=BUCKET, Key="data/warm/ecb/catalog-summary.json",
                  Body=json.dumps({
                      "as_of": now.isoformat(timespec="seconds"),
                      "n_dataflows": len(flows),
                      "key_flows_present": key_ids,
                      "sample": flows[:8]}).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "n_dataflows": len(flows),
           "key_flows": key_ids}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
