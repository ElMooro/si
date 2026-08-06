"""justhodl-eurostat-oecd — the last free-tier rail (ops 4481).

Eurostat + OECD SDMX dataflow catalogs (the ECB/BIS pattern, two more
agencies) -> data/warm/{eurostat,oecd}/catalog.json.gz. These maps are the
E10 walk-lists for European statistics and the OECD CLI family. Candidate-
chain per agency; explicit failures; F4 snapshots. Weekly."""
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

AGENCIES = {
    "eurostat": ["https://ec.europa.eu/eurostat/api/dissemination/"
                 "sdmx/2.1/dataflow/ESTAT/all",
                 "https://ec.europa.eu/eurostat/api/dissemination/"
                 "sdmx/2.1/dataflow/all/all/latest"],
    "oecd": ["https://sdmx.oecd.org/public/rest/dataflow/all/all/latest",
             "https://sdmx.oecd.org/public/rest/dataflow"],
}


def _fetch(u):
    req = urllib.request.Request(u, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai",
        "Accept": "application/xml"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def _parse_flows(raw):
    root = ET.fromstring(raw)
    flows = []
    for df in root.iter():
        if df.tag.endswith("}Dataflow"):
            fid = df.attrib.get("id")
            name = None
            for ch in df:
                if ch.tag.endswith("}Name"):
                    name = (ch.text or "").strip()
                    break
            if fid:
                flows.append({"id": fid, "name": name,
                              "version": df.attrib.get("version")})
    return sorted(flows, key=lambda x: x["id"])


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    summary = {"as_of": now}
    for ag, cands in AGENCIES.items():
        last = "none tried"
        for u in cands:
            try:
                raw = _fetch(u)
                rk = snapshot(ag, u, raw[:400000]) if snapshot else None
                flows = _parse_flows(raw)
                if len(flows) < 5:
                    raise ValueError(f"only {len(flows)} flows")
                s3.put_object(Bucket=BUCKET,
                              Key=f"data/warm/{ag}/catalog.json.gz",
                              Body=gzip.compress(json.dumps(
                                  {"as_of": now, "source_url": u,
                                   "raw_snapshot_key": rk,
                                   "n_dataflows": len(flows),
                                   "dataflows": flows}).encode()),
                              ContentType="application/gzip")
                summary[ag] = {"ok": True, "n_dataflows": len(flows),
                               "sample": [f["id"] for f in flows[:6]]}
                break
            except Exception as e:
                last = f"{u[:60]} -> {type(e).__name__}: {str(e)[:60]}"
        else:
            summary[ag] = {"data_unavailable": True, "reason": last}
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/eurostat-oecd-summary.json",
                  Body=json.dumps(summary, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(json.dumps(summary, default=str)[:400])
    return {"statusCode": 200, "body": json.dumps(summary, default=str)}
