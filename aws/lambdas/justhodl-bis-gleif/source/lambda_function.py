"""justhodl-bis-gleif — registry items: BIS SDMX + GLEIF (ops 4468).

BIS: dataflow registry (the map E10 walks — same pattern as E5/ECB) from
stats.bis.org SDMX -> data/warm/bis/catalog.json.gz.
GLEIF: the ISIN-LEI mapping file (the symbology cross-walk the council
wanted) via candidate-chain over mapping.gleif.org download shapes ->
stored zip + size; full Golden Copy (450MB+) is a stated E10/ECS-scale
follow-up, not silently skipped. F4 snapshots; explicit failures."""
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


def _fetch(u, timeout=90):
    req = urllib.request.Request(u, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai",
        "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _bis(summary):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cands = ["https://stats.bis.org/api/v1/dataflow/BIS/all/latest",
             "https://stats.bis.org/api/v1/dataflow/all/all/latest",
             "https://stats.bis.org/api/v1/dataflow"]
    for u in cands:
        try:
            raw = _fetch(u)
            rk = snapshot("bis", u, raw) if snapshot else None
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
                                      "version":
                                          df.attrib.get("version")})
            if len(flows) < 3:
                raise ValueError(f"only {len(flows)} flows parsed")
            flows.sort(key=lambda x: x["id"])
            s3.put_object(Bucket=BUCKET,
                          Key="data/warm/bis/catalog.json.gz",
                          Body=gzip.compress(json.dumps(
                              {"as_of": now, "source_url": u,
                               "raw_snapshot_key": rk,
                               "n_dataflows": len(flows),
                               "dataflows": flows}).encode()),
                          ContentType="application/gzip")
            summary["bis"] = {"ok": True, "n_dataflows": len(flows),
                              "sample": [f["id"] for f in flows[:8]],
                              "source": u}
            return
        except Exception as e:
            last = f"{u} -> {type(e).__name__}: {str(e)[:60]}"
    summary["bis"] = {"data_unavailable": True, "reason": last}


def _gleif(summary):
    cands = [
        "https://mapping.gleif.org/api/v2/isin-lei/latest/download",
        "https://isinmapping.gleif.org/api/v2/isin-lei/latest/download",
        "https://mapping.gleif.org/api/v2/isin-lei",
    ]
    for u in cands:
        try:
            raw = _fetch(u, timeout=240)
            if len(raw) < 10000:
                d = json.loads(raw)
                dl = (d.get("data") or [{}])[0] if isinstance(
                    d.get("data"), list) else d
                real = (dl.get("attributes", {}) or {}).get(
                    "downloadLink") or dl.get("downloadLink")
                if not real:
                    raise ValueError("no downloadLink in JSON")
                raw = _fetch(real, timeout=240)
                u = real
            rk = snapshot("gleif", u, raw[:200000]) if snapshot else None
            s3.put_object(Bucket=BUCKET,
                          Key="data/warm/gleif/isin-lei-latest.zip",
                          Body=raw)
            summary["gleif"] = {"ok": True, "bytes": len(raw),
                                "mb": round(len(raw) / 1e6, 1),
                                "source": u[:90],
                                "raw_snapshot_key": rk,
                                "note": "full Golden Copy = stated "
                                        "E10 follow-up"}
            return
        except Exception as e:
            last = f"{u[:60]} -> {type(e).__name__}: {str(e)[:60]}"
    summary["gleif"] = {"data_unavailable": True, "reason": last}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    summary = {"as_of": now}
    _bis(summary)
    _gleif(summary)
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/bis-gleif-summary.json",
                  Body=json.dumps(summary, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(json.dumps(summary, default=str)[:500])
    return {"statusCode": 200, "body": json.dumps(summary, default=str)}
