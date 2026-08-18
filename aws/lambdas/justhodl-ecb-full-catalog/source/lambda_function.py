"""justhodl-ecb-full-catalog — E5 v1 (ops 4454).

Nightly ECB SDMX dataflow catalog: the official registry of every ECB
dataset (EXR, BSI, MIR, ICP, YC, ...) from data-api.ecb.europa.eu ->
id + name + version -> data/warm/ecb/catalog.json.gz + summary. This is
the map E10's backfill walks; per-dataset series pulls are its job, stated
not silent. F4 snapshot of the raw XML."""
import gzip
import json
import os
import urllib.error
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
    # ops 4893 (Khalid: "investigate how ciss pulls ECB"): the single
    # Accept "application/xml" was the 406 -- ECB's STRUCTURE endpoint
    # refuses that representation while the DATA endpoint has served
    # justhodl-ciss-stress 24/7 the whole time (UA-only request,
    # ?format=csvdata sidesteps content negotiation entirely). Port
    # the same posture here: honest UA, then a ladder -- server-default
    # representation first, the documented SDMX-ML 2.1 structure type
    # second, wildcard last. First 2xx wins; the winner and every
    # refusal are recorded (stated, not silent).
    attempts = [
        ("no-accept", None),
        ("sdmx-structure-xml-2.1",
         "application/vnd.sdmx.structure+xml;version=2.1"),
        ("wildcard", "*/*"),
    ]
    raw, winner, ladder = None, None, []
    for label, acc in attempts:
        h = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
        if acc:
            h["Accept"] = acc
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
            winner = label
            ladder.append({"attempt": label, "status": 200})
            break
        except urllib.error.HTTPError as e:
            ladder.append({"attempt": label, "status": e.code})
        except Exception as e:
            ladder.append({"attempt": label,
                           "error": f"{type(e).__name__}: {str(e)[:80]}"})
    if raw is None:
        print(json.dumps({"ok": False, "negotiation": ladder}))
        raise RuntimeError(
            "ECB dataflow catalog: every Accept negotiation refused "
            "-- %s" % json.dumps(ladder))
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
                       "accept_winner": winner,
                       "negotiation": ladder,
                       "dataflows": flows}).encode()),
                  ContentType="application/gzip")
    key_ids = [f["id"] for f in flows
               if f["id"] in ("EXR", "BSI", "MIR", "ICP", "YC", "FM",
                              "STS", "GFS")]
    s3.put_object(Bucket=BUCKET, Key="data/warm/ecb/catalog-summary.json",
                  Body=json.dumps({
                      "as_of": now.isoformat(timespec="seconds"),
                      "n_dataflows": len(flows),
                      "accept_winner": winner,
                      "key_flows_present": key_ids,
                      "sample": flows[:8]}).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "n_dataflows": len(flows),
           "accept_winner": winner, "negotiation": ladder,
           "key_flows": key_ids}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
