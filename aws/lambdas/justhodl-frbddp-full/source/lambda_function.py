"""justhodl-frbddp-full v1.0.0 -- COMPLETE Fed Data Download packages.

Proven shape (usgov-direct ops 4467): Output.aspx?rel={R}&filetype=zip
returns the ENTIRE release package zip -- every series, full history.
This engine widens to the full program list and keeps them fresh with
size+etag conditionals. Unknown rels fail NAMED.
data/warm/frbddp-full/{REL}.zip · rate(12h)
"""
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-frbddp-full v1.0.0 ops4985 packages"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
ROOT = "data/warm/frbddp-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)"}
BUDGET_S = 640
RELS = ["H15", "H41", "H3", "H6", "H8", "H10", "Z1", "G17",
        "G19", "G20", "CP", "CHGDEL", "E2", "FOR", "PRATES",
        "SLOOS", "H2", "H4", "G5", "G5A", "SCOOS", "TA",
        "Z1A", "H1", "DKI"]
s3 = boto3.client("s3", region_name="us-east-1")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _j(key, default=None):
    try:
        return json.loads(s3.get_object(
            Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, indent=1).encode(),
                  ContentType="application/json")


def lambda_handler(event, ctx=None):
    t0 = time.time()
    state = _j(STATE_KEY, None) or {"version": "1.0.0",
                                    "rels": {}, "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put(STATE_KEY, state)
    rl = state["rels"]
    for rel in RELS:
        if time.time() - t0 > BUDGET_S - 60:
            break
        prev = rl.get(rel) or {}
        if prev.get("ok") and \
                time.time() - float(prev.get("epoch") or 0) \
                < 11 * 3600:
            continue
        u = ("https://www.federalreserve.gov/datadownload/"
             "Output.aspx?rel=%s&filetype=zip" % rel)
        try:
            req = urllib.request.Request(u, headers=UA)
            with urllib.request.urlopen(req, timeout=180) as r:
                raw = r.read(400_000_000)
            if len(raw) < 2000 or raw[:2] != b"PK":
                raise RuntimeError("not-a-zip (%dB)" % len(raw))
            dig = hashlib.sha256(raw).hexdigest()[:16]
            if prev.get("sha") != dig:
                s3.put_object(Bucket=BUCKET,
                              Key=ROOT + "%s.zip" % rel,
                              Body=raw,
                              ContentType="application/zip",
                              Metadata={"engine": "frbddp-full",
                                        "rel": rel})
            rl[rel] = {"ok": True, "bytes": len(raw), "sha": dig,
                       "status": "fresh" if prev.get("sha") != dig
                       else "unchanged",
                       "epoch": time.time(), "at": _now()}
            state["failures"].pop(rel, None)
        except Exception as e:
            rl[rel] = {"ok": False, "epoch": time.time()}
            state["failures"][rel] = str(e)[:90]
        time.sleep(0.4)
    state["lease_until"] = 0
    state["as_of"] = _now()
    _put(STATE_KEY, state)
    ok = {k: v for k, v in rl.items() if v.get("ok")}
    _put(MANIFEST_KEY, {
        "as_of": state["as_of"], "engine": "justhodl-frbddp-full",
        "version": "1.0.0", "packages": len(ok),
        "mb": round(sum(v.get("bytes") or 0
                        for v in ok.values()) / 1e6, 1),
        "failures": len(state["failures"]),
        "invalid_named": sorted(state["failures"]),
        "note": ("complete Fed Data Download release packages "
                 "(Z.1 flow of funds, H.4.1, H.15, H.8, G.17...) "
                 "-- every series full history, verbatim zips, "
                 "12h conditional refresh")})
    return {"ok": True, "packages": len(ok),
            "failures": len(state["failures"]),
            "elapsed_s": round(time.time() - t0, 1)}
