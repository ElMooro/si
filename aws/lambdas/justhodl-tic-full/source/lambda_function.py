"""justhodl-tic-full v1.0.0 -- US Treasury TIC text-file mirror.

Khalid's ask (eurodollar desk): bctype.txt -- US bank claims on
foreigners -- plus siblings. Path ladder per file (Publish/ and
resource-center/Documents/), verbatim mirror, sha-conditional.
data/warm/tic-full/{name}.txt · rate(12h)
"""
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-tic-full v1.0.0 ops4985 bctype"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
ROOT = "data/warm/tic-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)"}
BUDGET_S = 600
FILES = ["bctype.txt", "bltype.txt", "bcctry.txt", "blctry.txt",
         "mfh.txt", "mfhhis01.txt", "slt1d.txt", "slt2d.txt",
         "slt3d.txt", "ticpress.txt", "s1_99996.txt",
         "netfdi.txt"]
LADDER = ["https://ticdata.treasury.gov/Publish/%s",
          "https://ticdata.treasury.gov/resource-center/"
          "data-chart-center/tic/Documents/%s"]
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
                                    "files": {}, "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put(STATE_KEY, state)
    fl = state["files"]
    for name in FILES:
        if time.time() - t0 > BUDGET_S - 40:
            break
        prev = fl.get(name) or {}
        if prev.get("ok") and \
                time.time() - float(prev.get("epoch") or 0) \
                < 11 * 3600:
            continue
        got, err = None, "no-path-answered"
        for pat in LADDER:
            try:
                req = urllib.request.Request(pat % name,
                                             headers=UA)
                with urllib.request.urlopen(req,
                                            timeout=90) as r:
                    raw = r.read(60_000_000)
                head = raw[:400].lower()
                if len(raw) < 300 or b"<html" in head:
                    err = "html/thin(%dB)" % len(raw)
                    continue
                got = (raw, pat % name)
                break
            except Exception as e:
                err = str(e)[:70]
        if got:
            raw, src = got
            dig = hashlib.sha256(raw).hexdigest()[:16]
            if prev.get("sha") != dig:
                s3.put_object(Bucket=BUCKET, Key=ROOT + name,
                              Body=raw,
                              ContentType="text/plain",
                              Metadata={"engine": "tic-full",
                                        "src": src[:110]})
            fl[name] = {"ok": True, "bytes": len(raw),
                        "sha": dig, "src": src,
                        "status": "fresh"
                        if prev.get("sha") != dig
                        else "unchanged",
                        "epoch": time.time(), "at": _now()}
            state["failures"].pop(name, None)
        else:
            fl[name] = {"ok": False, "epoch": time.time()}
            state["failures"][name] = err
        time.sleep(0.4)
    state["lease_until"] = 0
    state["as_of"] = _now()
    _put(STATE_KEY, state)
    ok = {k: v for k, v in fl.items() if v.get("ok")}
    _put(MANIFEST_KEY, {
        "as_of": state["as_of"], "engine": "justhodl-tic-full",
        "version": "1.0.0", "files": len(ok),
        "mb": round(sum(v.get("bytes") or 0
                        for v in ok.values()) / 1e6, 2),
        "failures": len(state["failures"]),
        "invalid_named": sorted(state["failures"]),
        "note": ("TIC banking + flows text mirror (bctype claims "
                 "on foreigners, bltype liabilities, country "
                 "detail, SLT, MFH) verbatim since publication; "
                 "12h refresh -- eurodollar-desk source")})
    return {"ok": True, "files": len(ok),
            "failures": len(state["failures"]),
            "elapsed_s": round(time.time() - t0, 1)}
