"""justhodl-boj-full v1.0.0 -- Bank of Japan flat-file warehouse.

Harvest the official download page for every whole-database zip
(the ENTIRE BOJ time-series portal as flat files) and mirror them
verbatim, sha-conditional. Complements the cracked per-code API
(gov-sources). data/warm/boj-full/ · rate(12h)
"""
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-boj-full v1.0.0 ops4985 flatfiles"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SITE = "https://www.stat-search.boj.or.jp"
PAGE = SITE + "/info/dload_en.html"
ROOT = "data/warm/boj-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "Mozilla/5.0 JustHodl Research "
      "(raafouis@gmail.com)"}
BUDGET_S = 640
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
                                    "zips": {}, "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put(STATE_KEY, state)
    zl = state["zips"]
    hrefs = []
    try:
        req = urllib.request.Request(PAGE, headers=UA)
        with urllib.request.urlopen(req, timeout=60) as r:
            html = r.read(4_000_000).decode("utf-8", "replace")
        hrefs = sorted(set(re.findall(
            r'href="([^"]+?\.zip)"', html, re.I)))
        state["universe"] = len(hrefs)
    except Exception as e:
        state["failures"]["_page"] = str(e)[:90]
        hrefs = [v.get("href") for v in zl.values()
                 if v.get("href")]
    for h in hrefs:
        if time.time() - t0 > BUDGET_S - 60:
            break
        url = h if h.startswith("http") else (
            SITE + h if h.startswith("/") else
            SITE + "/info/" + h)
        name = re.sub(r"[^A-Za-z0-9._-]+", "_",
                      h.rsplit("/", 1)[-1])[:120]
        prev = zl.get(name) or {}
        if prev.get("ok") and \
                time.time() - float(prev.get("epoch") or 0) \
                < 11 * 3600:
            continue
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=180) as r:
                raw = r.read(300_000_000)
            if len(raw) < 500 or raw[:2] != b"PK":
                raise RuntimeError("not-a-zip (%dB)" % len(raw))
            dig = hashlib.sha256(raw).hexdigest()[:16]
            if prev.get("sha") != dig:
                s3.put_object(Bucket=BUCKET, Key=ROOT + name,
                              Body=raw,
                              ContentType="application/zip",
                              Metadata={"engine": "boj-full",
                                        "src": url[:110]})
            zl[name] = {"ok": True, "bytes": len(raw),
                        "sha": dig, "href": h,
                        "status": "fresh"
                        if prev.get("sha") != dig
                        else "unchanged",
                        "epoch": time.time(), "at": _now()}
            state["failures"].pop(name, None)
        except Exception as e:
            zl[name] = {"ok": False, "epoch": time.time(),
                        "href": h}
            state["failures"][name] = str(e)[:90]
        time.sleep(0.5)
    state["lease_until"] = 0
    state["as_of"] = _now()
    _put(STATE_KEY, state)
    ok = {k: v for k, v in zl.items() if v.get("ok")}
    _put(MANIFEST_KEY, {
        "as_of": state["as_of"], "engine": "justhodl-boj-full",
        "version": "1.0.0", "zips": len(ok),
        "universe": state.get("universe"),
        "mb": round(sum(v.get("bytes") or 0
                        for v in ok.values()) / 1e6, 1),
        "failures": len(state["failures"]),
        "note": ("BOJ flat-file warehouse: every whole-database "
                 "zip from the official download page, verbatim, "
                 "12h conditional refresh -- the entire "
                 "time-series portal")})
    return {"ok": True, "zips": len(ok),
            "universe": state.get("universe"),
            "failures": len(state["failures"]),
            "elapsed_s": round(time.time() - t0, 1)}
