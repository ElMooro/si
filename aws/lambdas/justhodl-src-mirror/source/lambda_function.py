"""justhodl-src-mirror — v1.0 (ops 4913).

Investigation verdict (Khalid: "are NY Fed and OFR importing
properly?"): data/warm/ofr-bsrm/ and data/warm/ofr-site/ were NEVER
importing — one-shot runner harvests (ops 4753 / 4755) with zero
refresh engine; their board freshness was just the last time an ops
touched them. This engine gives them a real import loop:

  ofr-bsrm : the two source workbooks, verbatim --
             financialresearch.gov/bank-systemic-risk-monitor/data/
             {ofr_bsrm.xlsx, ofr_bsrm_international_scores.xlsx}
  ofr-site : live page-harvest (the 4755 pattern) of every
             csv/xlsx/xls/json/zip data href on the FRG root pages,
             mirrored by basename.

Conditional fetch: upstream ETag/Length vs stored object metadata --
unchanged content is skipped, and a tiny _last-check.json is stamped
under each prefix EVERY run so the board's freshness reflects the
truth: the import attempt happened (an unchanged quarterly source is
healthy, not stale). Parsed-series re-transforms (the 500 bsrm series
from ops 4753) are phase-2, flagged in data/warm/_audit/
refresh-orphans.json — stated, not silent. Daily Scheduler.
"""
import gzip
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRG = "https://www.financialresearch.gov"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com",
      "Accept": "*/*"}
STATE_KEY = "data/_state/src-mirror.json"
BSRM_FILES = ("ofr_bsrm.xlsx", "ofr_bsrm_international_scores.xlsx")
SITE_PAGES = (FRG + "/", FRG + "/data")

s3 = boto3.client("s3", region_name="us-east-1")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _put_json(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")


def _head_meta(key):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        return (h.get("Metadata") or {}), h.get("ContentLength")
    except Exception:
        return {}, None


def _upstream_sig(url):
    try:
        rq = urllib.request.Request(url, headers=UA, method="HEAD")
        with urllib.request.urlopen(rq, timeout=25) as r:
            return (r.headers.get("ETag") or "",
                    r.headers.get("Content-Length") or "",
                    r.headers.get("Last-Modified") or "")
    except Exception:
        return None


def _fetch(url, cap=60_000_000):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=120) as r:
        raw = r.read(cap)
        ct = r.headers.get("Content-Type", "")
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw, ct


def _mirror(url, key):
    """Returns status: fresh|unchanged|err:<t>."""
    sig = _upstream_sig(url)
    meta, size = _head_meta(key)
    if sig and size is not None:
        et, cl, lm = sig
        if (et and et == meta.get("src_etag")) or \
                (cl and lm and cl == meta.get("src_len")
                 and lm == meta.get("src_lastmod")):
            return "unchanged", int(cl or size or 0)
    try:
        raw, ct = _fetch(url)
        if len(raw) < 40:
            return "err:tiny", len(raw)
        md = {"engine": "src-mirror", "src_url": url[:200],
              "fetched_at": _now()}
        if sig:
            md.update(src_etag=sig[0][:120], src_len=sig[1][:24],
                      src_lastmod=sig[2][:48])
        s3.put_object(Bucket=BUCKET, Key=key, Body=raw,
                      ContentType=ct or "application/octet-stream",
                      Metadata=md)
        return "fresh", len(raw)
    except urllib.error.HTTPError as e:
        return f"err:HTTP{e.code}", 0
    except Exception as e:
        return f"err:{type(e).__name__}", 0


def _page(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=40).read(
        1_500_000).decode("utf-8", "ignore")


def _absu(h):
    return h if h.startswith("http") else FRG + (
        h if h.startswith("/") else "/" + h)


def harvest_site():
    cands = set()
    for p in SITE_PAGES:
        try:
            body = _page(p)
        except Exception:
            continue
        for m in re.findall(
                r'href="([^"]+\.(?:csv|xlsx|xls|json|zip))"',
                body, re.I):
            u = _absu(m)
            if "bank-systemic-risk-monitor/data" in u:
                continue  # bsrm lane owns those
            cands.add(u)
    return sorted(cands)[:40]


def lambda_handler(event, context):
    t0 = time.time()
    res = {"lanes": {}}
    # lane 1: bsrm workbooks
    b = {}
    for fn in BSRM_FILES:
        st, nb = _mirror(f"{FRG}/bank-systemic-risk-monitor/data/"
                         f"{fn}", f"data/warm/ofr-bsrm/{fn}")
        b[fn] = {"status": st, "bytes": nb}
    _put_json("data/warm/ofr-bsrm/_last-check.json",
              {"at": _now(), "engine": "src-mirror", "files": b})
    res["lanes"]["ofr-bsrm"] = b

    # lane 2: ofr-site harvest mirror
    s_ = {}
    for u in harvest_site():
        name = u.rsplit("/", 1)[-1][:120]
        st, nb = _mirror(u, f"data/warm/ofr-site/{name}")
        s_[name] = {"status": st, "bytes": nb}
        if time.time() - t0 > 700:
            break
    _put_json("data/warm/ofr-site/_last-check.json",
              {"at": _now(), "engine": "src-mirror",
               "harvested": len(s_), "files": s_})
    res["lanes"]["ofr-site"] = {"harvested": len(s_),
                                "fresh": sum(1 for v in s_.values()
                                             if v["status"] ==
                                             "fresh")}

    _put_json(STATE_KEY, {"as_of": _now(), "ok": True,
                          "summary": res,
                          "elapsed_s": round(time.time() - t0, 1)})
    out = {"ok": True, **res,
           "elapsed_s": round(time.time() - t0, 1)}
    print(json.dumps(out)[:800])
    return {"statusCode": 200, "body": json.dumps(out)}
