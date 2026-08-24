"""justhodl-src-mirror — v1.1 (ops 4913 + nyfed-research lane ops 4953).

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
  nyfed-research (v1.1, ops 4953 -- closes the last audit orphan):
             (a) the two tri-party haircut workbooks verbatim
                 (medialibrary Interactives endpoints, ops 4759 URLs);
             (b) every file in data/warm/nyfed-research/_manifest.json
                 re-mirrored conditionally from its recorded
                 source_url (the 4757/4758 sweep IS the refresh map);
             (c) light re-harvest of the five seed pages (sce/hhdc/
                 dsge/datahub/research-data) appends NEW first-party
                 data files to the manifest, capped per run.
             Parsed haircuts-series/ stays a phase-2 re-transform,
             flagged beside bsrm's 500 series in refresh-orphans.

Conditional fetch: upstream ETag/Length vs stored object metadata --
unchanged content is skipped, and a tiny _last-check.json is stamped
under each prefix EVERY run so the board's freshness reflects the
truth: the import attempt happened (an unchanged quarterly source is
healthy, not stale). Daily Scheduler.
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
NYF = "https://www.newyorkfed.org"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com",
      "Accept": "*/*"}
STATE_KEY = "data/_state/src-mirror.json"
ORPHANS_KEY = "data/warm/_audit/refresh-orphans.json"
BSRM_FILES = ("ofr_bsrm.xlsx", "ofr_bsrm_international_scores.xlsx")
SITE_PAGES = (FRG + "/", FRG + "/data")
NYR = "data/warm/nyfed-research/"
NYR_MANIFEST = NYR + "_manifest.json"
HAIRCUTS = (
    ("haircuts/tri-party-repo_data_current.xlsx",
     NYF + "/medialibrary/Research/Interactives/Data/"
           "tri-party-repo/tri-party-repo_data"),
    ("haircuts/tri-party-repo_preNov25_history.xlsx",
     NYF + "/medialibrary/Research/Interactives/Data/"
           "tri-party-repo/tri-party-repo-preNov25_data"),
)
NYF_SEEDS = ("/microeconomics/sce", "/microeconomics/hhdc",
             "/research/policy/dsge", "/markets/data-hub",
             "/research/data_indicators")
NYF_EXCLUDE = ("google-analytics", "dap.digitalgov", "youtube",
               "twitter", "facebook", "linkedin", "doubleclick")

s3 = boto3.client("s3", region_name="us-east-1")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_json(key, default=None):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        return json.loads(raw)
    except Exception:
        return default


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

    # lane 3: nyfed-research (ops 4953 -- the last audit orphan) -----
    n_ = {"haircuts": {}, "mirrored": 0, "fresh": 0, "unchanged": 0,
          "errors": 0, "new_harvested": 0}
    for rel, url in HAIRCUTS:
        st, nb = _mirror(url, NYR + rel)
        n_["haircuts"][rel.rsplit("/", 1)[-1]] = {"status": st,
                                                  "bytes": nb}
    man = _get_json(NYR_MANIFEST) or {"files": {}}
    files = man.get("files") or {}
    for rel, meta in sorted(files.items()):
        if time.time() - t0 > 680:
            break
        src = (meta or {}).get("source_url")
        if not src or "newyorkfed.org" not in src:
            continue
        st, nb = _mirror(src, NYR + rel)
        n_["mirrored"] += 1
        if st == "fresh":
            n_["fresh"] += 1
        elif st == "unchanged":
            n_["unchanged"] += 1
        else:
            n_["errors"] += 1
    # light re-harvest: NEW first-party data files -> manifest
    if time.time() - t0 < 640:
        known = set(files)
        for sp in NYF_SEEDS:
            if n_["new_harvested"] >= 12 or time.time() - t0 > 680:
                break
            try:
                body = _page(NYF + sp)
            except Exception:
                continue
            for m in re.findall(
                    r'href="([^"]+\.(?:csv|xlsx|xls|json|zip))"',
                    body, re.I):
                u = m if m.startswith("http") else NYF + (
                    m if m.startswith("/") else "/" + m)
                if "newyorkfed.org" not in u or \
                        any(x in u for x in NYF_EXCLUDE):
                    continue
                rel = (sp.strip("/").split("/")[-1] + "/" +
                       re.sub(r"[^A-Za-z0-9._-]+", "_",
                              u.split("newyorkfed.org", 1)[-1]
                              .strip("/"))[:110])
                if rel in known:
                    continue
                st, nb = _mirror(u, NYR + rel)
                if st == "fresh":
                    files[rel] = {"source_url": u, "bytes": nb,
                                  "added_by": "src-mirror",
                                  "added_at": _now()}
                    known.add(rel)
                    n_["new_harvested"] += 1
                if n_["new_harvested"] >= 12:
                    break
        if n_["new_harvested"]:
            man["files"] = files
            man["extended_at"] = _now()
            _put_json(NYR_MANIFEST, man)
    _put_json(NYR + "_last-check.json",
              {"at": _now(), "engine": "src-mirror",
               "sources": len(files) + len(HAIRCUTS), **n_})
    res["lanes"]["nyfed-research"] = n_

    # phase-2 re-transforms: stated, never silent ---------------------
    _put_json(ORPHANS_KEY, {
        "as_of": _now(), "engine": "src-mirror",
        # bsrm-truth ops 4966: the "500 parsed bsrm series" ledger
        # entry was a FICTION -- ops 4753's own report shows
        # data/warm/ofr-bsrm/series/ is an accidental duplicate of
        # ofr-hfm (ops 4752 bug), flagged in-bucket by
        # _DUPLICATE_NOTE.json; canonical hfm has its own live
        # engine. No transform is owed.
        "phase2_retransforms": {
            "nyfed-haircuts-series": "parsed tri-party haircut "
                                     "series (seed ops 4793-94) not "
                                     "re-derived from the mirrored "
                                     "workbooks yet"},
        "closed": {
            "ofr-bsrm-series": "no transform owed: series/ = "
                               "flagged duplicate of ofr-hfm "
                               "(ops 4752 bug, 4753 _DUPLICATE_"
                               "NOTE); canonical ofr-hfm live"}})

    _put_json(STATE_KEY, {"as_of": _now(), "ok": True,
                          "summary": res,
                          "elapsed_s": round(time.time() - t0, 1)})
    out = {"ok": True, **res,
           "elapsed_s": round(time.time() - t0, 1)}
    print(json.dumps(out)[:800])
    return {"statusCode": 200, "body": json.dumps(out)}
