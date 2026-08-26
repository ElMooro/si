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

ENGINE_VERSION = "justhodl-boj-full v1.1.0 ops4987 api-universe"
# v1.1.0: API lane -- the FULL BOJ series universe via the cracked
# api/v1 (gov-sources spec): getMetadata?db=X lists every series
# code in a database; getDataCode pulls values full-window with
# comma-batched codes. DB candidates probe-validated, invalid
# named. Ops sync-drives to completion (AWS terminated
# self-chains fleet-wide).
API = "https://www.stat-search.boj.or.jp/api/v1/"
DB_CANDS = ["MD01", "MD02", "MD11", "MD12", "MD13", "CO", "IR01",
            "IR02", "IR03", "IR04", "FM01", "FM02", "FM03",
            "FM08", "BP01", "BP02", "PR01", "PR02", "PR03",
            "BS01", "BS02", "DL01", "FF", "FFYS", "TK", "ST01",
            "PS01", "CA01", "LA01", "SK01"]
BATCH = 40
API_BUDGET_GUARD = 60
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


def api_fetch(path, timeout=60, cap=40_000_000):
    req = urllib.request.Request(
        API + path, headers={**UA,
                             "Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(cap)


def api_discover(state, t0):
    ap = state.setdefault("api", {"dbs": {}, "invalid": {}})
    for db in DB_CANDS:
        if db in ap["dbs"] or db in ap["invalid"]:
            continue
        if time.time() - t0 > BUDGET_S - API_BUDGET_GUARD:
            return
        try:
            raw = api_fetch("getMetadata?format=json&lang=en&db="
                            + db, cap=20_000_000)
            js = json.loads(raw)
            codes = []

            def walk(o):
                if isinstance(o, dict):
                    for k, v in o.items():
                        if k.upper() in ("SERIES_CODE", "CODE")                                 and isinstance(v, str) and                                 len(v) >= 4:
                            codes.append(v)
                        else:
                            walk(v)
                elif isinstance(o, list):
                    for it in o:
                        walk(it)
            walk(js)
            codes = sorted(set(codes))
            if not codes:
                ap["invalid"][db] = "no-codes head=%r" %                     raw[:60].decode("utf-8", "replace")
            else:
                ap["dbs"][db] = {"codes": codes, "done": 0,
                                 "rows": 0, "parts": 0}
        except urllib.error.HTTPError as e:
            ap["invalid"][db] = "HTTP %s" % e.code
        except Exception as e:
            ap["invalid"][db] = str(e)[:70]
        time.sleep(0.4)


def api_drain(state, t0):
    ap = state.get("api") or {}
    for db, meta in sorted((ap.get("dbs") or {}).items()):
        codes = meta.get("codes") or []
        while meta["done"] < len(codes):
            if time.time() - t0 > BUDGET_S - API_BUDGET_GUARD:
                return
            batch = codes[meta["done"]:meta["done"] + BATCH]
            try:
                raw = api_fetch(
                    "getDataCode?format=json&lang=en&db=%s"
                    "&startDate=190001&endDate=209912&code=%s"
                    % (db, ",".join(batch)), timeout=90)
                json.loads(raw)          # validity check
                meta["parts"] += 1
                s3.put_object(
                    Bucket=BUCKET,
                    Key=ROOT + "api/%s/part%04d.json.gz" % (
                        db, meta["parts"]),
                    Body=__import__("gzip").compress(raw),
                    ContentType="application/gzip",
                    Metadata={"engine": "boj-full", "db": db,
                              "codes": str(len(batch))})
                meta["rows"] += raw.count(b"SURVEY_DATES")
                meta["done"] += len(batch)
                state["failures"].pop("api:" + db, None)
            except Exception as e:
                state["failures"]["api:" + db] =                     "@%d %s" % (meta["done"], str(e)[:70])
                meta["done"] += len(batch)   # skip bad batch
            _put(STATE_KEY, state)
            time.sleep(0.35)


def lambda_handler(event, ctx=None):
    t0 = time.time()
    state = _j(STATE_KEY, None) or {"version": "1.1.0",
                                    "zips": {}, "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put(STATE_KEY, state)
    if event.get("api_only"):
        api_discover(state, t0)
        api_drain(state, t0)
        ap = state.get("api") or {}
        state["lease_until"] = 0
        state["as_of"] = _now()
        _put(STATE_KEY, state)
        write_manifest(state)
        return {"ok": True, "mode": "api",
                "dbs": len((ap.get("dbs") or {})),
                "invalid": len((ap.get("invalid") or {})),
                "series_done": sum(m.get("done", 0) for m in
                                   (ap.get("dbs") or {}).values()),
                "elapsed_s": round(time.time() - t0, 1)}
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
    api_discover(state, t0)
    api_drain(state, t0)
    state["lease_until"] = 0
    state["as_of"] = _now()
    _put(STATE_KEY, state)
    write_manifest(state)
    ok = {k: v for k, v in state["zips"].items() if v.get("ok")}
    return {"ok": True, "zips": len(ok),
            "universe": state.get("universe"),
            "api_dbs": len(((state.get("api") or {})
                            .get("dbs") or {})),
            "failures": len(state["failures"]),
            "elapsed_s": round(time.time() - t0, 1)}


def write_manifest(state):
    zl = state.get("zips") or {}
    ok = {k: v for k, v in zl.items() if v.get("ok")}
    ap = state.get("api") or {}
    dbs = ap.get("dbs") or {}
    _put(MANIFEST_KEY, {
        "as_of": state["as_of"], "engine": "justhodl-boj-full",
        "version": "1.0.0", "zips": len(ok),
        "universe": state.get("universe"),
        "mb": round(sum(v.get("bytes") or 0
                        for v in ok.values()) / 1e6, 1),
        "failures": len(state["failures"]),
        "api_dbs": len(dbs),
        "api_invalid": len(ap.get("invalid") or {}),
        "api_series": sum(len(m.get("codes") or [])
                          for m in dbs.values()),
        "api_series_done": sum(m.get("done", 0)
                               for m in dbs.values()),
        "api_parts": sum(m.get("parts", 0)
                         for m in dbs.values()),
        "note": ("BOJ full warehouse: portal flat-file zips + "
                 "API universe (every series per db via "
                 "getMetadata, full-window values via batched "
                 "getDataCode); invalid dbs named")})
