"""justhodl-asia-trade-full v1.0.0 -- HK + Chile + Korea trade/
industry warehouse (Khalid: manufacturing, exports, ports,
imports, industrial for data.html).

  HK   data.gov.hk CKAN (keyless): package_search per theme ->
       mirror the top dataset resources (CSV/JSON/XLSX) verbatim
  CL   datos.gob.cl CKAN (keyless): same doctrine, Spanish terms
  KR   Bank of Korea ECOS needs a key -- vault item provider=ecos
       (or ECOS_API_KEY env) unlocks the full StatisticSearch
       drain; until then the lane states its status honestly
       (never fails the run)
data/warm/asia-trade/{hk,cl,kr}/ · themes fail NAMED · rate(24h)
"""
import gzip
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-asia-trade-full v1.0.0 ops4989"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
ROOT = "data/warm/asia-trade/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "Mozilla/5.0 JustHodl Research "
      "(raafouis@gmail.com)"}
BUDGET_S = int(os.environ.get("AT_BUDGET_S", "640"))
PER_THEME = 4
RES_CAP = 80_000_000

CKAN = {
    "hk": {"base": "https://data.gov.hk/en-data/api/3/action/",
           "themes": ["container throughput", "port cargo",
                      "merchandise trade", "industrial production",
                      "manufacturing", "imports exports"]},
    "cl": {"base": "https://datos.gob.cl/api/3/action/",
           "themes": ["exportaciones", "importaciones",
                      "manufactura", "puertos",
                      "produccion industrial",
                      "comercio exterior"]},
}
ECOS_KEY = os.environ.get("ECOS_API_KEY", "")

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


def fetch(url, timeout=90, cap=RES_CAP):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(cap)


def slug(x):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", x)[:110]


def ckan_lane(cc, state, t0):
    cfg = CKAN[cc]
    st = state.setdefault(cc, {"res": {}, "themes": {}})
    for theme in cfg["themes"]:
        if time.time() - t0 > BUDGET_S - 90:
            return
        tkey = theme.replace(" ", "-")
        try:
            raw = fetch(cfg["base"] + "package_search?rows=%d&q=%s"
                        % (PER_THEME, urllib.parse.quote(theme)),
                        cap=6_000_000)
            js = json.loads(raw)
            pkgs = (js.get("result") or {}).get("results") or []
            st["themes"][tkey] = {"hits": len(pkgs), "at": _now()}
            state["failures"].pop("%s:%s" % (cc, tkey), None)
        except Exception as e:
            state["failures"]["%s:%s" % (cc, tkey)] = str(e)[:80]
            continue
        for pkg in pkgs:
            pname = slug(pkg.get("name") or pkg.get("id") or "")
            for res in (pkg.get("resources") or [])[:3]:
                if time.time() - t0 > BUDGET_S - 60:
                    return
                fmt = (res.get("format") or "").lower()
                url = res.get("url") or ""
                if fmt not in ("csv", "json", "xlsx", "xls") or \
                        not url.startswith("http"):
                    continue
                rid = slug("%s__%s.%s" % (
                    pname, res.get("id") or
                    slug(res.get("name") or "r"), fmt))
                prev = st["res"].get(rid) or {}
                if prev.get("ok") and \
                        time.time() - float(prev.get("epoch")
                                            or 0) < 23 * 3600:
                    continue
                try:
                    body = fetch(url, timeout=120)
                    if len(body) < 200:
                        raise RuntimeError("thin %dB" % len(body))
                    dig = hashlib.sha256(body).hexdigest()[:16]
                    if prev.get("sha") != dig:
                        s3.put_object(
                            Bucket=BUCKET,
                            Key=ROOT + "%s/%s.gz" % (cc, rid),
                            Body=gzip.compress(body),
                            ContentType="application/gzip",
                            Metadata={"engine": "asia-trade",
                                      "theme": tkey[:60],
                                      "src": url[:110]})
                    st["res"][rid] = {
                        "ok": True, "bytes": len(body),
                        "sha": dig, "theme": tkey,
                        "epoch": time.time(), "at": _now()}
                    state["failures"].pop(
                        "%s:%s" % (cc, rid), None)
                except Exception as e:
                    st["res"][rid] = {"ok": False,
                                      "epoch": time.time()}
                    state["failures"]["%s:%s" % (cc, rid)] = \
                        str(e)[:70]
                time.sleep(0.3)


def kr_lane(state, t0):
    st = state.setdefault("kr", {"res": {}, "status": ""})
    key = ECOS_KEY
    if not key:
        try:
            ddb = boto3.client("dynamodb",
                               region_name="us-east-1")
            it = ddb.get_item(
                TableName="justhodl-api-keys",
                Key={"key_hash": {"S": "ecos"}}).get("Item") or {}
            key = (it.get("api_key") or it.get("key") or
                   {}).get("S") or ""
        except Exception:
            pass
    if not key:
        st["status"] = ("awaiting ECOS key (vault item "
                        "key_hash=ecos, attr api_key) -- Bank of "
                        "Korea StatisticSearch drains the moment "
                        "it lands")
        return
    st["status"] = "key present"
    for code, nm in [("901Y009", "industrial-production"),
                     ("403Y001", "exports-imports"),
                     ("901Y011", "manufacturing-shipments")]:
        if time.time() - t0 > BUDGET_S - 60:
            return
        try:
            url = ("https://ecos.bok.or.kr/api/StatisticSearch/"
                   "%s/json/en/1/100000/%s/M/195001/210012"
                   % (key, code))
            body = fetch(url, timeout=120, cap=60_000_000)
            if b"StatisticSearch" not in body:
                raise RuntimeError("shape %r" % body[:60])
            s3.put_object(
                Bucket=BUCKET,
                Key=ROOT + "kr/%s__%s.json.gz" % (code, nm),
                Body=gzip.compress(body),
                ContentType="application/gzip",
                Metadata={"engine": "asia-trade", "code": code})
            st["res"][code] = {"ok": True, "bytes": len(body),
                               "name": nm, "at": _now()}
            state["failures"].pop("kr:" + code, None)
        except Exception as e:
            st["res"][code] = {"ok": False}
            state["failures"]["kr:" + code] = str(e)[:80]
        time.sleep(0.4)


def lambda_handler(event, ctx=None):
    t0 = time.time()
    state = _j(STATE_KEY, None) or {"version": "1.0.0",
                                    "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put(STATE_KEY, state)
    ckan_lane("hk", state, t0)
    ckan_lane("cl", state, t0)
    kr_lane(state, t0)
    state["lease_until"] = 0
    state["as_of"] = _now()
    _put(STATE_KEY, state)

    def okn(cc):
        return sum(1 for v in (state.get(cc, {}).get("res")
                               or {}).values() if v.get("ok"))
    _put(MANIFEST_KEY, {
        "as_of": state["as_of"],
        "engine": "justhodl-asia-trade-full",
        "version": "1.0.0",
        "hk_resources": okn("hk"), "cl_resources": okn("cl"),
        "kr_series": okn("kr"),
        "kr_status": state.get("kr", {}).get("status"),
        "mb": round(sum(
            v.get("bytes") or 0
            for cc in ("hk", "cl", "kr")
            for v in (state.get(cc, {}).get("res")
                      or {}).values() if v.get("ok")) / 1e6, 1),
        "failures": len(state.get("failures") or {}),
        "note": ("HK+Chile trade/industry open-data mirror "
                 "(manufacturing, exports, imports, ports, "
                 "industrial) via CKAN theme harvest; Korea "
                 "ECOS drains when its key lands")})
    return {"ok": True, "hk": okn("hk"), "cl": okn("cl"),
            "kr": okn("kr"),
            "failures": len(state.get("failures") or {}),
            "elapsed_s": round(time.time() - t0, 1)}
