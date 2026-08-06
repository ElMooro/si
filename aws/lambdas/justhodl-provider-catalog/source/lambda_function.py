"""justhodl-provider-catalog — Khalid's Data section (ops 4506).

One engine keeps ~30 dedicated provider pages complete forever: scan the
bucket against a provider registry (prefixes + hot keys), pull per-key
size/age, and enrich with FULL series inventories from each provider's
own state/catalog files (OFR 442 mnemonics, NYFed PD 1,539 ids, FRED
panel ids, BIS/Eurostat/OECD/StatCan flow lists, Treasury datasets, rp op
counts...). Writes data/provider-catalog.json (hub index) +
data/providers/{slug}.json (the complete per-provider manifest the
template page renders). Daily 06:20 + on demand."""
import gzip
import json
import os
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")

REG = {
 "ofr": {"name": "OFR — Office of Financial Research",
  "api": "data.financialresearch.gov/v1",
  "engines": ["justhodl-ofr-stfm", "justhodl-nyfed-repo-deep",
              "justhodl-plumbing-panel", "justhodl-warm-bridge"],
  "prefixes": ["data/warm/ofr/"], "hot": ["data/ofr-funding.json"],
  "series_from": ("data/warm/ofr/state.json", "catalog")},
 "nyfed": {"name": "NY Fed — Markets API",
  "api": "markets.newyorkfed.org/api",
  "engines": ["justhodl-nyfed-markets-full", "justhodl-nyfed-repo-deep",
              "justhodl-plumbing-panel", "fedliquidityapi"],
  "prefixes": ["data/warm/nyfed-markets/"],
  "hot": ["data/soma-holdings.json"],
  "series_from": ("data/warm/nyfed-markets/pd-state.json", "catalog")},
 "treasury": {"name": "US Treasury — FiscalData",
  "api": "api.fiscaldata.treasury.gov",
  "engines": ["justhodl-usgov-direct", "justhodl-warm-bridge"],
  "prefixes": ["data/warm/treasury/"],
  "hot": ["data/treasury-fiscal.json"]},
 "bea": {"name": "BEA — Bureau of Economic Analysis",
  "api": "apps.bea.gov/api", "engines": ["justhodl-usgov-direct"],
  "prefixes": ["data/warm/usgov/bea/"], "hot": ["data/bea-gdp.json"],
  "series_from": ("data/_state/bea-walk.json", "tables")},
 "bls": {"name": "BLS — Bureau of Labor Statistics",
  "api": "api.bls.gov/publicAPI/v2",
  "engines": ["justhodl-usgov-direct", "justhodl-canary-macro"],
  "prefixes": ["data/warm/usgov/bls/",
               "data/warm/fred-canary/bls-labor"],
  "hot": ["data/bls-macro.json"]},
 "fed-board": {"name": "Federal Reserve Board — DDP",
  "api": "federalreserve.gov/datadownload",
  "engines": ["justhodl-usgov-direct"],
  "prefixes": ["data/warm/usgov/ddp/"]},
 "fred": {"name": "FRED — St. Louis Fed",
  "api": "fred.stlouisfed.org",
  "engines": ["justhodl-canary-macro", "many legacy engines"],
  "prefixes": ["data/warm/fred-canary/"],
  "hot": ["data/canary-macro.json"]},
 "sec-edgar": {"name": "SEC EDGAR — filings index",
  "api": "sec.gov/Archives", "engines": ["justhodl-edgar-index",
                                          "justhodl-cusip-map-rebuild"],
  "prefixes": ["data/warm/edgar-filings/"],
  "hot": ["data/13f-cusip-map-v2.json"]},
 "sec-bulk": {"name": "SEC — bulk XBRL",
  "api": "sec.gov (companyfacts/submissions)",
  "engines": ["justhodl-sec-bulk"],
  "prefixes": ["data/warm/sec-bulk/"]},
 "sec-dera": {"name": "SEC DERA — statement datasets",
  "api": "sec.gov/dera", "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/sec-dera/"]},
 "sec-midas": {"name": "SEC MIDAS — market structure",
  "api": "sec.gov/marketstructure",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/sec-midas/"]},
 "polygon": {"name": "Polygon.io — US equities",
  "api": "api.polygon.io", "engines": ["justhodl-polygon-daily"],
  "prefixes": ["data/warm/us-equities-daily/"]},
 "gleif": {"name": "GLEIF — LEI system",
  "api": "gleif.org", "engines": ["justhodl-bis-gleif",
                                   "justhodl-symbology-master"],
  "prefixes": ["data/warm/gleif/"]},
 "bis": {"name": "BIS — Bank for Intl Settlements",
  "api": "stats.bis.org/api/v1",
  "engines": ["justhodl-bis-gleif", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/bis/"],
  "series_from": ("data/warm/bis/catalog.json.gz", "dataflows")},
 "eurostat": {"name": "Eurostat",
  "api": "ec.europa.eu/eurostat SDMX",
  "engines": ["justhodl-eurostat-oecd", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/eurostat/"],
  "series_from": ("data/warm/eurostat/catalog.json.gz",
                  "dataflows")},
 "oecd": {"name": "OECD",
  "api": "sdmx.oecd.org/public/rest",
  "engines": ["justhodl-eurostat-oecd", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/oecd/"],
  "series_from": ("data/warm/oecd/catalog.json.gz", "dataflows")},
 "statcan": {"name": "Statistics Canada",
  "api": "www150.statcan.gc.ca/t1/wds",
  "engines": ["justhodl-global-expansion", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/statcan/"]},
 "banxico": {"name": "Banxico — SIE API",
  "api": "banxico.org.mx/SieAPIRest",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/banxico/"]},
 "boe": {"name": "Bank of England — IADB",
  "api": "bankofengland.co.uk/boeapps/database",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/boe/"]},
 "gdelt": {"name": "GDELT — global events",
  "api": "data.gdeltproject.org/gdeltv2",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/gdelt/"]},
 "eiopa": {"name": "EIOPA — Solvency II RFR",
  "api": "eiopa.europa.eu", "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/eiopa/"]},
 "nasa": {"name": "NASA POWER — ag weather",
  "api": "power.larc.nasa.gov",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/nasa-power/"]},
 "dol": {"name": "US DOL — ETA claims",
  "api": "oui.doleta.gov", "engines": ["justhodl-canary-macro"],
  "prefixes": ["data/warm/fred-canary/dol-"]},
 "chicagofed": {"name": "Chicago Fed — NFCI",
  "api": "chicagofed.org", "engines": ["justhodl-plumbing-panel"],
  "prefixes": ["data/warm/chicagofed/"]},
 "clevelandfed": {"name": "Cleveland Fed — yield-curve model",
  "api": "clevelandfed.org", "engines": ["justhodl-canary-macro"],
  "prefixes": ["data/warm/fred-canary/cleveland-"]},
 "atlantafed": {"name": "Atlanta Fed — GDPNow",
  "api": "atlantafed.org", "engines": ["justhodl-canary-macro"],
  "prefixes": ["data/warm/fred-canary/atlanta-"]},
 "occ": {"name": "OCC — options volume",
  "api": "theocc.com", "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/occ/"]},
 "cftc": {"name": "CFTC — futures positioning",
  "api": "publicreporting.cftc.gov",
  "engines": ["cftc-futures-positioning-agent",
              "justhodl-cftc-deep"],
  "prefixes": ["data/warm/cftc/"]},
 "ecb": {"name": "ECB — SDMX",
  "api": "data-api.ecb.europa.eu",
  "engines": ["justhodl-ecb-catalog"],
  "prefixes": ["data/warm/ecb/"]},
}


def _get_json(key):
    b = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz"):
        b = gzip.decompress(b)
    return json.loads(b)


def _series_list(spec):
    if not spec:
        return None
    key, field = spec
    try:
        d = _get_json(key)
        v = d.get(field)
        if isinstance(v, list):
            ids = [(x.get("id") if isinstance(x, dict) else x)
                   for x in v]
            ids = [str(i) for i in ids if i]
            return {"count": len(ids),
                    "ids": ids if len(ids) <= 2000 else
                    ids[:400] + ["…+" + str(len(ids) - 400) + " more"]}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:50]}"}
    return None


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    hub = {"as_of": now.isoformat(timespec="seconds"), "providers": []}
    for slug, r in REG.items():
        keys = []
        tot = 0
        for pref in r.get("prefixes", []):
            tok = None
            while True:
                kw = {"Bucket": BUCKET, "Prefix": pref,
                      "MaxKeys": 400}
                if tok:
                    kw["ContinuationToken"] = tok
                resp = s3.list_objects_v2(**kw)
                for o in resp.get("Contents", []):
                    age = round((now - o["LastModified"])
                                .total_seconds() / 3600, 1)
                    keys.append({"key": o["Key"],
                                 "bytes": o["Size"],
                                 "age_h": age})
                    tot += o["Size"]
                if not resp.get("IsTruncated"):
                    break
                tok = resp.get("NextContinuationToken")
        for hk in r.get("hot", []):
            try:
                h = s3.head_object(Bucket=BUCKET, Key=hk)
                keys.append({"key": hk, "bytes": h["ContentLength"],
                             "age_h": round(
                                 (now - h["LastModified"])
                                 .total_seconds() / 3600, 1),
                             "hot": True})
                tot += h["ContentLength"]
            except Exception:
                keys.append({"key": hk, "missing": True,
                             "hot": True})
        keys.sort(key=lambda x: x.get("bytes", 0), reverse=True)
        ser = _series_list(r.get("series_from"))
        doc = {"slug": slug, "name": r["name"], "api": r["api"],
               "engines": r["engines"],
               "as_of": hub["as_of"],
               "n_keys": len([k for k in keys
                              if not k.get("missing")]),
               "total_bytes": tot,
               "total_mb": round(tot / 1e6, 2),
               "freshest_h": min([k["age_h"] for k in keys
                                  if "age_h" in k] or [None]),
               "series": ser,
               "keys": keys[:600]}
        s3.put_object(Bucket=BUCKET,
                      Key=f"data/providers/{slug}.json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        hub["providers"].append(
            {"slug": slug, "name": r["name"], "api": r["api"],
             "n_keys": doc["n_keys"], "total_mb": doc["total_mb"],
             "series_count": (ser or {}).get("count"),
             "freshest_h": doc["freshest_h"]})
    hub["providers"].sort(key=lambda p: -(p["total_mb"] or 0))
    hub["totals"] = {"providers": len(hub["providers"]),
                     "keys": sum(p["n_keys"]
                                 for p in hub["providers"]),
                     "gb": round(sum(p["total_mb"]
                                     for p in hub["providers"])
                                 / 1000, 2)}
    s3.put_object(Bucket=BUCKET, Key="data/provider-catalog.json",
                  Body=json.dumps(hub, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    res = {"ok": True, **hub["totals"]}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
