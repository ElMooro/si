"""ops 4650 — STOCK-BUYING engine recon (evidence before build).

Khalid's flagship screener: largest-positive-change framework.
This op maps the composable estate: census store key + columns
(EPS/revenue quarters, shares, margins, ROIC inputs, backlog),
closes depth for SMA/RS/double-bottom, industry-boom scores,
deal-scanner catalysts, FMP estimates/surprises availability.
"""
import json
import os
import urllib.request

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def s3j(k):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=k)["Body"].read())
    except Exception as e:
        return {"__err": str(e)[:70]}


def shp(o, d=0):
    if d > 2:
        return type(o).__name__
    if isinstance(o, dict):
        return {k: shp(v, d + 1) for k, v in list(o.items())[:7]}
    if isinstance(o, list):
        return ["len=%d" % len(o), shp(o[0], d + 1) if o else "-"]
    return str(o)[:24] if isinstance(o, str) else o


def main():
    with report("4650_stock_buying_recon") as r:
        r.heading("ops 4650 — stock-buying recon")

        r.section("census-store discovery")
        pg = s3.list_objects_v2(Bucket=B, Prefix="data/")
        keys = [o["Key"] for o in pg.get("Contents") or []]
        while pg.get("IsTruncated"):
            pg = s3.list_objects_v2(
                Bucket=B, Prefix="data/",
                ContinuationToken=pg["NextContinuationToken"])
            keys += [o["Key"] for o in pg.get("Contents") or []]
        cand = [k for k in keys if any(w in k.lower() for w in
                ("census", "fundamental", "universe", "backlog",
                 "industry-boom", "deal-scan", "estimat",
                 "earnings"))][:24]
        r.log("candidates: %s" % cand)

        r.section("shapes")
        for k in ("data/fundamental-census.json",
                  "data/census.json",
                  "data/industry-boom.json",
                  "data/deal-scanner.json",
                  "data/_ma200/closes.json"):
            doc = s3j(k)
            r.log("%s -> %s" % (k, json.dumps(shp(doc))[:340]))
        for k in cand:
            if "census" in k and k not in (
                    "data/fundamental-census.json",
                    "data/census.json"):
                doc = s3j(k)
                r.log("%s -> %s" % (k,
                                    json.dumps(shp(doc))[:300]))
                break

        r.section("census row columns (first row sample)")
        for k in ("data/fundamental-census.json",
                  "data/census.json") + tuple(
                      x for x in cand if "census" in x)[:2]:
            doc = s3j(k)
            rows = None
            if isinstance(doc, dict):
                for kk in ("rows", "companies", "data",
                           "universe"):
                    if isinstance(doc.get(kk), list) \
                            and doc[kk]:
                        rows = doc[kk]
                        break
            if rows:
                cols = sorted(rows[0].keys())
                r.log("%s: %d rows, %d cols" % (k, len(rows),
                                                len(cols)))
                r.log("cols: %s" % cols[:80])
                r.log("cols2: %s" % cols[80:160])
                break

        r.section("FMP availability")
        fk = os.environ.get("FMP_API_KEY", "") or os.environ.get(
            "FMP_KEY", "")
        r.log("FMP key present: %s (len=%d)" % (bool(fk),
                                                len(fk)))
        if fk:
            for ep in ("earnings-surprises/NVDA",
                       "analyst-estimates/NVDA?period=quarter"
                       "&limit=4"):
                try:
                    url = ("https://financialmodelingprep.com/"
                           "api/v3/%s%sapikey=%s"
                           % (ep, "&" if "?" in ep else "?", fk))
                    req = urllib.request.Request(
                        url, headers={"User-Agent": "ops"})
                    with urllib.request.urlopen(req,
                                                timeout=15) as h:
                        d = json.loads(h.read())
                    r.log("%s -> %s" % (ep.split("?")[0],
                                        json.dumps(shp(d))[:260]))
                except Exception as e:
                    r.log("%s -> ERR %s" % (ep.split("?")[0],
                                            str(e)[:80]))
        r.ok("recon complete — build wires on the shapes above")


if __name__ == "__main__":
    main()
