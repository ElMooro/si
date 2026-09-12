"""ops_5482 -- FINRA warehouse surface, ALFRED with backoff, FiscalData v2 TIC."""
from __future__ import annotations

import gzip
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
WARM = "data/warm/inst-public/"
HOT = "data/inst-public-join.json"
FINRA_PREFIXES = ["data/warm/finra/", "data/warm/finra-full/", "data/finra/"]
TIC_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny?sort=-record_date&page[size]=100"
ALFRED = ["GDP", "PAYEMS", "CPIAUCSL", "PCEPI", "UNRATE"]


def _now():
    return datetime.now(timezone.utc).isoformat()


def _get(url, timeout=90):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodlOps/5482"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _put_gz_json(s3, key, obj):
    raw = json.dumps(obj, default=str).encode("utf-8")
    s3.put_object(Bucket=B, Key=key, Body=gzip.compress(raw), ContentType="application/json", ContentEncoding="gzip")


def main():
    with report("ops_5482_finra_alfred_tic") as R:
        R.heading("ops 5482")
        s3 = boto3.client("s3", region_name="us-east-1")
        ssm = boto3.client("ssm", region_name="us-east-1")
        key = ssm.get_parameter(Name="/justhodl/fred-api-key", WithDecryption=True)["Parameter"]["Value"]
        new = []

        finra_rows = []
        for pref in FINRA_PREFIXES:
            token = None
            n = 0
            newest = None
            while True:
                kw = {"Bucket": B, "Prefix": pref, "MaxKeys": 100}
                if token:
                    kw["ContinuationToken"] = token
                resp = s3.list_objects_v2(**kw)
                for obj in resp.get("Contents") or []:
                    n += 1
                    if newest is None or obj["LastModified"] > newest["LastModified"]:
                        newest = obj
                if not resp.get("IsTruncated"):
                    break
                token = resp.get("NextContinuationToken")
                if n > 5000:
                    break
            finra_rows.append({
                "prefix": pref, "n_listed": n,
                "newest_key": newest["Key"] if newest else None,
                "newest_lm": newest["LastModified"].astimezone(timezone.utc).isoformat() if newest else None,
            })
            R.ok("FINRA %s n=%s newest=%s" % (pref, n, finra_rows[-1]["newest_key"]))
        surface = {"source": "ops_5482", "generated_at": _now(), "prefixes": finra_rows}
        s3.put_object(Bucket=B, Key="data/finra-surface.json", Body=json.dumps(surface, default=str).encode("utf-8"), ContentType="application/json")
        new.append({"id": "finra_surface", "n": sum(r["n_listed"] for r in finra_rows), "status": "LIVE" if any(r["n_listed"] for r in finra_rows) else "EMPTY"})

        try:
            raw = _get(TIC_URL)
            s3.put_object(Bucket=B, Key=WARM + "json/tic_debt_to_penny.json.gz", Body=gzip.compress(raw),
                          ContentType="application/json", ContentEncoding="gzip")
            doc = json.loads(raw)
            new.append({"id": "tic_debt_to_penny", "n": len(doc.get("data") or []), "bytes": len(raw), "status": "LIVE"})
            R.ok("TIC v2 n=%s" % len(doc.get("data") or []))
        except Exception as e:
            new.append({"id": "tic_debt_to_penny", "status": "FAIL", "error": str(e)[:180]})
            R.warn("TIC %s" % e)

        time.sleep(8)
        for sid in ALFRED:
            try:
                vurl = "https://api.stlouisfed.org/fred/series/vintagedates?series_id=%s&api_key=%s&file_type=json" % (sid, key)
                vdoc = json.loads(_get(vurl))
                raw_v = vdoc.get("vintage_dates") or []
                dates = [x if isinstance(x, str) else (x.get("vintage_date") if isinstance(x, dict) else None) for x in raw_v]
                dates = [d for d in dates if d]
                tail = dates[-12:]
                vintages = []
                for vd in tail:
                    ourl = ("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s"
                            "&file_type=json&vintage_dates=%s&sort_order=desc&limit=1" % (sid, key, vd))
                    try:
                        odoc = json.loads(_get(ourl))
                        obs = odoc.get("observations") or []
                        if obs:
                            vintages.append({"vintage_date": vd, "as_of": obs[0].get("date"), "value": obs[0].get("value")})
                    except Exception as ie:
                        R.warn("ALFRED %s %s %s" % (sid, vd, ie))
                    time.sleep(1.2)
                payload = {"series_id": sid, "n_vintages_available": len(dates), "n_vintages_banked": len(vintages),
                           "vintages": vintages, "source": "alfred", "fetched_at": _now()}
                _put_gz_json(s3, WARM + "alfred/%s.json.gz" % sid, payload)
                new.append({"id": "ALFRED-" + sid, "n": len(vintages), "n_available": len(dates),
                            "status": "LIVE" if vintages else "FAIL"})
                R.ok("ALFRED %s avail=%s banked=%s" % (sid, len(dates), len(vintages)))
            except Exception as e:
                new.append({"id": "ALFRED-" + sid, "status": "FAIL", "error": str(e)[:180]})
                R.warn("ALFRED %s %s" % (sid, e))
            time.sleep(2)

        try:
            raw = s3.get_object(Bucket=B, Key=HOT)["Body"].read()
            hot = json.loads(raw)
        except Exception:
            hot = {"feeds": []}
        by = {f.get("id"): f for f in (hot.get("feeds") or [])}
        for f in new:
            by[f.get("id")] = f
        feeds = list(by.values())
        live = [f for f in feeds if f.get("status") == "LIVE"]
        fail = [f for f in feeds if f.get("status") != "LIVE"]
        out = {"schema": "inst-public-join.v1", "source": "ops_5482", "generated_at": _now(),
               "n_live": len(live), "n_fail": len(fail), "feeds": feeds, "warm_prefix": WARM}
        s3.put_object(Bucket=B, Key=HOT, Body=json.dumps(out, default=str).encode("utf-8"), ContentType="application/json")
        R.ok("hot live=%s fail=%s" % (len(live), len(fail)))


if __name__ == "__main__":
    main()
