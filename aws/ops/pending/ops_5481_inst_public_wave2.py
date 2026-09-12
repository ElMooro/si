"""ops_5481 -- fix 5480 misses + bank TIC / FDIC / FOMC public history."""
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

FRED_RETRY = [
    ("ANFCI", "philly_ads_proxy_anfci"),  # ADS 400'd; keep a daily conditions series labeled
    ("FRBATLWGT", "atlanta_wage_tracker"),
    ("ATLWGT", "atlanta_wage_alt"),
    ("WPU01", "usda_farm_ppi"),
    ("PWHEAMTUSDM", "wheat_usd"),
    ("PCOTTINDUSDM", "cotton_usd"),
    ("PSOILUSDM", "palm_oil_usd"),
    ("WTREGEN", "tic_foreign_ust_official_fred"),
    ("FDHBFIN", "tic_foreign_official_fed"),
    ("FDHBATN", "tic_all_foreign_ust"),
]
ALFRED = ["GDP", "PAYEMS", "CPIAUCSL", "PCEPI", "UNRATE"]
SF_URLS = [
    "https://www.frbsf.org/wp-content/uploads/sites/4/news_sentiment_data.csv",
    "https://www.frbsf.org/research-and-insights/data/daily-news-sentiment-index/files/news_sentiment_data.csv",
    "https://raw.githubusercontent.com/federalreserve/The-Fed-NLP/master/data/news_sentiment.csv",
]
TIC_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/debt_to_penny?sort=-record_date&page[size]=100"
FDIC_URL = "https://banks.data.fdic.gov/api/financials?filters=ACTIVE%3A1&fields=CERT,REPDTE,ASSET,DEP,EQ,NETINC,ROA,ROE,EEFFR&sort_by=REPDTE&sort_order=DESC&limit=10000&format=json"
FOMC_URL = "https://www.federalreserve.gov/json/ne-press.json"


def _now():
    return datetime.now(timezone.utc).isoformat()


def _get(url, timeout=90):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodlOps/5481"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _fred_key(ssm):
    return ssm.get_parameter(Name="/justhodl/fred-api-key", WithDecryption=True)["Parameter"]["Value"]


def _put_gz(s3, key, obj):
    raw = json.dumps(obj, default=str).encode("utf-8")
    s3.put_object(Bucket=B, Key=key, Body=gzip.compress(raw), ContentType="application/json", ContentEncoding="gzip")
    return len(raw)


def _load_hot(s3):
    try:
        b = s3.get_object(Bucket=B, Key=HOT)["Body"].read()
        return json.loads(b)
    except Exception:
        return {"schema": "inst-public-join.v1", "feeds": []}


def main():
    with report("ops_5481_inst_public_wave2") as R:
        R.heading("ops 5481 wave2")
        s3 = boto3.client("s3", region_name="us-east-1")
        ssm = boto3.client("ssm", region_name="us-east-1")
        key = _fred_key(ssm)
        new = []

        for sid, slug in FRED_RETRY:
            url = "https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=asc" % (sid, key)
            try:
                doc = json.loads(_get(url))
                obs = [o for o in (doc.get("observations") or []) if o.get("value") not in (".", "")]
                payload = {"series_id": sid, "slug": slug, "n": len(obs),
                           "start": obs[0]["date"] if obs else None, "end": obs[-1]["date"] if obs else None,
                           "last": obs[-1] if obs else None, "observations": obs, "source": "fred", "fetched_at": _now()}
                _put_gz(s3, WARM + "fred/%s.json.gz" % sid, payload)
                new.append({"id": sid, "slug": slug, "n": len(obs), "start": payload["start"], "end": payload["end"], "last": payload["last"], "status": "LIVE"})
                R.ok("FRED %s n=%s" % (sid, len(obs)))
            except Exception as e:
                new.append({"id": sid, "slug": slug, "status": "FAIL", "error": str(e)[:180]})
                R.warn("FRED %s %s" % (sid, e))
            time.sleep(0.5)

        for sid in ALFRED:
            try:
                vurl = "https://api.stlouisfed.org/fred/series/vintagedates?series_id=%s&api_key=%s&file_type=json" % (sid, key)
                vdoc = json.loads(_get(vurl))
                raw_v = vdoc.get("vintage_dates") or []
                dates = []
                for x in raw_v:
                    if isinstance(x, str):
                        dates.append(x)
                    elif isinstance(x, dict):
                        dates.append(x.get("vintage_date") or x.get("date"))
                dates = [d for d in dates if d]
                tail = dates[-24:]
                vintages = []
                for vd in tail:
                    ourl = ("https://api.stlouisfed.org/fred/series/observations"
                            "?series_id=%s&api_key=%s&file_type=json&vintage_dates=%s&sort_order=desc&limit=1" % (sid, key, vd))
                    odoc = json.loads(_get(ourl))
                    obs = odoc.get("observations") or []
                    if obs:
                        vintages.append({"vintage_date": vd, "as_of": obs[0].get("date"), "value": obs[0].get("value")})
                    time.sleep(0.35)
                payload = {"series_id": sid, "n_vintages_available": len(dates), "n_vintages_banked": len(vintages),
                           "first_vintage": dates[0] if dates else None, "last_vintage": dates[-1] if dates else None,
                           "vintages": vintages, "source": "alfred", "fetched_at": _now()}
                _put_gz(s3, WARM + "alfred/%s.json.gz" % sid, payload)
                new.append({"id": "ALFRED-" + sid, "slug": "alfred_" + sid.lower(), "n": len(vintages),
                            "n_available": len(dates), "end": dates[-1] if dates else None, "status": "LIVE"})
                R.ok("ALFRED %s avail=%s banked=%s" % (sid, len(dates), len(vintages)))
            except Exception as e:
                new.append({"id": "ALFRED-" + sid, "status": "FAIL", "error": str(e)[:180]})
                R.warn("ALFRED %s %s" % (sid, e))

        sf_ok = False
        for url in SF_URLS:
            try:
                raw = _get(url)
                s3.put_object(Bucket=B, Key=WARM + "csv/sf_news_sentiment.csv.gz", Body=gzip.compress(raw),
                              ContentType="text/csv", ContentEncoding="gzip")
                new.append({"id": "sf_news_sentiment", "bytes": len(raw), "status": "LIVE", "source": url})
                R.ok("SF sentiment %s bytes=%s" % (url, len(raw)))
                sf_ok = True
                break
            except Exception as e:
                R.warn("SF %s %s" % (url, e))
        if not sf_ok:
            new.append({"id": "sf_news_sentiment", "status": "FAIL", "error": "all urls missed"})

        for label, url, keyname in (
            ("tic_debt_to_penny", TIC_URL, "treasury_tic"),
            ("fdic_financials_sample", FDIC_URL, "fdic"),
            ("fomc_press", FOMC_URL, "fomc"),
        ):
            try:
                raw = _get(url, timeout=120)
                try:
                    doc = json.loads(raw)
                    s3.put_object(Bucket=B, Key=WARM + "json/%s.json.gz" % label, Body=gzip.compress(raw),
                                  ContentType="application/json", ContentEncoding="gzip")
                    n = None
                    if isinstance(doc, dict):
                        n = len(doc.get("data") or doc.get("financials") or doc.get() or [])
                    elif isinstance(doc, list):
                        n = len(doc)
                    new.append({"id": label, "slug": keyname, "n": n, "bytes": len(raw), "status": "LIVE"})
                    R.ok("%s bytes=%s n=%s" % (label, len(raw), n))
                except Exception:
                    s3.put_object(Bucket=B, Key=WARM + "raw/%s.bin.gz" % label, Body=gzip.compress(raw),
                                  ContentType="application/octet-stream", ContentEncoding="gzip")
                    new.append({"id": label, "bytes": len(raw), "status": "LIVE", "note": "non-json"})
                    R.ok("%s raw bytes=%s" % (label, len(raw)))
            except Exception as e:
                new.append({"id": label, "status": "FAIL", "error": str(e)[:180]})
                R.warn("%s %s" % (label, e))

        hot = _load_hot(s3)
        by = {}
        for f in (hot.get("feeds") or []) + new:
            by[f.get("id")] = f
        feeds = list(by.values())
        live = [f for f in feeds if f.get("status") == "LIVE"]
        fail = [f for f in feeds if f.get("status") != "LIVE"]
        out = {"schema": "inst-public-join.v1", "source": "ops_5481", "generated_at": _now(),
               "n_live": len(live), "n_fail": len(fail), "feeds": feeds, "warm_prefix": WARM}
        s3.put_object(Bucket=B, Key=HOT, Body=json.dumps(out, default=str).encode("utf-8"), ContentType="application/json")
        R.ok("hot live=%s fail=%s" % (len(live), len(fail)))
        if len(live) < 30:
            R.fail("live dropped below 30")
            sys.exit(1)


if __name__ == "__main__":
    main()
