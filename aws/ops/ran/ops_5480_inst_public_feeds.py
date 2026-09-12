"""ops_5480 -- bank free institutional feeds (FRED/ALFRED + public CSV) into warehouse.

Full history under data/warm/inst-public/. Hot join data/inst-public-join.json.
No paid APIs. FRED key from SSM only.
"""
from __future__ import annotations

import gzip
import json
import sys
import time
import urllib.error
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

# FRED ids that ARE the free institutional tape. Full observation history.
FRED_SERIES = [
    ("GDPNOW", "atlanta_gdpnow"),
    ("PAYEMS", "bls_payrolls"),
    ("CPIAUCSL", "bls_cpi"),
    ("PCEPI", "bea_pce"),
    ("ADSINDEX", "philly_ads"),
    ("T10Y3M", "curve_10y3m"),
    ("T10Y2Y", "curve_10y2y"),
    ("SOFR", "nyfed_sofr"),
    ("RRPONTSYD", "onrrp"),
    ("WDTGAL", "tga"),
    ("WALCL", "fed_bs"),
    ("DTWEXBGS", "dollar_broad"),
    ("BAMLH0A0HYM2", "hy_oas"),
    ("BAMLC0A0CM", "ig_oas"),
    ("VIXCLS", "vix"),
    ("ANFCI", "chicago_nfci"),
    ("NFCI", "chicago_nfci_headline"),
    ("UNRATE", "unemployment"),
    ("ICSA", "jobless_claims"),
    ("M2SL", "m2"),
    ("BOGMBASE", "base_money"),
    ("DGS10", "ust_10y"),
    ("DGS2", "ust_2y"),
    ("DCOILWTICO", "eia_wti"),
    ("DHHNGSP", "eia_hh_gas"),
    ("WUAA", "usda_wheat_proxy_fred"),
    ("TREAST", "fed_ust_holdings"),
    ("WLCFLPCL", "bank_loans_proxy"),
    ("IEABC", "current_account"),
    ("BOPGSTB", "trade_balance"),
    ("GFDEGDQ188S", "debt_to_gdp"),
    ("FRBATLWGT12MMTH", "atlanta_wage_tracker"),
    ("MEDCPIM158SFRBCLE", "cleveland_median_cpi"),
    ("EXPINF1YR", "cleveland_exp_inf_1y"),
    ("EXPINF10YR", "cleveland_exp_inf_10y"),
    ("T5YIE", "breakeven_5y"),
    ("T10YIE", "breakeven_10y"),
]

# ALFRED vintages for revision triangles (first print vs latest)
ALFRED_SERIES = ["GDP", "PAYEMS", "CPIAUCSL", "PCEPI", "UNRATE"]

PUBLIC_CSV = [
    (
        "sf_news_sentiment",
        "https://www.frbsf.org/-/media/project/frbsf/frbsf-edu/files/research-and-insights/data/news-sentiment-data/news_sentiment_data.csv",
    ),
]


def _now():
    return datetime.now(timezone.utc).isoformat()


def _get(url, timeout=60):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodlOps/5480"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _fred_key(ssm):
    try:
        return ssm.get_parameter(Name="/justhodl/fred-api-key", WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return ""


def _put_gz(s3, key, obj):
    raw = json.dumps(obj, default=str).encode("utf-8")
    s3.put_object(
        Bucket=B,
        Key=key,
        Body=gzip.compress(raw),
        ContentType="application/json",
        ContentEncoding="gzip",
    )
    return len(raw)


def main():
    with report("ops_5480_inst_public_feeds") as R:
        R.heading("ops 5480 free institutional public feeds")
        s3 = boto3.client("s3", region_name="us-east-1")
        ssm = boto3.client("ssm", region_name="us-east-1")
        key = _fred_key(ssm)
        if not key:
            R.fail("no FRED key in SSM /justhodl/fred-api-key")
            sys.exit(1)
        feeds = []
        for sid, slug in FRED_SERIES:
            url = (
                "https://api.stlouisfed.org/fred/series/observations"
                "?series_id=%s&api_key=%s&file_type=json&sort_order=asc"
                % (sid, key)
            )
            try:
                doc = json.loads(_get(url))
                obs = [o for o in (doc.get("observations") or []) if o.get("value") not in (".", "")]
                payload = {
                    "series_id": sid,
                    "slug": slug,
                    "n": len(obs),
                    "start": obs[0]["date"] if obs else None,
                    "end": obs[-1]["date"] if obs else None,
                    "last": obs[-1] if obs else None,
                    "observations": obs,
                    "source": "fred",
                    "fetched_at": _now(),
                }
                nbytes = _put_gz(s3, WARM + "fred/%s.json.gz" % sid, payload)
                feeds.append({
                    "id": sid, "slug": slug, "n": len(obs),
                    "start": payload["start"], "end": payload["end"],
                    "last": payload["last"], "bytes": nbytes, "status": "LIVE",
                })
                R.ok("FRED %s n=%s %s..%s" % (sid, len(obs), payload["start"], payload["end"]))
            except Exception as e:
                feeds.append({"id": sid, "slug": slug, "status": "FAIL", "error": str(e)[:180]})
                R.warn("FRED %s %s" % (sid, e))
            time.sleep(0.6)

        for sid in ALFRED_SERIES:
            try:
                vurl = (
                    "https://api.stlouisfed.org/fred/series/vintagedates"
                    "?series_id=%s&api_key=%s&file_type=json"
                    % (sid, key)
                )
                vdoc = json.loads(_get(vurl))
                dates = [x.get("vintage_date") for x in (vdoc.get("vintage_dates") or []) if x.get("vintage_date")]
                # last 24 vintages — full cube is enormous; history of dates is stored
                tail = dates[-24:]
                vintages = []
                for vd in tail:
                    ourl = (
                        "https://api.stlouisfed.org/fred/series/observations"
                        "?series_id=%s&api_key=%s&file_type=json&vintage_dates=%s&sort_order=desc&limit=1"
                        % (sid, key, vd)
                    )
                    odoc = json.loads(_get(ourl))
                    obs = odoc.get("observations") or []
                    if obs:
                        vintages.append({"vintage_date": vd, "as_of": obs[0].get("date"), "value": obs[0].get("value")})
                    time.sleep(0.4)
                payload = {
                    "series_id": sid,
                    "n_vintages_available": len(dates),
                    "n_vintages_banked": len(vintages),
                    "first_vintage": dates[0] if dates else None,
                    "last_vintage": dates[-1] if dates else None,
                    "vintages": vintages,
                    "source": "alfred",
                    "fetched_at": _now(),
                }
                _put_gz(s3, WARM + "alfred/%s.json.gz" % sid, payload)
                feeds.append({
                    "id": "ALFRED-" + sid, "slug": "alfred_" + sid.lower(),
                    "n": len(vintages), "n_available": len(dates),
                    "end": dates[-1] if dates else None, "status": "LIVE",
                })
                R.ok("ALFRED %s available=%s banked_tail=%s" % (sid, len(dates), len(vintages)))
            except Exception as e:
                feeds.append({"id": "ALFRED-" + sid, "status": "FAIL", "error": str(e)[:180]})
                R.warn("ALFRED %s %s" % (sid, e))

        for slug, url in PUBLIC_CSV:
            try:
                raw = _get(url, timeout=90)
                s3.put_object(
                    Bucket=B,
                    Key=WARM + "csv/%s.csv.gz" % slug,
                    Body=gzip.compress(raw),
                    ContentType="text/csv",
                    ContentEncoding="gzip",
                )
                feeds.append({"id": slug, "slug": slug, "bytes": len(raw), "status": "LIVE", "source": url})
                R.ok("CSV %s bytes=%s" % (slug, len(raw)))
            except Exception as e:
                feeds.append({"id": slug, "status": "FAIL", "error": str(e)[:180], "source": url})
                R.warn("CSV %s %s" % (slug, e))

        live = [f for f in feeds if f.get("status") == "LIVE"]
        fail = [f for f in feeds if f.get("status") != "LIVE"]
        join = {
            "schema": "inst-public-join.v1",
            "source": "ops_5480",
            "generated_at": _now(),
            "n_live": len(live),
            "n_fail": len(fail),
            "feeds": feeds,
            "warm_prefix": WARM,
        }
        s3.put_object(
            Bucket=B,
            Key=HOT,
            Body=json.dumps(join, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("hot %s live=%s fail=%s" % (HOT, len(live), len(fail)))
        if not live:
            R.fail("zero live feeds")
            sys.exit(1)


if __name__ == "__main__":
    main()
