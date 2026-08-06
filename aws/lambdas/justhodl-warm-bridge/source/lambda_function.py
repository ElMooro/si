"""justhodl-warm-bridge — the wiring arc's keystone (ops 4484).

Warm -> hot: publish page-consumable feeds from tonight's archives, every
numeric value wrapped in a full F1 provenance envelope (value + source URL
+ raw_snapshot_key + fetched_at) — so this single engine both wires the
supply side to pages AND lifts the F9 coverage baseline. Feeds:
  data/ofr-funding.json     tri-party/GCF/DVP repo + volumes (OFR)
  data/soma-holdings.json   SOMA portfolio summary (NY Fed)
  data/treasury-fiscal.json 6 fiscaldata datasets, latest+prev (Treasury)
  data/bls-macro.json       CPI/PPI/JOLTS/unemployment latest (BLS)
  data/bea-gdp.json         Real GDP q/q latest from NIPA (BEA)
Hourly. Absent archives -> missing() envelopes, never zeros."""
import gzip
import json
import os
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from provenance import wrap, missing
except Exception:
    def wrap(v, **kw):
        return {"value": v, **kw}

    def missing(reason, **kw):
        return {"data_unavailable": True, "reason": reason, **kw}


def _get(k):
    b = s3.get_object(Bucket=BUCKET, Key=k)["Body"].read()
    if k.endswith(".gz"):
        b = gzip.decompress(b)
    return json.loads(b)


def _pub(key, doc):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")


def _ofr(now):
    out = {"as_of": now, "source": "OFR Short-Term Funding Monitor"}
    for mn, label in [("REPO-TRI_AR_OO-P", "triparty_rate"),
                      ("REPO-TRI_TV_OO-P", "triparty_volume"),
                      ("REPO-DVP_AR_OO-P", "dvp_rate"),
                      ("REPO-GCF_AR_OO-P", "gcf_rate"),
                      ("FNYR-SOFR-A", "sofr")]:
        try:
            d = _get(f"data/warm/ofr/series/{mn}.json.gz")
            ts = (d.get("payload", {}).get("timeseries", {})
                  .get("aggregation") or [])
            last = ts[-1] if ts else None
            if last and len(last) >= 2:
                out[label] = wrap(
                    last[1], source_url=d.get("source_url"),
                    raw_snapshot_key=d.get("raw_snapshot_key"),
                    provider="ofr", observed=str(last[0]),
                    fetched_at=d.get("as_of"))
            else:
                out[label] = missing("empty timeseries", provider="ofr")
        except Exception as e:
            out[label] = missing(f"{type(e).__name__}: {str(e)[:50]}",
                                 provider="ofr")
    _pub("data/ofr-funding.json", out)
    return sum(1 for v in out.values()
               if isinstance(v, dict) and "value" in v)


def _soma(now):
    try:
        d = _get("data/warm/nyfed-markets/soma_summary.json.gz")
        rows = (d.get("payload", {}).get("soma", {}).get("summary")
                or [])
        last = rows[-1] if rows else {}
        out = {"as_of": now, "source": "NY Fed SOMA",
               "as_of_date": last.get("asOfDate")}
        n = 0
        for k, label in [("total", "total"), ("bills", "bills"),
                         ("notesbonds", "notes_bonds"),
                         ("mbs", "mbs"), ("tips", "tips")]:
            v = last.get(k)
            if v is not None:
                out[label] = wrap(
                    float(v), provider="nyfed",
                    source_url="markets.newyorkfed.org/api"
                               "/soma/summary.json",
                    raw_snapshot_key=d.get("raw_snapshot_key"),
                    fetched_at=d.get("as_of"))
                n += 1
            else:
                out[label] = missing("field absent", provider="nyfed")
        _pub("data/soma-holdings.json", out)
        return n
    except Exception as e:
        _pub("data/soma-holdings.json",
             {"as_of": now,
              **missing(f"{type(e).__name__}: {str(e)[:60]}",
                        provider="nyfed")})
        return 0


def _treasury(now):
    out = {"as_of": now, "source": "Treasury fiscaldata"}
    n = 0
    for ds in ("debt_to_penny", "tga_operating_cash",
               "avg_interest_rates", "interest_expense",
               "debt_outstanding", "rates_of_exchange"):
        try:
            d = _get(f"data/warm/treasury/{ds}.json.gz")
            obs = d.get("observations") or []
            if obs:
                out[ds] = wrap(
                    obs[-1]["value"], observed=obs[-1]["date"],
                    unit=d.get("unit"), provider="treasury",
                    source_url=d.get("source_url"),
                    raw_snapshot_key=d.get("raw_snapshot_key"))
                n += 1
            else:
                out[ds] = missing("no observations",
                                  provider="treasury")
        except Exception as e:
            out[ds] = missing(f"{type(e).__name__}: {str(e)[:40]}",
                              provider="treasury")
    _pub("data/treasury-fiscal.json", out)
    return n


def _bls(now):
    out = {"as_of": now, "source": "BLS v2 API"}
    n = 0
    for sid, label in [("CUUR0000SA0", "cpi_headline"),
                       ("CUUR0000SA0L1E", "cpi_core"),
                       ("WPUFD4", "ppi_final_demand"),
                       ("LNS14000000", "unemployment_rate"),
                       ("JTS000000000000000JOL", "jolts_openings")]:
        try:
            d = _get(f"data/warm/usgov/bls/{sid}.json.gz")
            data = d.get("data") or []
            last = data[0] if data else None
            if last:
                out[label] = wrap(
                    float(last.get("value")),
                    observed=f"{last.get('year')}-"
                             f"{last.get('period')}",
                    provider="bls", series=sid,
                    source_url="api.bls.gov/publicAPI/v2",
                    raw_snapshot_key=d.get("raw_snapshot_key"))
                n += 1
            else:
                out[label] = missing("empty series", provider="bls",
                                     series=sid)
        except Exception as e:
            out[label] = missing(f"{type(e).__name__}: {str(e)[:40]}",
                                 provider="bls", series=sid)
    _pub("data/bls-macro.json", out)
    return n


def _bea(now):
    try:
        d = _get("data/warm/usgov/bea/nipa-t10101.json.gz")
        rows = [r for r in (d.get("rows") or [])
                if r.get("LineNumber") == "1"]
        rows.sort(key=lambda r: r.get("TimePeriod", ""))
        last = rows[-1] if rows else None
        out = {"as_of": now, "source": "BEA NIPA T10101"}
        if last:
            out["real_gdp_qq_pct"] = wrap(
                float(str(last.get("DataValue", "")
                          ).replace(",", "")),
                observed=last.get("TimePeriod"), provider="bea",
                source_url="apps.bea.gov/api (NIPA T10101 L1)")
            _pub("data/bea-gdp.json", out)
            return 1
        out["real_gdp_qq_pct"] = missing("line 1 absent",
                                         provider="bea")
        _pub("data/bea-gdp.json", out)
        return 0
    except Exception as e:
        _pub("data/bea-gdp.json",
             {"as_of": now,
              **missing(f"{type(e).__name__}: {str(e)[:60]}",
                        provider="bea")})
        return 0


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    res = {"ok": True,
           "wrapped": {"ofr": _ofr(now), "soma": _soma(now),
                       "treasury": _treasury(now), "bls": _bls(now),
                       "bea": _bea(now)}}
    res["total_enveloped"] = sum(res["wrapped"].values())
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
