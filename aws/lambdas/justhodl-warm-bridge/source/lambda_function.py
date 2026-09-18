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

from ofr_funding import build_funding
from evidence_store import capture
from provenance import wrap as _lib_wrap, missing as _lib_missing

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")


def wrap(v, **kw):
    """Explicit adapter: provider metadata cannot occupy the field-name slot."""
    field = kw.get("field") or kw.get("series") or "unidentified_measurement"
    env = _lib_wrap(v, field, unit=kw.get("unit"), source=kw.get("provider", "unknown"),
                    series_id=kw.get("series") or field, url=kw.get("source_url"),
                    as_of=kw.get("observed"), received_at=kw.get("fetched_at"),
                    raw_key=kw.get("raw_snapshot_key"), evidence=kw.get("evidence"),
                    confidence=kw.get("confidence"))
    for key, value in kw.items():
        env.setdefault(key, value)
    return env


def missing(reason, **kw):
    field = kw.get("field") or kw.get("series") or "unidentified_measurement"
    env = _lib_missing(field, reason, unit=kw.get("unit"), source=kw.get("provider"))
    for key, value in kw.items():
        env.setdefault(key, value)
    return env


def _get(k, *, storage_metadata=False, archive=False):
    obj = s3.get_object(Bucket=BUCKET, Key=k)
    b = obj["Body"].read()
    if k.endswith(".gz"):
        b = gzip.decompress(b)
    doc = json.loads(b)
    if archive and isinstance(doc, dict):
        proof = capture(s3, BUCKET, "warehouse", "https://" + BUCKET + ".s3.amazonaws.com/" + k, b)
        proof["basis"] = "exact_warehouse_input; original-provider provenance evaluated separately"
        doc["_input_evidence"] = proof
    if storage_metadata and isinstance(doc, dict) and obj.get("LastModified"):
        doc["_warehouse_last_modified"] = obj["LastModified"].isoformat()
    return doc


def _pub(key, doc):
    doc.setdefault("generated_at", datetime.now(timezone.utc).isoformat())
    doc.setdefault("schema_version", "2.0")
    doc.setdefault("sizing_eligible", False)
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")


def _ofr(now):
    out = build_funding(lambda key: _get(key, storage_metadata=True), now)
    _pub("data/ofr-funding.json", out)
    return out["available_fields"]


def _soma(now):
    try:
        d = _get("data/warm/nyfed-markets/soma_summary.json.gz", archive=True)
        rows = (d.get("payload", {}).get("soma", {}).get("summary")
                or [])
        last = max(rows, key=lambda row: row.get("asOfDate", "")) if rows else {}
        out = {"as_of": now, "source": "NY Fed SOMA",
               "as_of_date": last.get("asOfDate")}
        n = 0
        for k, label in [("total", "total"), ("bills", "bills"),
                         ("notesbonds", "notes_bonds"),
                         ("mbs", "mbs"), ("tips", "tips"), ("frn", "frn"),
                         ("cmbs", "cmbs"), ("agencies", "agencies"),
                         ("tipsInflationCompensation", "tips_inflation_compensation")]:
            v = last.get(k)
            if v is not None:
                out[label] = wrap(
                    float(v), field=label, series="SOMA:" + k, provider="nyfed", unit="USD",
                    observed=last.get("asOfDate"), evidence=d.get("_input_evidence"),
                    source_url="https://markets.newyorkfed.org/api"
                               "/soma/summary.json",
                    raw_snapshot_key=d.get("raw_snapshot_key"),
                    fetched_at=d.get("as_of"))
                n += 1
            else:
                out[label] = missing("field absent", field=label, series="SOMA:" + k, provider="nyfed")
        out["measurement_basis"] = "SOMA domestic holdings; par/current face basis, not market value or total Federal Reserve assets"
        components = [out.get(k, {}).get("value") for k in ("bills", "notes_bonds", "mbs", "tips", "frn", "cmbs", "agencies")]
        total = out.get("total", {}).get("value")
        out["reconciliation"] = {"component_sum_usd": sum(components) if all(v is not None for v in components) else None,
                                 "tips_inflation_compensation_included_in_total": False,
                                 "basis": "NY Fed SOMA total excludes separately reported TIPS inflation compensation"}
        subtotal = out["reconciliation"]["component_sum_usd"]
        out["reconciliation"]["difference_usd"] = total - subtotal if total is not None and subtotal is not None else None
        out["reconciliation"]["status"] = ("unavailable" if total is None or subtotal is None else
                                             "reconciled" if abs(total - subtotal) < 1 else "mismatch")
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
            d = _get(f"data/warm/treasury/{ds}.json.gz", archive=True)
            obs = d.get("observations") or []
            if obs:
                latest_date = max(row.get("date", "") for row in obs)
                latest = [row for row in obs if row.get("date") == latest_date]
                if len(latest) != 1 or ds in ("rates_of_exchange", "avg_interest_rates", "interest_expense", "debt_outstanding"):
                    out[ds] = missing("dataset dimensions are not preserved by the legacy collector; scalar selection withheld",
                                      field=ds, provider="treasury", unit=d.get("unit"))
                    out[ds]["last_observation_date"] = latest_date
                    out[ds]["latest_period_rows"] = len(latest)
                    out[ds]["evidence"] = d.get("_input_evidence")
                    continue
                out[ds] = wrap(
                    latest[0]["value"], observed=latest_date, field=ds, series="TREASURY:" + ds,
                    unit=d.get("unit"), provider="treasury", evidence=d.get("_input_evidence"),
                    source_url=d.get("source_url"),
                    raw_snapshot_key=d.get("raw_snapshot_key"))
                n += 1
            else:
                out[ds] = missing("no observations",
                                  field=ds, provider="treasury")
        except Exception as e:
            out[ds] = missing(f"{type(e).__name__}: {str(e)[:40]}",
                              field=ds, provider="treasury")
    _pub("data/treasury-fiscal.json", out)
    return n


def _bls(now):
    out = {"as_of": now, "source": "BLS v2 API"}
    n = 0
    for sid, label, unit, adjustment in [("CUUR0000SA0", "cpi_headline", "Index 1982-1984=100", "NSA"),
                       ("CUUR0000SA0L1E", "cpi_core", "Index 1982-1984=100", "NSA"),
                       ("WPUFD4", "ppi_final_demand", "Index November 2009=100", "NSA"),
                       ("LNS14000000", "unemployment_rate", "Percent", "SA"),
                       ("JTS000000000000000JOL", "jolts_openings", "Thousands of job openings", "SA")]:
        try:
            d = _get(f"data/warm/usgov/bls/{sid}.json.gz", archive=True)
            data = d.get("data") or []
            # BLS M13 is an annual average, never a newer monthly observation.
            monthly = [row for row in data if str(row.get("period", "")) in {"M%02d" % m for m in range(1, 13)}]
            last = max(monthly, key=lambda row: (str(row.get("year", "")), row["period"])) if monthly else None
            if last:
                out[label] = wrap(
                    float(last.get("value")), field=label,
                    observed=f"{last.get('year')}-"
                             f"{last.get('period')}",
                    provider="bls", series=sid, unit=unit, seasonal_adjustment=adjustment,
                    evidence=d.get("_input_evidence"),
                    source_url="https://api.bls.gov/publicAPI/v2/timeseries/data/" + sid,
                    raw_snapshot_key=d.get("raw_snapshot_key"))
                n += 1
            else:
                out[label] = missing("empty series", provider="bls",
                                     field=label, series=sid)
        except Exception as e:
            out[label] = missing(f"{type(e).__name__}: {str(e)[:40]}",
                                 field=label, provider="bls", series=sid)
    _pub("data/bls-macro.json", out)
    return n


def _bea(now):
    try:
        d = _get("data/warm/usgov/bea/nipa-t10101.json.gz", archive=True)
        rows = [r for r in (d.get("rows") or [])
                if r.get("LineNumber") == "1"]
        rows.sort(key=lambda r: r.get("TimePeriod", ""))
        last = rows[-1] if rows else None
        out = {"as_of": now, "source": "BEA NIPA T10101"}
        if last:
            out["real_gdp_qq_pct"] = wrap(
                float(str(last.get("DataValue", "")
                          ).replace(",", "")),
                    observed=last.get("TimePeriod"), field="real_gdp_qq_pct", provider="bea",
                series="NIPA:T10101:L1", unit=last.get("CL_UNIT") or "Percent change from preceding period at annual rate",
                evidence=d.get("_input_evidence"), source_url="https://apps.bea.gov/api/data/?datasetname=NIPA&TableName=T10101")
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
    if isinstance(event, dict) and event.get("feed") == "ofr":
        # Bounded repair/verification invocation: only the OFR hot key is written.
        count = _ofr(now)
        return {"statusCode": 200, "body": json.dumps({
            "ok": count == 5, "wrapped": {"ofr": count}})}
    res = {"ok": True,
           "wrapped": {"ofr": _ofr(now), "soma": _soma(now),
                       "treasury": _treasury(now), "bls": _bls(now),
                       "bea": _bea(now)}}
    res["total_enveloped"] = sum(res["wrapped"].values())
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
