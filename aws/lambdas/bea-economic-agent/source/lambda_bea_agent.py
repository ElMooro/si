"""
bea-economic-agent — Bureau of Economic Analysis desk. The source behind real GDP,
the Fed's preferred inflation gauge (PCE & core PCE price indexes), and personal
income / the saving rate. Persists to S3 (data/bea-economic.json).

Key from env BEA_API_KEY (SSM + Lambda env; not hardcoded). Real data only.
"""
import os
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/bea-economic.json"
API_KEY = os.environ.get("BEA_API_KEY", "")
BASE = "https://apps.bea.gov/api/data"


def bea_table(table, frequency):
    now = datetime.now(timezone.utc)
    years = ",".join(str(now.year - i) for i in range(3))
    params = {
        "UserID": API_KEY, "method": "GetData", "datasetname": "NIPA",
        "TableName": table, "Frequency": frequency, "Year": years, "ResultFormat": "json",
    }
    url = BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/bea"})
    with urllib.request.urlopen(req, timeout=40) as r:
        resp = json.loads(r.read())
    return ((resp.get("BEAAPI", {}) or {}).get("Results", {}) or {}).get("Data", []) or []


def _num(v):
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None


def line_series(data, match):
    """period->value for the first LineDescription containing `match` (case-insensitive)."""
    m = match.lower()
    ser = {}
    for row in data:
        desc = (row.get("LineDescription") or "").lower()
        if m in desc:
            v = _num(row.get("DataValue"))
            if v is not None:
                ser[row.get("TimePeriod")] = v
    return ser


def latest_and_yoy(ser):
    if not ser:
        return None, None, None
    periods = sorted(ser.keys())
    latest = periods[-1]
    val = ser[latest]
    yoy = None
    # match same month/quarter prior year
    if "M" in latest:
        y, m = latest.split("M"); prior = f"{int(y)-1}M{m}"
    elif "Q" in latest:
        y, q = latest.split("Q"); prior = f"{int(y)-1}Q{q}"
    else:
        prior = None
    if prior and prior in ser and ser[prior]:
        yoy = round((val / ser[prior] - 1) * 100, 2)
    return val, latest, yoy


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    if not API_KEY:
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps({"generated_at": now.isoformat(), "error": "BEA_API_KEY not set"}).encode(),
                      ContentType="application/json")
        return {"ok": False, "error": "no key"}

    out = {"generated_at": now.isoformat(),
           "source": "U.S. Bureau of Economic Analysis API (apps.bea.gov) — direct, free",
           "gdp": {}, "pce_inflation": {}, "income": {}, "_error": None}
    errs = []

    # Real GDP % change (annualized), quarterly — T10101 line 1
    try:
        d = bea_table("T10101", "Q")
        ser = line_series(d, "gross domestic product")
        val, per, _ = latest_and_yoy(ser)
        out["gdp"] = {"real_gdp_qoq_saar_pct": val, "quarter": per,
                      "signal": ("STRONG" if val and val >= 3 else "SOLID" if val and val >= 2
                                 else "SOFT" if val and val >= 1 else "STALL" if val is not None else None)}
    except Exception as e:
        errs.append("gdp:" + str(e)[:80])

    # PCE price index + core PCE (the Fed's gauge) — T20804 monthly index levels -> YoY
    try:
        d = bea_table("T20804", "M")
        headline = line_series(d, "personal consumption expenditures")
        core = line_series(d, "excluding food and energy")
        hv, hp, hy = latest_and_yoy(headline)
        cv, cp, cy = latest_and_yoy(core)
        out["pce_inflation"] = {
            "pce_yoy_pct": hy, "pce_index": hv, "month": hp,
            "core_pce_yoy_pct": cy, "core_pce_index": cv,
            "vs_fed_target_bps": round((cy - 2.0) * 100) if cy is not None else None,
            "read": ("ABOVE TARGET" if cy and cy > 2.3 else "NEAR TARGET" if cy is not None else None),
        }
    except Exception as e:
        errs.append("pce:" + str(e)[:80])

    # Personal income, DPI, saving rate — T20600 monthly
    try:
        d = bea_table("T20600", "M")
        pi = line_series(d, "personal income")
        dpi = line_series(d, "disposable personal income")
        sav = line_series(d, "personal saving rate")
        piv, pip, piy = latest_and_yoy(pi)
        sv, sp, _ = latest_and_yoy(sav)
        out["income"] = {"personal_income_bil": piv, "personal_income_yoy_pct": piy,
                         "disposable_income_bil": latest_and_yoy(dpi)[0],
                         "saving_rate_pct": sv, "month": pip or sp}
    except Exception as e:
        errs.append("income:" + str(e)[:80])

    out["_error"] = "; ".join(errs) or None
    out["_blocks_live"] = sum([bool(out["gdp"].get("real_gdp_qoq_saar_pct") is not None),
                               bool(out["pce_inflation"].get("core_pce_yoy_pct") is not None),
                               bool(out["income"].get("saving_rate_pct") is not None)])
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=3600")
    return {"ok": True, "blocks_live": out["_blocks_live"]}
