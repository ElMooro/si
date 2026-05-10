#!/usr/bin/env python3
"""Step 394 — Find FRESH (≤3mo) real-time business-cycle inputs per country.

Probes FRED for each major country across multiple data families:
 - OECD Business Confidence (BCI):  {iso}BSCICP02STSAM, {iso}BSCURT01STSAM
 - OECD Consumer Confidence (CCI):  {iso}CSCICP02STSAM
 - S&P Global / IHS Markit PMI:     country-specific (varies)
 - Industrial Production:           country-specific
 - Equity index level:              country-specific (always fresh)
 - Yield-curve slope (proxy):       country-specific

Returns per country: list of (series_id, latest_date, months_stale, value).
We pick the best fresh ones to feed the synthetic CLI engine.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/394_fresh_inputs_probe.json"
NAME = "justhodl-tmp-fresh-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = r'''
import json, urllib.request
from datetime import datetime
FRED_KEY = "2f057499936072679d8843d7fce99989"
NOW = datetime.utcnow()


def months_stale(date_str):
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return (NOW.year - d.year) * 12 + (NOW.month - d.month)
    except: return 999


def fred(sid, limit=1):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"JH-probe/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            d = json.loads(r.read().decode("utf-8"))
        obs = d.get("observations", [])
        valid = [o for o in obs if o.get("value") not in (".","",None)]
        if not valid: return None
        return {"id":sid, "date":valid[0]["date"], "value":valid[0]["value"],
                 "ms":months_stale(valid[0]["date"])}
    except Exception as e:
        return {"id":sid, "err":str(e)[:80]}


# Candidate series per family for each country
COUNTRIES = ["USA","CHN","DEU","JPN","GBR","FRA","ITA","ESP","CAN","AUS",
              "KOR","IND","BRA","MEX","NLD","CHE","SWE","POL","TUR","ZAF"]


def probe_country(iso3):
    """Test a comprehensive set of candidate FRED series for this country."""
    candidates = []

    # ── OECD Business Confidence family ──
    for tmpl in ["{iso}BSCICP02STSAM",   # business confidence composite
                  "{iso}BSCICP03STSAM",   # alt v3
                  "{iso}BSCURT01STSAM",   # capacity utilization
                  "{iso}BSCITR02STSAM"]:  # business trend
        candidates.append(("BCI", tmpl.format(iso=iso3)))

    # ── OECD Consumer Confidence family ──
    for tmpl in ["{iso}CSCICP02STSAM",   # consumer confidence composite
                  "{iso}CSCICP03STSAM"]:
        candidates.append(("CCI", tmpl.format(iso=iso3)))

    # ── Industrial Production family ──
    for tmpl in ["{iso}PROINDMISMEI",    # production index MEI
                  "{iso}PRMNTO01IXOBM",  # mfg production
                  "{iso}PRINTO01IXOBM"]: # industrial production index
        candidates.append(("IP", tmpl.format(iso=iso3)))

    # ── Country-specific PMI / additional indicators ──
    # These vary widely. We list known fresh series for major countries:
    extras = {
        "USA": [("PMI","USPMINDXM"),       # US ISM Mfg
                 ("PMI","NAPMNOI"),           # New orders
                 ("PMI","USSLIND"),           # Conference Board / state LEI
                 ("LEI","USALOLITONOSTSAM"),  # OECD CLI (known stale, kept for compare)
                 ("CONSCONF","UMCSENT"),     # Michigan consumer
                 ("BUILDING","PERMIT"),       # Housing permits
                 ("CLAIMS","ICSA")],           # Initial claims (weekly!)
        "CHN": [("PMI","CHNPMINDIX"),
                 ("PMI","CHNMANPMI")],
        "DEU": [("PMI","DEUMANPMI"),
                 ("BCI","BSCICP03DEM665S"),  # ZEW/IFO equivalent
                 ("BCI","DEUBSCICP02STSAM")],
        "JPN": [("PMI","JPNMANPMI"),
                 ("BCI","BSCICP03JPM665S")],
        "GBR": [("PMI","GBRMANPMI"),
                 ("BCI","BSCICP03GBM665S")],
        "FRA": [("BCI","BSCICP03FRM665S"),
                 ("PMI","FRAMANPMI")],
        "ITA": [("BCI","BSCICP03ITM665S")],
    }
    for fam, sid in extras.get(iso3, []):
        candidates.append((fam, sid))

    # Test each
    results = []
    for fam, sid in candidates:
        info = fred(sid)
        if info and "err" not in info:
            info["family"] = fam
            results.append(info)
    return results


def lambda_handler(event, context):
    out = {"now": NOW.isoformat(), "by_country": {}}
    for iso3 in COUNTRIES:
        out["by_country"][iso3] = probe_country(iso3)
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=600, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
