#!/usr/bin/env python3
"""Step 395 — Find ALWAYS-FRESH inputs per country: sovereign yields,
equity indices, FX, unemployment. These FRED series are all updated daily
or monthly with <1mo lag and give us real-time cycle signals."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/395_realtime_inputs_probe.json"
NAME = "justhodl-tmp-rt-probe"
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


def fred(sid):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=2"
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
        return None


# Standardized OECD interest-rate series on FRED follow this pattern:
#   IRLT01{ISO}M156N  - 10y long-term rate
#   IR3TIB01{ISO}M156N - 3-month interbank rate
#   IRSTCB01{ISO}M156N - short-term central-bank rate
#   {ISO}LRHUTTTTM156S - harmonised unemployment rate

COUNTRIES = ["USA","CHN","DEU","JPN","GBR","FRA","ITA","ESP","CAN","AUS",
              "KOR","IND","BRA","MEX","NLD","CHE","SWE","POL","TUR","ZAF",
              "IDN","BEL","NOR","DNK","FIN","AUT","IRL","PRT","GRC","CZE",
              "HUN","CHL","NZL","ISR"]

# Country → known fresh equity index FRED IDs
EQUITY_IDS = {
    "USA": ["SP500", "NASDAQCOM", "DJIA"],
    "DEU": ["DAXAUUD"],
    "JPN": ["NIKKEI225"],
    "GBR": ["FTSE"],
    "FRA": ["CAC40"],
    "CHN": ["SSEC"],
    "ITA": ["FTSEMIB"],
    "ESP": ["IBEX35"],
    "CAN": ["TSX"],
    "AUS": ["AORD"],
    "KOR": ["KOSPI"],
    "IND": ["BSE"],
    "BRA": ["BVSP"],
    "MEX": ["MEXBOL"],
    "NLD": ["AEX"],
    "CHE": ["SMI"],
    "SWE": ["OMXSPI"],
    "POL": ["WIG20"],
    "TUR": ["XU100"],
    "ZAF": ["JALSH"],
}


def probe_country(iso3):
    """Probe yield curve, unemployment, equity, FX for one country."""
    found = []
    # Yield curve
    for sid in [f"IRLT01{iso3}M156N", f"IR3TIB01{iso3}M156N", f"IRSTCB01{iso3}M156N"]:
        info = fred(sid)
        if info: info["family"] = "yield"; found.append(info)
    # Unemployment
    for sid in [f"LRHUTTTT{iso3}M156S", f"{iso3}URTOTLM156S"]:
        info = fred(sid)
        if info: info["family"] = "unemp"; found.append(info)
    # Equity index (country-specific)
    for sid in EQUITY_IDS.get(iso3, []):
        info = fred(sid)
        if info: info["family"] = "equity"; found.append(info)
    # FX vs USD (for non-USD countries)
    if iso3 != "USA":
        for sid in [f"DEX{iso3[:2]}US", f"EX{iso3[:2]}USA"]:
            info = fred(sid)
            if info: info["family"] = "fx"; found.append(info)
    return found


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
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
