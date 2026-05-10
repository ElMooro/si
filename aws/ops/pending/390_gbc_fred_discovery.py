#!/usr/bin/env python3
"""Step 390 — Bulk-test FRED CLI series IDs across multiple naming patterns
to find which IDs actually return data for each country."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/390_gbc_fred_discovery.json"
NAME = "justhodl-tmp-gbc-fred"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

# Countries currently UNKNOWN — need new FRED IDs
MISSING = ["USA","BRA","KOR","CAN","AUS","MEX","IDN","ESP","ITA","NLD",
            "CHE","BEL","SWE","IRL","AUT","NOR","ZAF","DNK","FIN","CZE",
            "HUN","CHL","COL","PRT","GRC","NZL","ISR"]

# Patterns to test (OECD CLI families)
PATTERNS = [
    "{iso3}LOLITONOSTSAM",   # normalized, smoothed (original)
    "{iso3}LOLITOAASTSAM",   # amplitude adjusted, smoothed
    "{iso3}LOLITOTRSTSAM",   # trend restored, smoothed
    "{iso3}LORSGPNOSTSAM",   # ratio, GDP-based
    "{iso3}LORSGPORIGNM",    # original
    "{iso3}LOLITOAASTSAMP",  # alt with P suffix
    "{iso3}LOLITONOSTSAMP",  # alt with P suffix
]

DIAG_CODE = '''
import json, urllib.request
FRED_KEY = "2f057499936072679d8843d7fce99989"

def test_id(sid):
    """Returns (works, n_obs, latest_value, latest_date)"""
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=3"
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"JH-test/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        obs = data.get("observations", [])
        if not obs: return False, 0, None, None
        valid = [o for o in obs if o.get("value") not in (".","",None)]
        if not valid: return False, 0, None, None
        latest = valid[0]
        return True, len(valid), latest["value"], latest["date"]
    except Exception as e:
        return False, 0, None, f"err: {str(e)[:80]}"

def lambda_handler(event, context):
    missing = event["missing"]
    patterns = event["patterns"]
    results = {}
    for iso3 in missing:
        results[iso3] = {}
        for pat in patterns:
            sid = pat.format(iso3=iso3)
            works, n, val, date = test_id(sid)
            results[iso3][sid] = {"works": works, "n": n,
                                     "latest_value": val, "latest_date": date}
            if works: break  # found one, no need to try others for this country
    return {"statusCode": 200, "body": json.dumps(results, default=str)}
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
                            MemorySize=256, Timeout=600, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    payload = json.dumps({"missing": MISSING, "patterns": PATTERNS})
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                       Payload=payload.encode())
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
