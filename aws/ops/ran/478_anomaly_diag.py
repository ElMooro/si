#!/usr/bin/env python3
"""Step 478 — Diagnose why anomaly-detector found zero anomalies in 1s."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/478_anomaly_diag.json"
NAME = "justhodl-tmp-478"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

# Note: this diagnostic Lambda inherits NOTHING — it tests by reading
# the anomaly-detector's env values directly.

DIAG = '''
import json, urllib.request, time, os
import boto3
lam = boto3.client("lambda", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Get anomaly-detector's actual env values (just lengths, not raw values)
    cfg = lam.get_function_configuration(FunctionName="justhodl-anomaly-detector")
    env = (cfg.get("Environment") or {}).get("Variables", {})
    out["env_summary"] = {k: {"len": len(v or ""), "first3": (v or "")[:3]} for k, v in env.items()}

    # 2. Test FRED with the actual key  
    fred = env.get("FRED_KEY", "")
    if fred:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id=VIXCLS&api_key={fred}&file_type=json&observation_start=2026-01-01&sort_order=desc&limit=5"
        try:
            t0 = time.time()
            r = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent":"JH-diag/1.0"}), timeout=15)
            body = r.read().decode("utf-8")
            data = json.loads(body)
            obs = data.get("observations", [])
            out["fred_test"] = {"ok": True, "n_obs": len(obs), "first_obs": obs[0] if obs else None,
                                  "elapsed_s": round(time.time()-t0, 2)}
        except Exception as e:
            out["fred_test"] = {"ok": False, "err": str(e)[:300]}
    else:
        out["fred_test"] = {"err": "no FRED_KEY in env"}

    # 3. Test Polygon
    poly = env.get("POLY_KEY", "")
    if poly:
        url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/2026-04-01/2026-05-10?adjusted=true&sort=desc&limit=5&apiKey={poly}"
        try:
            t0 = time.time()
            r = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent":"JH-diag/1.0"}), timeout=15)
            data = json.loads(r.read().decode("utf-8"))
            results = data.get("results", [])
            out["poly_test"] = {"ok": True, "status": data.get("status"),
                                 "n_results": len(results),
                                 "first": results[0] if results else None,
                                 "elapsed_s": round(time.time()-t0, 2)}
        except Exception as e:
            out["poly_test"] = {"ok": False, "err": str(e)[:300]}
    else:
        out["poly_test"] = {"err": "no POLY_KEY in env"}

    # 4. Try fetching FRED VIX over a longer window
    if fred:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id=VIXCLS&api_key={fred}&file_type=json&observation_start=2025-01-01&sort_order=asc"
        try:
            t0 = time.time()
            r = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent":"JH-diag/1.0"}), timeout=20)
            data = json.loads(r.read().decode("utf-8"))
            obs = data.get("observations", [])
            # Count those with valid values
            valid = [o for o in obs if o.get("value") not in (".","",None)]
            out["fred_year_test"] = {"ok": True, "total_obs": len(obs), "valid_obs": len(valid),
                                       "elapsed_s": round(time.time()-t0, 2),
                                       "last_valid": valid[-1] if valid else None}
        except Exception as e:
            out["fred_year_test"] = {"ok": False, "err": str(e)[:300]}

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=120, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
