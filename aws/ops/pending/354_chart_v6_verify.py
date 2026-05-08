#!/usr/bin/env python3
"""Step 354 — Verify ECB ILM, Bitcoin, and other key endpoints."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/354_chart_v6_verify.json"
NAME = "justhodl-test-v6"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request

def lambda_handler(event, context):
    out = {}
    base = "https://zsgb72zf4ayw6ajw7phbyq6wzq0haobh.lambda-url.us-east-1.on.aws/"
    
    tests = [
        ("ecb_ilm_claims", base + "?series=ilm_claims_fx&kind=ecb"),
        ("ecb_ilm_liab",   base + "?series=ilm_liab_eur&kind=ecb"),
        ("ecb_ciss_us",    base + "?series=ciss_us&kind=ecb"),
        ("btc_crypto",     base + "?series=X:BTCUSD&kind=crypto&from=2020-01-01"),
        ("eth_crypto",     base + "?series=X:ETHUSD&kind=crypto&from=2022-01-01"),
        ("ibit_stock",     base + "?series=IBIT&kind=stock&from=2024-01-01"),
        ("ofr_fails",      base + "?series=NYPD-PD_AFtD_TOT-A&kind=ofr"),
        ("dgs10",          base + "?series=DGS10&kind=fred&from=2024-01-01"),
    ]
    
    for label, url in tests:
        try:
            req = urllib.request.Request(url,
                headers={"Origin": "https://justhodl.ai", "User-Agent": "verify/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                body = r.read().decode("utf-8")
                d = json.loads(body)
                first = (d.get("data") or [None])[0]
                last = (d.get("data") or [None])[-1] if d.get("data") else None
                out[label] = {
                    "status": r.status, "n_obs": d.get("n_obs"),
                    "source": d.get("source"), "error": d.get("error"),
                    "first": first, "last": last,
                }
        except Exception as e:
            out[label] = {"err": str(e)[:200]}
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=300, Code={"ZipFile": buf.getvalue()})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        try:
            lam.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
            lam.update_function_configuration(FunctionName=NAME, Timeout=300)
            lam.get_waiter("function_updated").wait(FunctionName=NAME)
        except Exception as e:
            out["err"] = str(e)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        out["test"] = json.loads(json.loads(body).get("body", "{}"))
    except Exception:
        out["raw"] = body[:1500]
    try: lam.delete_function(FunctionName=NAME)
    except: pass
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
