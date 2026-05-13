#!/usr/bin/env python3
"""503 — Re-probe yahoo-proxy after fresh deploy + invoke GEX."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/503_dealer_gex_proxy_verify.json"
NAME = "justhodl-dealer-gex"

lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = """
import json, urllib.request, time, boto3
ssm = boto3.client('ssm', region_name='us-east-1')

def probe(url, headers=None):
    headers = headers or {}
    headers.setdefault('User-Agent', 'Mozilla/5.0 (Macintosh) Chrome/120 Safari/537.36')
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as r:
            return {'url': url, 'status': r.status,
                     'body_preview': r.read().decode('utf-8','replace')[:400],
                     'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8','replace')[:400] if hasattr(e,'read') else ''
        return {'url': url, 'status': e.code, 'body_preview': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'url': url, 'status': 'EXC', 'err': str(e)[:200], 'elapsed_ms': int((time.time()-t0)*1000)}

def lambda_handler(event, context):
    try: tok = ssm.get_parameter(Name='/justhodl/ai-chat/auth-token', WithDecryption=True)['Parameter']['Value']
    except: tok = None
    out = {'token_len': len(tok or '')}
    out['p1'] = probe('https://justhodl-yahoo-proxy.raafouis.workers.dev/')
    out['p2_auth_health'] = probe('https://justhodl-yahoo-proxy.raafouis.workers.dev/', {'x-justhodl-token': tok or ''})
    out['p3_auth_spy'] = probe('https://justhodl-yahoo-proxy.raafouis.workers.dev/options/SPY', {'x-justhodl-token': tok or ''})
    out['p4_no_auth_spy'] = probe('https://justhodl-yahoo-proxy.raafouis.workers.dev/options/SPY')
    return {'statusCode': 200, 'body': json.dumps(out)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    PROBE_NAME = "justhodl-tmp-probe-503"

    try:
        lam.create_function(
            FunctionName=PROBE_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=512, Timeout=45, Code={"ZipFile": zip_str(PROBE_CODE)},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=PROBE_NAME)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=PROBE_NAME, ZipFile=zip_str(PROBE_CODE))
        lam.get_waiter("function_updated").wait(FunctionName=PROBE_NAME)

    _time.sleep(2)
    try:
        r = lam.invoke(FunctionName=PROBE_NAME, InvocationType="RequestResponse",
                        Payload=b"{}")
        body = r["Payload"].read().decode("utf-8")
        p = json.loads(body)
        out["probes"] = json.loads(p["body"]) if p.get("body") else p
    except Exception as e:
        out["probe_err"] = str(e)[:300]

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except: pass

    # Invoke GEX again (no redeploy — Lambda already has the right code)
    _time.sleep(2)
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["gex_status"] = resp.get("StatusCode")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["gex_response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["gex_raw"] = body[:1500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8","replace")[-2500:]
    except Exception as e:
        out["gex_err"] = str(e)[:300]

    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/dealer-gex.json")
        p = json.loads(obj["Body"].read())
        ulying = p.get("underlyings", {})
        out["sidecar"] = {
            "modified": obj["LastModified"].isoformat()[:19],
            "composite_regime": (p.get("market_composite") or {}).get("composite_regime"),
            "underlyings_ok": [s for s, r in ulying.items() if not r.get("err")],
            "underlyings_err": {s: r.get("err") for s, r in ulying.items() if r.get("err")},
            "spy": {k: v for k, v in ulying.get("SPY", {}).items()
                     if k in ["spot","total_dealer_gex_billions","zero_gamma_flip_level",
                               "regime","n_contracts_modeled","pcr_oi"]},
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
