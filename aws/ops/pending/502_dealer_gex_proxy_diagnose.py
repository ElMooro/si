#!/usr/bin/env python3
"""Step 502 — Test proxy reachability from inside Lambda, then redeploy GEX
with the correct workers.dev URL. Confirms whether the 403 is sandbox-only
or also blocks AWS IPs."""
import io, json, os, time as _time, zipfile, base64, urllib.request, urllib.parse
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/502_dealer_gex_proxy_diagnose.json"
SOURCE = "aws/lambdas/justhodl-dealer-gex/source/lambda_function.py"
NAME = "justhodl-dealer-gex"

lam = boto3.client("lambda", region_name="us-east-1")

# Test Lambda inline — probes the proxy from AWS us-east-1 IP space
PROBE_CODE = """
import json, urllib.request, urllib.parse, boto3, time

ssm = boto3.client('ssm', region_name='us-east-1')

def fetch_token():
    try:
        return ssm.get_parameter(Name='/justhodl/ai-chat/auth-token',
                                  WithDecryption=True)['Parameter']['Value']
    except Exception as e:
        return None

def probe(url, headers=None):
    headers = headers or {}
    headers.setdefault('User-Agent', 'JustHodl-Probe/1.0')
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=8) as r:
            body = r.read().decode('utf-8', 'replace')[:500]
            return {'url': url, 'status': r.status,
                     'body_preview': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', 'replace')[:500] if hasattr(e, 'read') else ''
        return {'url': url, 'status': e.code, 'body_preview': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'url': url, 'status': 'EXCEPTION', 'err': str(e)[:200], 'elapsed_ms': int((time.time()-t0)*1000)}

def lambda_handler(event, context):
    tok = fetch_token()
    out = {'token_len': len(tok) if tok else 0, 'probes': []}

    # Probe 1: workers.dev base (health endpoint)
    out['probes'].append({'desc': 'workers.dev /', **probe(
        'https://justhodl-yahoo-proxy.raafouis.workers.dev/')})

    # Probe 2: workers.dev with auth token
    out['probes'].append({'desc': 'workers.dev / with token',
        **probe('https://justhodl-yahoo-proxy.raafouis.workers.dev/',
                 {'x-justhodl-token': tok or ''})})

    # Probe 3: workers.dev options/SPY with token
    out['probes'].append({'desc': 'workers.dev /options/SPY with token',
        **probe('https://justhodl-yahoo-proxy.raafouis.workers.dev/options/SPY',
                 {'x-justhodl-token': tok or ''})})

    # Probe 4: same but with browser-like UA
    out['probes'].append({'desc': 'workers.dev /options/SPY with token + browser UA',
        **probe('https://justhodl-yahoo-proxy.raafouis.workers.dev/options/SPY',
                 {'x-justhodl-token': tok or '',
                  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                  'Accept': 'application/json'})})

    # Probe 5: custom domain
    out['probes'].append({'desc': 'yahoo-proxy.justhodl.ai /',
        **probe('https://yahoo-proxy.justhodl.ai/')})

    # Probe 6: direct Yahoo from AWS (for comparison)
    out['probes'].append({'desc': 'Yahoo direct from AWS',
        **probe('https://query2.finance.yahoo.com/v7/finance/options/SPY',
                 {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})})

    return {'statusCode': 200, 'body': json.dumps(out, default=str)}
"""


def zip_source_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def zip_source(path):
    with open(path, "rb") as f: code = f.read()
    return zip_source_str(code.decode())


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ─── Step 1: Create temp probe Lambda ───
    PROBE_NAME = "justhodl-tmp-proxy-probe"
    role_arn = "arn:aws:iam::857687956942:role/lambda-execution-role"
    zb = zip_source_str(PROBE_CODE)
    try:
        lam.create_function(
            FunctionName=PROBE_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=role_arn,
            MemorySize=512, Timeout=60, Code={"ZipFile": zb},
            Description="Temp proxy probe",
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=PROBE_NAME)
        out["probe_lambda_created"] = True
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=PROBE_NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=PROBE_NAME)
        out["probe_lambda_updated"] = True

    _time.sleep(2)
    try:
        r = lam.invoke(FunctionName=PROBE_NAME, InvocationType="RequestResponse",
                        Payload=b"{}")
        body = r["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        out["probes"] = json.loads(parsed["body"]) if parsed.get("body") else parsed
    except Exception as e:
        out["probe_err"] = str(e)[:300]

    # Cleanup probe
    try:
        lam.delete_function(FunctionName=PROBE_NAME)
        out["probe_cleaned"] = True
    except Exception as e:
        out["probe_cleanup_err"] = str(e)[:100]

    # ─── Step 2: Redeploy GEX with fixed URL ───
    zb = zip_source(SOURCE)
    try:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        out["gex_redeploy"] = "ok"
    except Exception as e:
        out["gex_redeploy_err"] = str(e)[:300]

    # ─── Step 3: Invoke GEX and capture result ───
    _time.sleep(2)
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["gex_invoke_status"] = resp.get("StatusCode")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["gex_response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["gex_raw"] = body[:1500]
        if resp.get("LogResult"):
            out["gex_log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2500:]
    except Exception as e:
        out["gex_invoke_err"] = str(e)[:300]

    # ─── Step 4: Read sidecar to see if data is now populated ───
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/dealer-gex.json")
        p = json.loads(obj["Body"].read())
        out["sidecar"] = {
            "modified": obj["LastModified"].isoformat()[:19],
            "composite_regime": (p.get("market_composite") or {}).get("composite_regime"),
            "underlyings_with_data": [
                sym for sym, r in (p.get("underlyings") or {}).items()
                if not r.get("err")
            ],
            "underlyings_errors": {
                sym: r.get("err") for sym, r in (p.get("underlyings") or {}).items()
                if r.get("err")
            },
            "spy_summary": {
                k: v for k, v in (p.get("underlyings") or {}).get("SPY", {}).items()
                if k in ["spot", "total_dealer_gex_billions", "zero_gamma_flip_level",
                          "regime", "n_contracts_modeled", "total_call_oi", "total_put_oi"]
            },
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
