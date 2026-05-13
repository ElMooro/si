#!/usr/bin/env python3
"""510 — Probe alternative SP500 universe sources to find one accessible from Lambda."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/510_sp500_sources.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = """
import json, urllib.request, time, os, boto3

FMP_KEY = os.environ.get('FMP_KEY', '')

def probe(url, headers=None, timeout=15):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode('utf-8', 'replace')
            return {'url': url[:80], 'status': r.status, 'bytes': len(body),
                     'head': body[:600], 'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        return {'url': url[:80], 'status': e.code, 'err': True, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'url': url[:80], 'status': 'EXC', 'err': str(e)[:150], 'elapsed_ms': int((time.time()-t0)*1000)}


def lambda_handler(event, context):
    out = {'probes': {}}

    # Try various FMP endpoints
    fmp_tests = {
        'fmp_sp500': f'https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={FMP_KEY}',
        'fmp_sp500_historical': f'https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey={FMP_KEY}',
        'fmp_symbol_list': f'https://financialmodelingprep.com/api/v3/stock/list?apikey={FMP_KEY}',
    }
    for name, url in fmp_tests.items():
        out['probes'][name] = probe(url)
        # Try parsing as JSON
        if out['probes'][name].get('status') == 200:
            try:
                data = json.loads(out['probes'][name]['head'])
                if isinstance(data, list) and data:
                    out['probes'][name]['parsed_len'] = len(data) if len(data) < 9999 else 'truncated'
                    out['probes'][name]['first_item'] = data[0] if data else None
            except: pass

    # GitHub datahub
    out['probes']['github_datahub'] = probe(
        'https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv',
        {'User-Agent': 'JustHodl/1.0'})

    # Wikipedia
    out['probes']['wikipedia'] = probe(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
        {'User-Agent': 'JustHodl/1.0'})

    # Check if there's an existing S3 file with sp500 data
    try:
        s3 = boto3.client('s3', region_name='us-east-1')
        # Check known screener output paths
        for key in ['data/stock-screener.json', 'data/screener.json', 'data/sp500.json',
                    'data/screener/sp500.json', 'data/screener-latest.json', 'screener/data.json']:
            try:
                obj = s3.head_object(Bucket='justhodl-dashboard-live', Key=key)
                out['probes'][f's3:{key}'] = {'status': 200, 'size_kb': round(obj['ContentLength']/1024, 1),
                                                'modified': obj['LastModified'].isoformat()[:19]}
            except: pass
    except Exception as e:
        out['s3_err'] = str(e)[:150]

    return {'statusCode': 200, 'body': json.dumps(out)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    PROBE_NAME = "justhodl-tmp-sp500-probe"

    # Get FMP_KEY from finra-short (we just put it there)
    cfg = lam.get_function_configuration(FunctionName="justhodl-finra-short")
    env = (cfg.get("Environment") or {}).get("Variables", {})

    try:
        lam.create_function(
            FunctionName=PROBE_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=512, Timeout=60, Code={"ZipFile": zip_str(PROBE_CODE)},
            Environment={"Variables": {"FMP_KEY": env.get("FMP_KEY", "")}},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=PROBE_NAME)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=PROBE_NAME, ZipFile=zip_str(PROBE_CODE))
        lam.get_waiter("function_updated").wait(FunctionName=PROBE_NAME)
        lam.update_function_configuration(FunctionName=PROBE_NAME,
            Environment={"Variables": {"FMP_KEY": env.get("FMP_KEY", "")}})
        lam.get_waiter("function_updated").wait(FunctionName=PROBE_NAME)

    _time.sleep(3)
    r = lam.invoke(FunctionName=PROBE_NAME, InvocationType="RequestResponse",
                    Payload=b"{}")
    body = r["Payload"].read().decode("utf-8")
    p = json.loads(body)
    out["result"] = json.loads(p["body"]) if p.get("body") else p

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
