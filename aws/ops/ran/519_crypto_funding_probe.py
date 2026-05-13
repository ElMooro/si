#!/usr/bin/env python3
"""519 — Probe OKX + Bybit perp funding/OI/ticker endpoints from Lambda."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/519_crypto_funding_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = """
import json, urllib.request, time

URLS = [
    # OKX
    'https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP',
    'https://www.okx.com/api/v5/public/funding-rate-history?instId=BTC-USDT-SWAP&limit=10',
    'https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=BTC-USDT-SWAP',
    'https://www.okx.com/api/v5/market/index-tickers?instId=BTC-USDT',
    # Bybit v5
    'https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT',
    'https://api.bybit.com/v5/market/funding/history?category=linear&symbol=BTCUSDT&limit=5',
    'https://api.bybit.com/v5/market/open-interest?category=linear&symbol=BTCUSDT&intervalTime=1h&limit=5',
    # Bitmex liquidation (free)
    'https://www.bitmex.com/api/v1/liquidation?symbol=XBTUSD&count=5&reverse=true',
    # Coinalyze public
    'https://api.coinalyze.net/v1/exchanges',
]

def probe(url):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh) Chrome/120 Safari/537.36',
            'Accept': 'application/json',
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read()
            head = body[:500].decode('utf-8','replace')
            return {'url': url[-90:], 'status': r.status, 'bytes': len(body),
                     'head': head, 'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        return {'url': url[-90:], 'status': e.code, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'url': url[-90:], 'status': 'EXC', 'err': str(e)[:200],
                 'elapsed_ms': int((time.time()-t0)*1000)}

def lambda_handler(event, context):
    return {'statusCode': 200, 'body': json.dumps({'probes': [probe(u) for u in URLS]})}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-crypto-probe"
    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=512, Timeout=60, Code={"ZipFile": zip_str(PROBE_CODE)},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=NAME, ZipFile=zip_str(PROBE_CODE))
        lam.get_waiter("function_updated").wait(FunctionName=NAME)

    _time.sleep(2)
    try:
        r = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                        Payload=b"{}")
        body = r["Payload"].read().decode("utf-8")
        p = json.loads(body)
        out["result"] = json.loads(p["body"]) if p.get("body") else p
    except Exception as e:
        out["err"] = str(e)[:300]

    try: lam.delete_function(FunctionName=NAME)
    except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
