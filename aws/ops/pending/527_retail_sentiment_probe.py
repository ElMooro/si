#!/usr/bin/env python3
"""527 — Probe retail sentiment sources from AWS Lambda IP space.

Tests:
  1. apewisdom.io (pre-aggregated Reddit/StockTwits mention counts)
  2. reddit.com JSON endpoints (no auth needed but rate-limited by IP)
  3. stocktwits.com streams (no auth for basic stream)
  4. tradestie.com WSB API (alternative aggregator)
"""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/527_retail_sentiment_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = r"""
import json, urllib.request, urllib.error, time

UAS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'JustHodlAI/1.0 (financial-research)',
]

def http_get(url, timeout=15, ua=UAS[0], extra_headers=None):
    t0 = time.time()
    headers = {'User-Agent': ua, 'Accept': 'application/json,text/html;q=0.9,*/*;q=0.8'}
    if extra_headers: headers.update(extra_headers)
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            ct = r.headers.get('content-type', '')
            txt = body.decode('utf-8', 'replace') if 'text' in ct or 'json' in ct else f'<binary {len(body)}b>'
            return {'status': r.status, 'bytes': len(body),
                     'content_type': ct, 'preview': txt[:2500],
                     'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        body = e.read()[:500].decode('utf-8','replace') if hasattr(e,'read') else ''
        return {'status': e.code, 'err_body': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'status': 'EXC', 'err': str(e)[:200]}


def lambda_handler(event, context):
    out = {}

    # ─── apewisdom.io (Reddit + StockTwits aggregator) ───
    out["apewisdom_all"] = http_get(
        "https://apewisdom.io/api/v1.0/filter/all-stocks/page/1")
    out["apewisdom_wsb"] = http_get(
        "https://apewisdom.io/api/v1.0/filter/wallstreetbets/page/1")
    out["apewisdom_stocks"] = http_get(
        "https://apewisdom.io/api/v1.0/filter/stocks/page/1")
    out["apewisdom_crypto"] = http_get(
        "https://apewisdom.io/api/v1.0/filter/crypto/page/1")

    # ─── tradestie.com (WSB ticker mentions/sentiment) ───
    out["tradestie_wsb"] = http_get(
        "https://tradestie.com/api/v1/apps/reddit")

    # ─── reddit.com direct (typically blocked from AWS IPs) ───
    out["reddit_wsb_hot"] = http_get(
        "https://www.reddit.com/r/wallstreetbets/hot.json?limit=10",
        ua=UAS[1])
    out["reddit_wsb_top"] = http_get(
        "https://www.reddit.com/r/wallstreetbets/top.json?t=day&limit=10",
        ua=UAS[1])

    # ─── stocktwits.com (free stream endpoint) ───
    out["stocktwits_aapl"] = http_get(
        "https://api.stocktwits.com/api/2/streams/symbol/AAPL.json")
    out["stocktwits_trending"] = http_get(
        "https://api.stocktwits.com/api/2/trending/symbols.json")

    # ─── sentilink.io (FYI; may be paywall) ───
    # Skip — known paywall

    return {'statusCode': 200, 'body': json.dumps(out, default=str)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-retail-probe"
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=256, Timeout=60, Code={"ZipFile": zip_str(PROBE_CODE)})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=NAME, ZipFile=zip_str(PROBE_CODE))
        lam.get_waiter("function_updated").wait(FunctionName=NAME)

    _time.sleep(2)
    try:
        r = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["probes"] = json.loads(p["body"]) if p.get("body") else p
        except: out["raw"] = body[:2500]
    except Exception as e:
        out["err"] = str(e)[:400]

    try: lam.delete_function(FunctionName=NAME)
    except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
