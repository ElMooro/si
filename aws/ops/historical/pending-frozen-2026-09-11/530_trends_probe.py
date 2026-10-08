#!/usr/bin/env python3
"""530 — Probe Google Trends data sources from AWS Lambda.

Google Trends doesn't have a public API. Options:
  1. pytrends library (unofficial, scrapes trends.google.com — often blocked from AWS)
  2. SerpAPI (paid)
  3. trends.google.com direct JSON endpoints (rate-limited but free)
  4. Datawrapper or other aggregators
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/530_trends_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = r"""
import json, urllib.request, urllib.error, time
import urllib.parse

UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0'

def http_get(url, timeout=15, extra_headers=None):
    t0 = time.time()
    headers = {'User-Agent': UA, 'Accept': 'application/json,text/html'}
    if extra_headers: headers.update(extra_headers)
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            txt = body.decode('utf-8','replace')
            return {'status': r.status, 'bytes': len(body),
                     'preview': txt[:2500],
                     'elapsed_ms': int((time.time()-t0)*1000),
                     'cookies': dict(r.getheaders()).get('Set-Cookie', '')[:200]}
    except urllib.error.HTTPError as e:
        body = e.read()[:500].decode('utf-8','replace') if hasattr(e,'read') else ''
        return {'status': e.code, 'err_body': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'status':'EXC','err':str(e)[:200]}

def google_trends_explore(query, geo='US', timeframe='now 7-d'):
    """Use Google Trends explore JSON endpoint (no auth, but rate-limited)."""
    base = 'https://trends.google.com/trends/api/explore'
    params = {
        'hl': 'en-US',
        'tz': '0',
        'req': json.dumps({
            'comparisonItem': [{'keyword': query, 'geo': geo, 'time': timeframe}],
            'category': 0,
            'property': '',
        }),
    }
    url = base + '?' + urllib.parse.urlencode(params)
    return http_get(url)

def google_daily_trends(geo='US'):
    """Daily trending searches feed (free, no auth needed)."""
    url = f'https://trends.google.com/trends/api/dailytrends?hl=en-US&tz=0&geo={geo}'
    return http_get(url)

def google_realtime_trends(geo='US'):
    """Realtime trending searches."""
    url = f'https://trends.google.com/trends/api/realtimetrends?hl=en-US&tz=0&cat=all&fi=0&fs=0&geo={geo}&ri=300&rs=20&sort=0'
    return http_get(url)

def lambda_handler(event, context):
    out = {}

    # PROBE 1: Google Trends daily trending feed (no auth)
    out["daily_trends_US"] = google_daily_trends('US')
    out["daily_trends_GB"] = google_daily_trends('GB')

    # PROBE 2: Realtime trending
    out["realtime_trends_US"] = google_realtime_trends('US')

    # PROBE 3: Specific search query (e.g. "NVDA" interest over past 7 days)
    out["explore_NVDA_7d"] = google_trends_explore('NVDA', 'US', 'now 7-d')

    # PROBE 4: Search via simpler suggest endpoint
    out["suggest_NVDA"] = http_get('https://trends.google.com/trends/api/autocomplete/NVDA?hl=en-US&tz=0')

    return {'statusCode': 200, 'body': json.dumps(out, default=str)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-trends-probe"
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
        except: out["raw"] = body[:3000]
    except Exception as e:
        out["err"] = str(e)[:400]

    try: lam.delete_function(FunctionName=NAME)
    except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
