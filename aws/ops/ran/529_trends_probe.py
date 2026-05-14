#!/usr/bin/env python3
"""529 — Probe Google Trends, GDELT, Yahoo trending, StockAnalysis for BUILD 10."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/529_trends_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = r"""
import json, urllib.request, urllib.error, time, os

def http_get(url, timeout=15, headers=None):
    t0 = time.time()
    hdr = {'User-Agent':'Mozilla/5.0 (Macintosh) Chrome/120 Safari/537.36',
           'Accept':'application/json, text/html, */*'}
    if headers: hdr.update(headers)
    try:
        req = urllib.request.Request(url, headers=hdr)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            txt = body.decode('utf-8', 'replace')
            return {'status': r.status, 'bytes': len(body),
                    'preview': txt[:2000],
                    'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        body = e.read()[:500].decode('utf-8','replace') if hasattr(e,'read') else ''
        return {'status': e.code, 'err_body': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'status':'EXC','err':str(e)[:200]}


def lambda_handler(event, context):
    out = {}

    # Google Trends dailytrends API (XSSI prefix stripped before parse)
    out['google_dailytrends'] = http_get(
        "https://trends.google.com/trends/api/dailytrends?geo=US&hl=en-US")

    # Google Trends related queries
    out['google_realtime_us'] = http_get(
        "https://trends.google.com/trends/api/realtimetrends?geo=US&cat=all")

    # GDELT 2.0 GKG (Global Knowledge Graph) — search articles by topic
    out['gdelt_doc_aapl_24h'] = http_get(
        "https://api.gdeltproject.org/api/v2/doc/doc?query=AAPL&mode=ArtList&maxrecords=10&format=json&timespan=24h")

    # GDELT 2.0 TimelineVolinfo (article volume timeline)
    out['gdelt_volume_aapl_30d'] = http_get(
        "https://api.gdeltproject.org/api/v2/doc/doc?query=AAPL&mode=TimelineVolInfo&timespan=30d&format=json")

    # GDELT 2.0 Tone over time (sentiment)
    out['gdelt_tone_aapl_30d'] = http_get(
        "https://api.gdeltproject.org/api/v2/doc/doc?query=Apple%20Inc&mode=TimelineTone&timespan=30d&format=json")

    # Yahoo Finance trending tickers
    out['yahoo_trending_us'] = http_get(
        "https://query1.finance.yahoo.com/v1/finance/trending/US?count=20")

    # StockAnalysis trending (web HTML scrape)
    out['stockanalysis_trending'] = http_get(
        "https://stockanalysis.com/trending/", timeout=10)

    # Reddit r/all top (not WSB, broader)
    out['reddit_all_top_day'] = http_get(
        "https://www.reddit.com/r/all/top/.json?t=day&limit=10",
        headers={'User-Agent':'JustHodl-Trends/1.0'})

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
