#!/usr/bin/env python3
"""529 — Probe Fed/ECB RSS + statement page endpoints to find a reliable source."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/529_cb_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = r"""
import json, urllib.request, urllib.error, time, re

def http_get(url, timeout=20):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml,application/rss+xml;q=0.9,*/*;q=0.8',
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            txt = body.decode('utf-8','replace')
            return {'status': r.status, 'bytes': len(body),
                     'preview': txt[:1500],
                     'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        body = e.read()[:300].decode('utf-8','replace') if hasattr(e,'read') else ''
        return {'status': e.code, 'err_body': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'status': 'EXC', 'err': str(e)[:200]}


def lambda_handler(event, context):
    out = {}
    # Fed
    out['fed_press_rss'] = http_get('https://www.federalreserve.gov/feeds/press_monetary.xml')
    out['fed_all_press_rss'] = http_get('https://www.federalreserve.gov/feeds/press_all.xml')
    out['fed_fomc_calendar'] = http_get('https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm')
    # FOMC statements pages - try direct
    out['fed_fomc_2026_jan'] = http_get('https://www.federalreserve.gov/newsevents/pressreleases/monetary20260128a.htm')
    out['fed_fomc_2026_mar'] = http_get('https://www.federalreserve.gov/newsevents/pressreleases/monetary20260318a.htm')
    out['fed_fomc_2026_may'] = http_get('https://www.federalreserve.gov/newsevents/pressreleases/monetary20260507a.htm')

    # ECB
    out['ecb_press_rss'] = http_get('https://www.ecb.europa.eu/press/pr/date/2026/html/index.en.html')
    out['ecb_monetary_decisions'] = http_get('https://www.ecb.europa.eu/press/press_conference/monetary-policy-statement/2026/html/index.en.html')

    # BoE
    out['boe_mpr'] = http_get('https://www.bankofengland.co.uk/news/rss?Taxonomies=00000168-3eb3-da42-ad7d-bff768870000')

    # BoJ
    out['boj_news'] = http_get('https://www.boj.or.jp/en/whatsnew/rss/index.html')

    return {'statusCode': 200, 'body': json.dumps(out, default=str)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-cb-probe"
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=256, Timeout=120, Code={"ZipFile": zip_str(PROBE_CODE)})
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
