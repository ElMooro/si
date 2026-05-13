#!/usr/bin/env python3
"""515 — Probe squeezemetrics DIX CSV from Lambda to verify availability + format."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/515_dix_probe.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = """
import json, urllib.request, time

URLS = [
    'https://squeezemetrics.com/monitor/static/DIX.csv',
    'https://squeezemetrics.com/monitor/download/dix.csv',
    'https://squeezemetrics.com/data/dix.csv',
    'https://squeezemetrics.com/dix',
]

def probe(url):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh) Chrome/120 Safari/537.36',
            'Accept': 'text/csv,application/octet-stream,*/*',
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read()
            txt = body[:600].decode('utf-8', 'replace')
            return {'url': url, 'status': r.status, 'bytes': len(body),
                     'head': txt, 'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        body = e.read()[:200].decode('utf-8','replace') if hasattr(e,'read') else ''
        return {'url': url, 'status': e.code, 'body_preview': body, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'url': url, 'status': 'EXC', 'err': str(e)[:200], 'elapsed_ms': int((time.time()-t0)*1000)}

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
    NAME = "justhodl-tmp-dix-probe"
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
                        LogType="Tail", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8")
        p = json.loads(body)
        out["result"] = json.loads(p["body"]) if p.get("body") else p
        if r.get("LogResult"):
            out["log_tail"] = base64.b64decode(r["LogResult"]).decode("utf-8","replace")[-1500:]
    except Exception as e:
        out["err"] = str(e)[:400]

    try: lam.delete_function(FunctionName=NAME)
    except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
