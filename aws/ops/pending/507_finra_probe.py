#!/usr/bin/env python3
"""507 — Probe FINRA short volume CDN to confirm format + URL conventions."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/507_finra_probe.json"

PROBE_CODE = """
import json, urllib.request, time
from datetime import datetime, timedelta

def probe(url):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible) JustHodl/1.0',
            'Accept': 'text/plain,*/*'})
        with urllib.request.urlopen(req, timeout=12) as r:
            body = r.read().decode('utf-8', 'replace')
            return {'url': url, 'status': r.status, 'bytes': len(body),
                     'lines': body.count(chr(10)) + 1,
                     'head': body[:600],
                     'sample_lines': body.split(chr(10))[1:6],
                     'elapsed_ms': int((time.time()-t0)*1000)}
    except urllib.error.HTTPError as e:
        return {'url': url, 'status': e.code, 'err': True, 'elapsed_ms': int((time.time()-t0)*1000)}
    except Exception as e:
        return {'url': url, 'status': 'EXC', 'err': str(e)[:200], 'elapsed_ms': int((time.time()-t0)*1000)}

def lambda_handler(event, context):
    out = {'today_utc': datetime.now().strftime('%Y-%m-%d'), 'probes': []}

    # Try the last several business days (FINRA publishes after market close ET, ~7:45pm)
    today = datetime.now()
    for n in range(0, 7):
        d = today - timedelta(days=n)
        # Skip weekends
        if d.weekday() >= 5: continue
        ds = d.strftime('%Y%m%d')
        # Try Consolidated NMS file (the main one)
        out['probes'].append({'tried_date': ds, 'kind': 'CNMS',
            **probe(f'https://cdn.finra.org/equity/regsho/daily/CNMSshvol{ds}.txt')})
    # Also try alternative file names
    yesterday = (today - timedelta(days=1)).strftime('%Y%m%d')
    out['probes_alternatives'] = [
        {'kind': 'FNYX (NYSE)', **probe(f'https://cdn.finra.org/equity/regsho/daily/FNYXshvol{yesterday}.txt')},
        {'kind': 'FNSQ (Nasdaq)', **probe(f'https://cdn.finra.org/equity/regsho/daily/FNSQshvol{yesterday}.txt')},
        {'kind': 'FNRA (FINRA TRF)', **probe(f'https://cdn.finra.org/equity/regsho/daily/FNRAshvol{yesterday}.txt')},
    ]
    return {'statusCode': 200, 'body': json.dumps(out)}
"""

lam = boto3.client("lambda", region_name="us-east-1")

def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()

def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    PROBE_NAME = "justhodl-tmp-finra-probe"
    try:
        lam.create_function(
            FunctionName=PROBE_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=512, Timeout=60, Code={"ZipFile": zip_str(PROBE_CODE)},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=PROBE_NAME)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=PROBE_NAME, ZipFile=zip_str(PROBE_CODE))
        lam.get_waiter("function_updated").wait(FunctionName=PROBE_NAME)

    _time.sleep(2)
    try:
        r = lam.invoke(FunctionName=PROBE_NAME, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8")
        p = json.loads(body)
        out["result"] = json.loads(p["body"]) if p.get("body") else p
    except Exception as e:
        out["err"] = str(e)[:300]

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
