#!/usr/bin/env python3
"""559 — Test GDELT API directly from a temp Lambda for multiple tickers
to understand why 14/15 return 'no GDELT data'."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/559_gdelt_direct_probe.json"
NAME = "justhodl-temp-gdelt-probe"

ACCOUNT = "857687956942"
REGION = "us-east-1"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)

PROBE_CODE = '''
import json, urllib.request, urllib.parse, urllib.error, time

GDELT_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"

QUERIES = [
    ("AAPL", 'AAPL OR "Apple Inc"'),
    ("MSFT", 'MSFT OR "Microsoft"'),
    ("NVDA", 'NVDA OR "Nvidia"'),
    ("TSLA", 'TSLA OR "Tesla Inc"'),
    ("V",    '"Visa Inc"'),
    ("MA",   '"Mastercard"'),
    ("XOM",  'XOM OR "ExxonMobil"'),
]

def fetch_one(ticker, query, mode="TimelineVolInfo", timespan="30d"):
    enc = urllib.parse.quote(query)
    url = f"{GDELT_BASE}?query={enc}&mode={mode}&timespan={timespan}&format=json"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh) Chrome/120",
        "Accept": "application/json",
    })
    out = {"ticker": ticker, "query": query, "url": url}
    try:
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=15) as r:
            status = r.status
            body = r.read().decode("utf-8")
            elapsed = time.time() - t0
        out["status"] = status
        out["body_size"] = len(body)
        out["elapsed_s"] = round(elapsed, 2)
        try:
            data = json.loads(body)
            out["data_keys"] = list(data.keys())
            timeline = data.get("timeline", [])
            out["timeline_len"] = len(timeline)
            if timeline:
                ser = timeline[0]
                out["timeline_first_series"] = ser.get("series")
                pts = ser.get("data", [])
                out["timeline_n_points"] = len(pts)
                if pts:
                    out["timeline_first_3"] = pts[:3]
                    out["timeline_last_3"] = pts[-3:]
        except Exception as pe:
            out["parse_err"] = str(pe)[:120]
            out["body_excerpt"] = body[:400]
    except urllib.error.HTTPError as he:
        out["http_err_code"] = he.code
        try: out["http_err_body"] = he.read().decode("utf-8", "replace")[:400]
        except: pass
    except Exception as e:
        out["err"] = str(e)[:200]
    return out


def lambda_handler(event, context):
    results = []
    for ticker, query in QUERIES:
        results.append(fetch_one(ticker, query))
        time.sleep(2.5)  # gentle pace
    return {"results": results}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Create temp Lambda
    try:
        try: lam.delete_function(FunctionName=NAME)
        except: pass
        _time.sleep(2)

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("lambda_function.py", PROBE_CODE)

        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE,
            Code={"ZipFile": buf.getvalue()}, MemorySize=256, Timeout=120,
            Description="temp GDELT probe",
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
        out["lambda_create"] = "OK"

        _time.sleep(2)
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            out["results"] = json.loads(body).get("results")
        except:
            out["raw"] = body[:1500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2000:]

        try: lam.delete_function(FunctionName=NAME)
        except: pass
        out["cleanup"] = "OK"
    except Exception as e:
        out["err"] = str(e)[:300]
        try: lam.delete_function(FunctionName=NAME)
        except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
