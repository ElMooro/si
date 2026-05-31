#!/usr/bin/env python3
"""1049 — find the best Quiver Quant endpoint shape for Congress data.

We know /beta/historical/congresstrading/{TICKER} works (482KB for AAPL
with 1093 records). We want to find either:
  a) A bulk/recent endpoint that returns all-tickers recent trades, OR
  b) Confirm we have to iterate per-ticker
"""
import io, json, os, pathlib, time, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1049_quiver_endpoints.json"
REGION = "us-east-1"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)
long_lam = boto3.client("lambda", region_name=REGION,
                          config=Config(read_timeout=120))


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    probe_code = r"""
import urllib.request, json

def probe(label, url):
    h = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl/1.0; raafouis@gmail.com)",
         "Accept": "application/json"}
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=20) as r:
            code = r.status
            body = r.read()
            result = {"status": code, "size": len(body)}
            try:
                data = json.loads(body)
                if isinstance(data, list):
                    result["n_items"] = len(data)
                    if data and isinstance(data[0], dict):
                        result["sample_keys"] = list(data[0].keys())[:15]
                        result["sample_item"] = {k: str(v)[:60] for k, v in list(data[0].items())[:8]}
                elif isinstance(data, dict):
                    result["top_keys"] = list(data.keys())[:15]
            except Exception as je:
                result["parse_err"] = str(je)[:100]
                result["first_300"] = body[:300].decode("utf-8", errors="replace")
            return result
    except Exception as e:
        return {"err": str(e)[:200]}

results = {}

# Variants that might give recent across all tickers
endpoints = [
    ("live_congress",           "https://api.quiverquant.com/beta/live/congresstrading"),
    ("bulk_congress",           "https://api.quiverquant.com/beta/bulk/congresstrading"),
    ("recent_congress",         "https://api.quiverquant.com/beta/recent/congresstrading"),
    ("congresstrading_root",    "https://api.quiverquant.com/beta/congresstrading"),
    ("historical_root",         "https://api.quiverquant.com/beta/historical/congresstrading"),
    # Per-ticker spot check on a different ticker
    ("historical_nvda",         "https://api.quiverquant.com/beta/historical/congresstrading/NVDA"),
    # Senate / House split
    ("live_senate",             "https://api.quiverquant.com/beta/live/senatetrading"),
    ("live_house",              "https://api.quiverquant.com/beta/live/housetrading"),
    # Other Quiver datasets
    ("live_insiders",           "https://api.quiverquant.com/beta/live/insiders"),
    ("live_lobbying",           "https://api.quiverquant.com/beta/live/lobbying"),
]

for label, url in endpoints:
    results[label] = probe(label, url)

print("__RESULTS__" + json.dumps(results, default=str) + "__END__")
"""
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", f"""
def lambda_handler(event, context):
{chr(10).join('    ' + line for line in probe_code.strip().split(chr(10)))}
    return {{"ok": True}}
""")
    
    tmp_name = "justhodl-tmp-1049-probe"
    try:
        lam.delete_function(FunctionName=tmp_name)
        time.sleep(2)
    except Exception:
        pass
    
    lam.create_function(
        FunctionName=tmp_name,
        Runtime="python3.12",
        Role=ROLE_ARN,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": buf.getvalue()},
        Timeout=120, MemorySize=512, Publish=False,
    )
    lam.get_waiter("function_active").wait(FunctionName=tmp_name)
    
    r = long_lam.invoke(FunctionName=tmp_name,
                          InvocationType="RequestResponse", Payload=b"{}",
                          LogType="Tail")
    log_tail = base64.b64decode(r["LogResult"]).decode("utf-8", errors="replace") if "LogResult" in r else ""
    
    start = log_tail.find("__RESULTS__")
    end = log_tail.find("__END__")
    if start >= 0 and end > start:
        try:
            out["probe"] = json.loads(log_tail[start + 11:end])
        except Exception as e:
            out["parse_err"] = str(e)[:200]
            out["raw"] = log_tail[start + 11:end][:3000]
    else:
        out["log_tail"] = log_tail[-3000:]
    
    try:
        lam.delete_function(FunctionName=tmp_name)
    except Exception:
        pass
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
