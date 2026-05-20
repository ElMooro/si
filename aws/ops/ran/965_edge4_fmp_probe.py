"""ops 965: probe FMP /stable/historical-price-eod/full for SPY to see what comes back."""
import datetime as dt, json, os, urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120, connect_timeout=10))

FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

CHECKS = []
def add(n, ok, d): CHECKS.append({"name":n,"passed":ok,"detail":str(d)[:340]})

# 1. Direct FMP probe -- test 2 URL variants
urls = [
    ("base", f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=SPY&apikey={FMP_KEY}"),
    ("light", f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=SPY&apikey={FMP_KEY}"),
    ("v3", f"https://financialmodelingprep.com/api/v3/historical-price-full/SPY?apikey={FMP_KEY}"),
]
for label, url in urls:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ops/965"})
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode("utf-8", errors="ignore")
            j = json.loads(raw)
            if isinstance(j, list):
                n = len(j)
                sample_keys = list(j[0].keys())[:8] if j else []
                add(f"fmp.{label}", n>=50, f"type=list n={n} sample_keys={sample_keys}")
            elif isinstance(j, dict):
                if "historical" in j:
                    h = j.get("historical", [])
                    add(f"fmp.{label}", len(h)>=50, f"type=dict n_historical={len(h)} keys={list(j.keys())[:5]}")
                else:
                    add(f"fmp.{label}", False, f"type=dict keys={list(j.keys())[:8]}")
            else:
                add(f"fmp.{label}", False, f"type={type(j).__name__}")
    except Exception as e:
        add(f"fmp.{label}", False, str(e)[:200])

# 2. Inspect the deployed Edge #4 code; print first 60 lines of fmp_history-equivalent
try:
    fn = "justhodl-vol-target-unwind"
    info = lam.get_function(FunctionName=fn)
    code_url = info.get("Code", {}).get("Location", "")
    add("e4.code_location", bool(code_url), f"url_len={len(code_url)}")
    # Don't actually fetch the URL (it's signed; works fine but verbose). Just confirm.
except ClientError as e:
    add("e4.code_location", False, str(e)[:200])

# 3. Invoke Edge #4 with verbose response
try:
    r = lam.invoke(FunctionName="justhodl-vol-target-unwind",
                   InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    import base64 as b64
    log = b64.b64decode(r.get("LogResult","")).decode("utf-8","ignore")
    payload = r["Payload"].read().decode()
    # Extract just the print/error lines from log
    relevant = [l for l in log.split("\n") if any(k in l.lower() for k in ["fmp","spy","history","row","error","report"])]
    add("e4.invoke_with_logs", True, f"body={payload[:160]} relevant_logs={relevant[-8:]}")
except ClientError as e:
    add("e4.invoke_with_logs", False, str(e)[:240])

rep = {"ops":965,"run_at":dt.datetime.utcnow().isoformat()+"Z","checks":CHECKS,
       "summary":{"total":len(CHECKS),"passed":sum(1 for c in CHECKS if c['passed'])}}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/965_edge4_fmp_probe.json","w").write(json.dumps(rep,indent=2,default=str))
print(f"\n=== {rep['summary']['passed']}/{rep['summary']['total']} ===")
for c in CHECKS:
    print(f"  [{'OK' if c['passed'] else 'X '}] {c['name']:25} {c['detail'][:240]}")
