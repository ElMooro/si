#!/usr/bin/env python3
"""Step 464 — Identify the 5 funds silently dropped from the live sidecar.

Stage 16.5 verify (ops 463) showed:
  Funds attempted: 68
  Funds successful: 63
  Missing: 5

Approach:
  1. Read the live sidecar funds list
  2. Compare against CONCENTRATED_FUNDS in Lambda source
  3. For each missing CIK, retest extract endpoint to see if FMP has data
  4. If FMP has data → Lambda bug; if not → genuine FMP gap
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/464_dropped_funds.json"
NAME = "justhodl-tmp-464"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

# Full list of 68 from the Lambda source (mirror)
EXPECTED_CIKS = [
    "0001067983","0001336528","0000921669","0001517137","0001345471",
    "0001647251","0001656456","0000898382","0001135778","0001138995",
    "0001387322","0001747057","0000949509","0000909661","0001325447",
    "0000807249","0001512857","0001448574","0002051323","0001562230",
    "0002054122","0000905567","0000936753","0001353316","0001835549",
    "0001897612","0001535630","0000934639","0001720792","0001549575",
    "0000883965","0000807985","0001056831","0001569049","0001510387",
    "0001478735","0001993888","0001067983","0001336528","0001345471",
    "0001647251","0001656456","0000898382","0001135778","0001138995",
    # … actually let me just have the diagnostic Lambda read from S3 + compare
]

DIAG = r'''
import json, urllib.request
from concurrent.futures import ThreadPoolExecutor
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP_BASE = "https://financialmodelingprep.com/stable"

# Read the live Lambda source from GitHub to get authoritative CONCENTRATED_FUNDS
GITHUB_RAW = "https://raw.githubusercontent.com/ElMooro/si/main/aws/lambdas/justhodl-smart-money-holdings/source/lambda_function.py"


def fetch_extract(cik):
    for year, quarter in [(2025, 4), (2025, 3)]:
        url = f"{FMP_BASE}/institutional-ownership/extract?cik={cik}&year={year}&quarter={quarter}&apikey={FMP}"
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
                timeout=8) as r:
                d = json.loads(r.read().decode("utf-8"))
            if isinstance(d, list) and d:
                n_with_sym = sum(1 for x in d if x.get("symbol"))
                return {"year": year, "quarter": quarter,
                        "n_records": len(d), "n_with_sym": n_with_sym,
                        "first_sym": d[0].get("symbol") if d else None,
                        "first_value": d[0].get("value") if d else None}
        except Exception as e:
            continue
    return None


def fetch_perf(cik):
    url = f"{FMP_BASE}/institutional-ownership/holder-performance-summary?cik={cik}&apikey={FMP}"
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent":"JH/1.0"}),
            timeout=8) as r:
            d = json.loads(r.read().decode("utf-8"))
        if isinstance(d, list) and d:
            d.sort(key=lambda x: x.get("date",""), reverse=True)
            return {"mv": d[0].get("marketValue"), "size": d[0].get("portfolioSize"),
                    "name": d[0].get("investorName"), "date": d[0].get("date","")[:10]}
    except Exception:
        pass
    return None


def lambda_handler(event, context):
    out = {}
    # 1. Pull authoritative CONCENTRATED_FUNDS list from GitHub
    try:
        req = urllib.request.Request(GITHUB_RAW,
            headers={"User-Agent": "JH/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            src = r.read().decode("utf-8")
        # Parse CONCENTRATED_FUNDS = [...]
        import re
        m = re.search(r"CONCENTRATED_FUNDS\s*=\s*\[(.*?)^\]", src, re.MULTILINE | re.DOTALL)
        if not m:
            out["err"] = "couldnt find CONCENTRATED_FUNDS in source"
            return {"statusCode": 200, "body": json.dumps(out)}
        block = m.group(1)
        # Extract tuples (cik, label)
        tuples = re.findall(r'\("(\d+)",\s*"([^"]+)"\)', block)
        expected = [{"cik": c, "label": l} for c, l in tuples]
        out["expected_count"] = len(expected)
    except Exception as e:
        out["github_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out)}

    # 2. Read live sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="screener/smart-money-holdings.json")
        p = json.loads(obj["Body"].read())
        live_funds = p.get("funds") or []
        live_ciks = {f["cik"] for f in live_funds}
        out["live_count"] = len(live_funds)
    except Exception as e:
        out["s3_err"] = str(e)[:200]
        return {"statusCode": 200, "body": json.dumps(out)}

    # 3. Find missing
    missing = [e for e in expected if e["cik"] not in live_ciks]
    out["missing_count"] = len(missing)
    out["missing_list"] = [{"cik": m["cik"], "label": m["label"]} for m in missing]

    # 4. Validate each missing one against FMP fresh
    def diagnose(m):
        cik = m["cik"]
        extract = fetch_extract(cik)
        perf = fetch_perf(cik)
        if extract:
            verdict = "FMP_HAS_DATA — Lambda bug? Retry in next run."
        elif perf:
            verdict = "FMP has perf but no extract — fund may file 13F-NT (notice) not 13F-HR"
        else:
            verdict = "FMP has neither — CIK invalid or stale"
        return {**m, "verdict": verdict, "extract": extract, "perf": perf}

    with ThreadPoolExecutor(max_workers=8) as ex:
        out["diagnostics"] = list(ex.map(diagnose, missing))

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=300, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
