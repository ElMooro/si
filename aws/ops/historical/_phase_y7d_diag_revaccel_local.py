"""Phase Y7d — Diagnose why rev-accel returns 0.
Test the EXACT logic of evaluate_ticker on a known-good name like AAPL."""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)


def main():
    code = r'''
import json, urllib.request, urllib.error, time

FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

def fetch_url(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-RevAccel/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def lambda_handler(event=None, context=None):
    out = {}
    
    # Test 1: same URL the rev-accel Lambda uses
    sym = "AAPL"
    url = "https://financialmodelingprep.com/stable/income-statement?symbol=" + sym + "&period=quarter&limit=8&apikey=" + FMP_KEY
    out["test_url"] = url
    
    try:
        data = fetch_url(url, timeout=15)
        out["result"] = "ok"
        out["type"] = type(data).__name__
        out["is_list"] = isinstance(data, list)
        out["len"] = len(data) if isinstance(data, list) else None
        if isinstance(data, list) and data:
            out["first_keys"] = list(data[0].keys())[:25]
            # Check for the fields the rev-accel parser expects
            f = data[0]
            out["has_revenue"] = "revenue" in f and f.get("revenue") is not None
            out["has_grossProfit"] = "grossProfit" in f and f.get("grossProfit") is not None
            out["has_operatingExpenses"] = "operatingExpenses" in f and f.get("operatingExpenses") is not None
            out["has_eps"] = "eps" in f or "epsdiluted" in f
            out["sample_row"] = {k: v for k, v in list(f.items())[:15] if v is not None}
    except Exception as e:
        out["result"] = "error"
        out["error"] = str(e)
    
    # Test 2: try a few more symbols
    for s in ["NVDA", "AGIO", "AAOI", "AXTI", "CRDO"]:
        url = "https://financialmodelingprep.com/stable/income-statement?symbol=" + s + "&period=quarter&limit=8&apikey=" + FMP_KEY
        try:
            data = fetch_url(url, timeout=15)
            out["sym_" + s + "_n"] = len(data) if isinstance(data, list) else 0
        except Exception as e:
            out["sym_" + s + "_err"] = str(e)
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-revaccel-diag-temp"
    try:
        L.get_function(FunctionName=NAME)
        L.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
    except L.exceptions.ResourceNotFoundException:
        L.create_function(FunctionName=NAME, Runtime="python3.12",
                           Handler="lambda_function.lambda_handler",
                           Role=ROLE_ARN, Code={"ZipFile": buf.getvalue()},
                           Timeout=60, MemorySize=256)
    for _ in range(20):
        c = L.get_function_configuration(FunctionName=NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)

    log("  invoking...")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ " + resp["errorMessage"][:300])
    else:
        d = json.loads(resp.get("body", "{}"))
        for k, v in d.items():
            log("  " + k + ": " + json.dumps(v, default=str)[:200])

    try:
        L.delete_function(FunctionName=NAME)
        log("  ✓ deleted")
    except Exception:
        pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_y7d_revaccel_diag.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
