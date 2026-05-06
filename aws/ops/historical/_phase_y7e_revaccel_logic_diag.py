"""Phase Y7e — Run revenue acceleration logic on AAPL with detailed tracing."""
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
import json, urllib.request, time

FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

def fetch_url(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Diag/1.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def lambda_handler(event=None, context=None):
    out = {"trace": []}
    
    # Replicate evaluate_ticker step by step on AAPL
    sym = "AAPL"
    
    # Fetch quarters
    url = "https://financialmodelingprep.com/stable/income-statement?symbol=" + sym + "&period=quarter&limit=8&apikey=" + FMP_KEY
    quarters = fetch_url(url)
    
    out["trace"].append("fetched " + str(len(quarters)) + " quarters")
    if len(quarters) < 5:
        out["result"] = "rejected: < 5 quarters"
        return {"statusCode": 200, "body": json.dumps(out, default=str)}
    
    # Sort newest first
    quarters_sorted = sorted(quarters, key=lambda q: q.get("date", ""), reverse=True)
    out["trace"].append("first quarter date: " + str(quarters_sorted[0].get("date")))
    out["trace"].append("last quarter date: " + str(quarters_sorted[-1].get("date")))
    
    # YoY growth computation
    yoy_growth = []
    for i in range(min(4, len(quarters_sorted) - 4)):
        cur_rev = quarters_sorted[i].get("revenue") or 0
        ago_rev = quarters_sorted[i + 4].get("revenue") or 0
        out["trace"].append("i=" + str(i) + " cur_rev=" + str(cur_rev) + " ago_rev=" + str(ago_rev))
        if ago_rev > 0:
            growth = (cur_rev - ago_rev) / abs(ago_rev) * 100
            yoy_growth.append({
                "quarter_end": quarters_sorted[i].get("date"),
                "revenue": cur_rev,
                "ago_revenue": ago_rev,
                "yoy_pct": growth,
            })
    
    out["trace"].append("yoy_growth length: " + str(len(yoy_growth)))
    if len(yoy_growth) < 2:
        out["result"] = "rejected: yoy_growth < 2"
        return {"statusCode": 200, "body": json.dumps(out, default=str)}
    
    out["yoy_growth"] = yoy_growth
    out["result"] = "ok"
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-revaccel-logic-temp"
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

    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ " + resp["errorMessage"][:300])
    else:
        d = json.loads(resp.get("body", "{}"))
        for line in d.get("trace", []):
            log("  " + line)
        log("  result: " + str(d.get("result")))
        if d.get("yoy_growth"):
            for y in d["yoy_growth"][:4]:
                log("    " + json.dumps(y, default=str))

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
    with open(os.path.join(out, "phase_y7e_revaccel_logic.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
