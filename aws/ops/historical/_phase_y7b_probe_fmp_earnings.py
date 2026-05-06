"""Phase Y7b — Probe FMP for the right earnings-surprises endpoint."""
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

def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Test/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
            return {"ok": True, "url": url[-130:],
                    "n": len(d) if isinstance(d, list) else 1,
                    "sample": d[:2] if isinstance(d, list) else [d]}
    except urllib.error.HTTPError as e:
        return {"ok": False, "url": url[-130:], "status": e.code,
                "body": e.read().decode("utf-8", "replace")[:200]}
    except Exception as e:
        return {"ok": False, "url": url[-130:], "error": str(e)}


def lambda_handler(event=None, context=None):
    base = "https://financialmodelingprep.com/stable"
    
    tests = [
        # Try various earnings endpoint names
        ("earnings-surprises AAPL",
         base + "/earnings-surprises?symbol=AAPL&apikey=" + FMP_KEY),
        ("earnings-surprises-bulk AAPL",
         base + "/earnings-surprises-bulk?symbol=AAPL&apikey=" + FMP_KEY),
        ("historical-earnings-surprises AAPL",
         base + "/historical-earnings-surprises?symbol=AAPL&apikey=" + FMP_KEY),
        ("earnings AAPL",
         base + "/earnings?symbol=AAPL&apikey=" + FMP_KEY),
        ("earnings-historical AAPL",
         base + "/earnings-historical?symbol=AAPL&apikey=" + FMP_KEY),
        ("earnings-calendar",
         base + "/earnings-calendar?from=2026-04-15&to=2026-05-15&apikey=" + FMP_KEY),
        ("earning-calendar (singular)",
         base + "/earning-calendar?from=2026-04-15&to=2026-05-15&apikey=" + FMP_KEY),
        ("earnings-confirmed",
         base + "/earnings-confirmed?from=2026-04-15&to=2026-05-15&apikey=" + FMP_KEY),
        ("earnings-confirmed AAPL",
         base + "/earnings-confirmed?symbol=AAPL&apikey=" + FMP_KEY),
        # Try income-statement which has eps actual
        ("income-statement quarterly AAPL",
         base + "/income-statement?symbol=AAPL&period=quarter&limit=4&apikey=" + FMP_KEY),
        # Try v3 versions
        ("v3 earnings-surprises",
         "https://financialmodelingprep.com/api/v3/earnings-surprises/AAPL?apikey=" + FMP_KEY),
        ("v3 historical-earnings-surprises",
         "https://financialmodelingprep.com/api/v3/historical/earning_calendar/AAPL?apikey=" + FMP_KEY),
        ("v4 earnings-surprises",
         "https://financialmodelingprep.com/api/v4/earnings-surprises?symbol=AAPL&apikey=" + FMP_KEY),
    ]
    
    return {"statusCode": 200, "body": json.dumps([fetch(u) for label, u in tests], default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-pead-test-temp"
    try:
        L.get_function(FunctionName=NAME)
        L.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
    except L.exceptions.ResourceNotFoundException:
        L.create_function(FunctionName=NAME, Runtime="python3.12",
                           Handler="lambda_function.lambda_handler",
                           Role=ROLE_ARN, Code={"ZipFile": buf.getvalue()},
                           Timeout=180, MemorySize=512)
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
        results = json.loads(resp.get("body", "[]"))
        for res in results:
            mark = "✅" if res.get("ok") else "❌"
            if res.get("ok"):
                log("  " + mark + " " + res["url"])
                log("       n={}, sample={}".format(res.get("n"), 
                    str(res.get("sample"))[:200]))
            else:
                log("  " + mark + " " + res["url"])
                log("       status={} body={}".format(
                    res.get("status"),
                    (res.get("body") or res.get("error") or "")[:120]))

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
    with open(os.path.join(out, "phase_y7b_fmp_earnings_probe.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
