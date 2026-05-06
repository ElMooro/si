"""Phase Y3c — Test FMP endpoints for float data + microcap stock screener.
We need:
  1. FMP /share-float endpoint (or alternate float source)
  2. FMP /stock-screener with mktCap range to build microcap universe
"""
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

def fetch(url, label):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Test/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = r.read()
            j = json.loads(d)
            return {"label": label, "ok": True,
                    "n": len(j) if isinstance(j, list) else 1,
                    "sample": j[:3] if isinstance(j, list) else [j]}
    except urllib.error.HTTPError as e:
        return {"label": label, "ok": False, "status": e.code,
                "body": e.read().decode("utf-8", "replace")[:200]}
    except Exception as e:
        return {"label": label, "ok": False, "error": str(e)}


def lambda_handler(event=None, context=None):
    base = "https://financialmodelingprep.com/stable"
    
    tests = [
        # FLOAT DATA ENDPOINTS
        ("share-float AAPL",
         base + "/share-float?symbol=AAPL&apikey=" + FMP_KEY),
        
        ("share-float-latest",
         base + "/share-float-latest?apikey=" + FMP_KEY + "&limit=5"),
        
        ("shares-outstanding AAPL",
         base + "/shares-outstanding?symbol=AAPL&apikey=" + FMP_KEY),
        
        ("key-metrics AAPL (has float?)",
         base + "/key-metrics?symbol=AAPL&period=quarter&limit=1&apikey=" + FMP_KEY),
        
        # SCREENER ENDPOINTS (for building microcap universe)
        ("screener: mcap 50M-500M, US, NASDAQ",
         base + "/company-screener?marketCapMoreThan=50000000&marketCapLowerThan=500000000&isActivelyTrading=true&exchange=NASDAQ&country=US&limit=50&apikey=" + FMP_KEY),
        
        ("screener: mcap 500M-2B, US",
         base + "/company-screener?marketCapMoreThan=500000000&marketCapLowerThan=2000000000&isActivelyTrading=true&country=US&limit=50&apikey=" + FMP_KEY),
        
        # Alt float source
        ("profile-bulk AAPL,MSFT",
         base + "/profile-bulk?part=0&apikey=" + FMP_KEY),
        
        ("balance-sheet AAPL (has shares info?)",
         base + "/balance-sheet-statement?symbol=AAPL&period=quarter&limit=1&apikey=" + FMP_KEY),
    ]
    
    return {"statusCode": 200, "body": json.dumps([fetch(u, l) for l, u in tests], default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-fmp-test-temp"
    try:
        L.get_function(FunctionName=NAME)
        L.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
    except L.exceptions.ResourceNotFoundException:
        L.create_function(FunctionName=NAME, Runtime="python3.12",
                           Handler="lambda_function.lambda_handler",
                           Role=ROLE_ARN, Code={"ZipFile": buf.getvalue()},
                           Timeout=120, MemorySize=256)
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
                log("  " + mark + " {:<48} n={}".format(res["label"][:48], res.get("n")))
                samples = res.get("sample", [])[:2]
                for s in samples:
                    if isinstance(s, dict):
                        # Show key fields
                        keys_of_interest = ["symbol", "freeFloat", "floatShares",
                                            "outstandingShares", "sharesFloat",
                                            "sharesOutstanding", "marketCap",
                                            "price", "companyName", "exchange",
                                            "country", "industry"]
                        cherry = {k: s.get(k) for k in keys_of_interest if k in s}
                        log("       " + json.dumps(cherry, default=str)[:200])
                    else:
                        log("       " + str(s)[:200])
            else:
                log("  " + mark + " {:<48} status={}  body={}".format(
                    res["label"][:48], res.get("status"),
                    (res.get("body") or res.get("error") or "")[:150]))

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
    with open(os.path.join(out, "phase_y3c_test_fmp.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
