"""Phase Y7b — Diagnose why PEAD + rev-accel return 0 evaluated.
Likely: FMP /earnings-surprises and /income-statement endpoint paths changed."""
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
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Diag/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = r.read()
            j = json.loads(d)
            return {"label": label, "ok": True,
                    "n": len(j) if isinstance(j, list) else 1,
                    "sample_keys": list(j[0].keys())[:25] if isinstance(j, list) and j and isinstance(j[0], dict) else None,
                    "sample": j[:2] if isinstance(j, list) else [j]}
    except urllib.error.HTTPError as e:
        return {"label": label, "ok": False, "status": e.code,
                "body": e.read().decode("utf-8", "replace")[:200]}
    except Exception as e:
        return {"label": label, "ok": False, "error": str(e)}


def lambda_handler(event=None, context=None):
    base = "https://financialmodelingprep.com/stable"
    
    # Test multiple variants for both endpoints
    tests = [
        # PEAD: earnings-surprises
        ("earnings-surprises AAPL",
         base + "/earnings-surprises?symbol=AAPL&apikey=" + FMP_KEY),
        ("earnings-surprises-bulk",
         base + "/earnings-surprises-bulk?symbol=AAPL&limit=8&apikey=" + FMP_KEY),
        ("earnings AAPL",
         base + "/earnings?symbol=AAPL&apikey=" + FMP_KEY),
        ("earnings-calendar today",
         base + "/earnings-calendar?from=2026-04-01&to=2026-05-06&apikey=" + FMP_KEY),
        # Rev accel: income-statement
        ("income-statement AAPL quarter",
         base + "/income-statement?symbol=AAPL&period=quarter&limit=8&apikey=" + FMP_KEY),
        ("income-statement-quarterly AAPL",
         base + "/income-statement-quarterly?symbol=AAPL&limit=8&apikey=" + FMP_KEY),
        ("financials/income-statement AAPL",
         base + "/financials/income-statement?symbol=AAPL&period=quarter&apikey=" + FMP_KEY),
        # Quote sanity
        ("quote AAPL",
         base + "/quote?symbol=AAPL&apikey=" + FMP_KEY),
    ]
    
    return {"statusCode": 200, "body": json.dumps([fetch(u, l) for l, u in tests], default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-pead-diag-temp"
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

    log("  invoking diag...")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ " + resp["errorMessage"][:300])
    else:
        results = json.loads(resp.get("body", "[]"))
        for res in results:
            mark = "✅" if res.get("ok") else "❌"
            if res.get("ok"):
                log("  " + mark + " {:<40} n={}".format(res["label"][:40], res.get("n")))
                if res.get("sample_keys"):
                    log("       keys: " + str(res["sample_keys"][:15]))
                if res.get("sample"):
                    s = res["sample"][0]
                    if isinstance(s, dict):
                        log("       sample: " + json.dumps({k: v for k, v in list(s.items())[:8]}, default=str)[:200])
            else:
                log("  " + mark + " {:<40} status={}  body={}".format(
                    res["label"][:40], res.get("status"),
                    (res.get("body") or res.get("error") or "")[:100]))

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
    with open(os.path.join(out, "phase_y7b_diag.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
