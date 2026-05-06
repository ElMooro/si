"""Phase Y4 — Test screener quality across ALL cap buckets.
Need to know: how many real common stocks per cap bucket, and how to
filter funds/preferred shares/etc."""
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
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def lambda_handler(event=None, context=None):
    base = "https://financialmodelingprep.com/stable/company-screener"
    
    # Test pagination + extra filters
    buckets = [
        # (label, params, ideal_count_target)
        ("nano  $5M-50M", "marketCapMoreThan=5000000&marketCapLowerThan=50000000", 1000),
        ("micro $50M-300M", "marketCapMoreThan=50000000&marketCapLowerThan=300000000", 1500),
        ("small $300M-2B", "marketCapMoreThan=300000000&marketCapLowerThan=2000000000", 2000),
        ("mid   $2B-10B", "marketCapMoreThan=2000000000&marketCapLowerThan=10000000000", 1500),
        ("large $10B-200B", "marketCapMoreThan=10000000000&marketCapLowerThan=200000000000", 800),
        ("mega  >$200B", "marketCapMoreThan=200000000000", 100),
    ]
    
    out = {}
    for label, params, target in buckets:
        # Try with limit=1000 to see how many it returns
        url = (base + "?" + params +
               "&isActivelyTrading=true&country=US&exchange=NYSE,NASDAQ,AMEX&limit=1000&apikey=" + FMP_KEY)
        try:
            data = fetch(url)
            n = len(data) if isinstance(data, list) else 0
            sample = data[:5] if isinstance(data, list) else []
            
            # Count common stocks vs funds/etfs by industry
            n_funds = sum(1 for d in (data or [])
                          if (d.get("industry") or "").lower() in ("asset management", "shell companies"))
            n_real = n - n_funds
            
            out[label] = {
                "url_params": params,
                "total_returned": n,
                "estimated_real_stocks": n_real,
                "estimated_funds_or_shells": n_funds,
                "limit_target": target,
                "sample": [
                    {"sym": d.get("symbol"), "mc": d.get("marketCap"),
                     "name": (d.get("companyName") or "")[:35],
                     "industry": d.get("industry"),
                     "exchange": d.get("exchange")}
                    for d in sample
                ],
            }
        except urllib.error.HTTPError as e:
            out[label] = {"error": "HTTP " + str(e.code)}
        except Exception as e:
            out[label] = {"error": str(e)}
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-screener-test-temp"
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
        results = json.loads(resp.get("body", "{}"))
        total = 0
        total_real = 0
        for label, info in results.items():
            log("")
            log("# " + label)
            if "error" in info:
                log("  ERR: " + info["error"])
                continue
            n = info.get("total_returned", 0)
            n_real = info.get("estimated_real_stocks", 0)
            n_funds = info.get("estimated_funds_or_shells", 0)
            log("  Returned: " + str(n) + " (real: ~" + str(n_real) + ", funds/shells: " + str(n_funds) + ")")
            total += n
            total_real += n_real
            for s in info.get("sample", [])[:3]:
                mc = s.get("mc") or 0
                mc_str = ("${:.0f}M".format(mc / 1_000_000) if mc < 1e9
                           else "${:.1f}B".format(mc / 1e9))
                log("    {:<6} {:<8} {:<35} | {}".format(
                    s.get("sym") or "?", mc_str, (s.get("name") or "")[:35], s.get("industry") or "?"))
        log("")
        log("# GRAND TOTAL: " + str(total) + " stocks across all buckets (real: ~" + str(total_real) + ")")

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
    with open(os.path.join(out, "phase_y4_screener_test.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
