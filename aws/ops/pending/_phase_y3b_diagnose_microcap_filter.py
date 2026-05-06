"""Phase Y3b — Diagnose why microcap-float-squeeze filtered all 338 out.
Probable causes:
  1. Universe is all >$2B mcap (we filter $50M-$2B)
  2. Profile endpoint returns no shares info
  3. Price < $1 filter

Print first 30 stocks' actual mcap + price + float."""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)


def main():
    code = r'''
import json, urllib.request, time
import boto3

S3 = boto3.client("s3")
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

def fetch_quote(symbol):
    url = "https://financialmodelingprep.com/stable/quote?symbol=" + symbol + "&apikey=" + FMP_KEY
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Diag/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
            if isinstance(d, list) and d:
                return d[0]
    except Exception as e:
        return {"err": str(e)}
    return None

def fetch_profile(symbol):
    url = "https://financialmodelingprep.com/stable/profile?symbol=" + symbol + "&apikey=" + FMP_KEY
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Diag/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
            if isinstance(d, list) and d:
                return d[0]
    except Exception as e:
        return {"err": str(e)}
    return None


def lambda_handler(event=None, context=None):
    obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/universe.json")
    u = json.loads(obj["Body"].read())
    stocks = u.get("stocks", [])
    
    diag = {
        "n_universe": len(stocks),
        "first_5_raw": stocks[:5],
        "by_mcap_buckets": {
            "<50M": 0, "50M-500M": 0, "500M-2B": 0,
            "2B-10B": 0, "10B-100B": 0, ">100B": 0,
            "no_mcap": 0,
        },
        "samples_under_2B": [],
        "samples_50M_to_2B": [],
        "no_data": 0,
        "price_under_1": 0,
        "no_float": 0,
        "field_check": {},
    }
    
    # Check mcap distribution by sampling first 100
    import concurrent.futures as cf
    
    def check(s):
        sym = (s.get("symbol") or "").upper()
        q = fetch_quote(sym)
        p = fetch_profile(sym)
        mc = (q or {}).get("marketCap") if q else None
        pr = (q or {}).get("price") if q else None
        # Check available float fields
        float_fields = {}
        if p:
            for k in ["sharesOutstanding", "sharesFloat", "shareFloat", "float"]:
                if k in p:
                    float_fields[k] = p[k]
        return {
            "sym": sym,
            "mcap": mc,
            "price": pr,
            "profile_keys": list(p.keys())[:25] if isinstance(p, dict) else None,
            "float_fields": float_fields,
        }
    
    with cf.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(check, s) for s in stocks[:60]]
        results = [f.result() for f in cf.as_completed(futures)]
    
    for r in results:
        mc = r.get("mcap")
        if mc is None:
            diag["by_mcap_buckets"]["no_mcap"] += 1
            diag["no_data"] += 1
            continue
        if mc < 50_000_000:
            diag["by_mcap_buckets"]["<50M"] += 1
            diag["samples_under_2B"].append(r)
        elif mc < 500_000_000:
            diag["by_mcap_buckets"]["50M-500M"] += 1
            diag["samples_50M_to_2B"].append(r)
        elif mc < 2_000_000_000:
            diag["by_mcap_buckets"]["500M-2B"] += 1
            diag["samples_50M_to_2B"].append(r)
        elif mc < 10_000_000_000:
            diag["by_mcap_buckets"]["2B-10B"] += 1
        elif mc < 100_000_000_000:
            diag["by_mcap_buckets"]["10B-100B"] += 1
        else:
            diag["by_mcap_buckets"][">100B"] += 1
        if r.get("price") and r["price"] < 1:
            diag["price_under_1"] += 1
        if not r.get("float_fields"):
            diag["no_float"] += 1
    
    diag["samples_50M_to_2B"] = diag["samples_50M_to_2B"][:6]
    diag["samples_under_2B"] = diag["samples_under_2B"][:3]
    
    # Field discovery: which keys does FMP profile return?
    if results and results[0].get("profile_keys"):
        diag["field_check"]["profile_keys_sample"] = results[0]["profile_keys"]
    
    return {"statusCode": 200, "body": json.dumps(diag, default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-microcap-diag-temp"
    try:
        L.get_function(FunctionName=NAME)
        L.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
    except L.exceptions.ResourceNotFoundException:
        L.create_function(FunctionName=NAME, Runtime="python3.12",
                           Handler="lambda_function.lambda_handler",
                           Role=ROLE_ARN, Code={"ZipFile": buf.getvalue()},
                           Timeout=120, MemorySize=512)
    for _ in range(20):
        c = L.get_function_configuration(FunctionName=NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)

    log("  invoking diag (60 stocks sample)...")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ " + resp["errorMessage"][:300])
    else:
        d = json.loads(resp.get("body", "{}"))
        log("  Total universe: " + str(d.get("n_universe")))
        log("  ")
        log("  ── Mcap distribution (60 stock sample) ──")
        for bucket, n in d.get("by_mcap_buckets", {}).items():
            log("    " + bucket + ": " + str(n))
        log("")
        log("  no_mcap: " + str(d.get("no_data")) + " | price_under_1: " + str(d.get("price_under_1")) +
              " | no_float: " + str(d.get("no_float")))
        log("")
        log("  ── FMP profile keys returned ──")
        keys = d.get("field_check", {}).get("profile_keys_sample") or []
        for k in keys:
            log("    " + str(k))
        log("")
        log("  ── Sample stocks $50M-$2B (qualify for filter) ──")
        for r in d.get("samples_50M_to_2B", []) or []:
            mcap_m = (r.get("mcap") or 0) / 1_000_000
            log("    {:<6}  mcap=${:.0f}M  price=${}  float_fields={}".format(
                r.get("sym"), mcap_m, r.get("price"), r.get("float_fields")))
        log("")
        log("  ── First 5 raw universe entries ──")
        for r in d.get("first_5_raw", [])[:5]:
            log("    " + json.dumps({k: r.get(k) for k in ["symbol", "sector", "industry"]}))

    try:
        L.delete_function(FunctionName=NAME)
        log("  ✓ probe deleted")
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
    with open(os.path.join(out, "phase_y3b_diagnose_microcap.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
