"""Phase X6f — Diagnose SEC EDGAR 403. From AWS Lambda IPs, SEC has been
known to block / throttle. Test multiple variants of headers + routes to
find one that works."""
import json, time, os, urllib.request, urllib.error
import boto3

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def main():
    section("Run a one-off diagnostic Lambda — test 8 different header / URL variants from AWS IP")

    # Build a minimal probe Lambda inline
    diag_code = r'''
import json, urllib.request, urllib.error, time

def try_fetch(url, headers, label):
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as r:
            ct = r.headers.get("Content-Type", "")
            data = r.read()
            return {"label": label, "ok": True, "status": r.status,
                    "ct": ct[:80], "size": len(data),
                    "preview": data[:200].decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", "replace")[:200]
        except Exception:
            pass
        return {"label": label, "ok": False, "status": e.code,
                "body": body, "headers": dict(e.headers or {})}
    except Exception as e:
        return {"label": label, "ok": False, "error": str(e)}

def lambda_handler(event=None, context=None):
    today = time.gmtime(time.time() - 2 * 86400)  # 2 days ago
    yyyy = time.strftime("%Y", today)
    qtr = (today.tm_mon - 1) // 3 + 1
    yymmdd = time.strftime("%Y%m%d", today)
    
    base_email = "raafouis@gmail.com"
    
    # Different combinations to try
    tests = [
        # 1: Plain UA with email (current code)
        ("plain_email_ua",
         "https://www.sec.gov/Archives/edgar/full-index/" + yyyy + "/QTR" + str(qtr) + "/master." + yymmdd + ".idx",
         {"User-Agent": "JustHodl-AI " + base_email}),
        
        # 2: SEC's recommended UA format: "Sample Company Name AdminContact@samplecompany.com"
        ("sec_recommended_format",
         "https://www.sec.gov/Archives/edgar/full-index/" + yyyy + "/QTR" + str(qtr) + "/master." + yymmdd + ".idx",
         {"User-Agent": "JustHodl-AI Khalid " + base_email,
          "Accept-Encoding": "gzip, deflate",
          "Host": "www.sec.gov"}),
        
        # 3: Browser-mimicking
        ("browser_chrome",
         "https://www.sec.gov/Archives/edgar/full-index/" + yyyy + "/QTR" + str(qtr) + "/master." + yymmdd + ".idx",
         {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
          "Accept": "text/html,application/xhtml+xml",
          "Accept-Language": "en-US,en;q=0.9",
          "Accept-Encoding": "gzip, deflate",
          "Host": "www.sec.gov"}),
        
        # 4: Try the Atom feed (should work since RSS endpoint was working before)
        ("atom_feed_baseline",
         "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=SC+13D&output=atom",
         {"User-Agent": "JustHodl-AI " + base_email}),
        
        # 5: company_tickers.json (was working in our probe)
        ("company_tickers",
         "https://www.sec.gov/files/company_tickers.json",
         {"User-Agent": "JustHodl-AI " + base_email}),
        
        # 6: master.idx without date suffix (current quarter)
        ("quarterly_master",
         "https://www.sec.gov/Archives/edgar/full-index/" + yyyy + "/QTR" + str(qtr) + "/master.idx",
         {"User-Agent": "JustHodl-AI " + base_email}),
        
        # 7: full-index-listing
        ("full_index_dir",
         "https://www.sec.gov/Archives/edgar/full-index/" + yyyy + "/QTR" + str(qtr) + "/",
         {"User-Agent": "JustHodl-AI " + base_email}),
        
        # 8: efts.sec.gov full-text search (was working in our probe)
        ("efts_search",
         "https://efts.sec.gov/LATEST/search-index?q=%22schedule+13D%22&forms=SC+13D",
         {"User-Agent": "JustHodl-AI " + base_email,
          "Accept": "application/json"}),
    ]
    
    results = []
    for label, url, headers in tests:
        results.append(try_fetch(url, headers, label))
        time.sleep(0.5)  # be polite
    
    return {
        "statusCode": 200,
        "body": json.dumps(results, default=str)
    }
'''
    
    # Deploy a one-off probe Lambda
    import io, zipfile, base64
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, diag_code)
    zb = buf.getvalue()
    
    PROBE_NAME = "justhodl-sec-probe-temp"
    ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
    
    try:
        L.get_function(FunctionName=PROBE_NAME)
        log("  exists — updating")
        L.update_function_code(FunctionName=PROBE_NAME, ZipFile=zb)
        for _ in range(20):
            c = L.get_function_configuration(FunctionName=PROBE_NAME)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
    except L.exceptions.ResourceNotFoundException:
        log("  creating temp probe Lambda")
        L.create_function(FunctionName=PROBE_NAME, Runtime="python3.12",
                           Handler="lambda_function.lambda_handler",
                           Role=ROLE_ARN, Code={"ZipFile": zb},
                           Timeout=60, MemorySize=256)
        for _ in range(20):
            c = L.get_function_configuration(FunctionName=PROBE_NAME)
            if c.get("State") == "Active":
                break
            time.sleep(1)

    log("  invoking probe...")
    r = L.invoke(FunctionName=PROBE_NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = json.loads(r["Payload"].read())
    inner = json.loads(body.get("body", "[]"))
    
    log("  results from AWS Lambda IP:")
    log("")
    for res in inner:
        if res.get("ok"):
            log("    ✅ {:<30} status={}  ct={}  size={:,}b".format(
                res["label"][:30], res["status"], res["ct"][:30], res.get("size", 0)))
            log("       preview: " + (res.get("preview") or "")[:150].replace("\n", " "))
        else:
            log("    ❌ {:<30} status={}  err: {}".format(
                res["label"][:30], res.get("status"), 
                (res.get("body") or res.get("error") or "")[:120]))
        log("")

    section("Cleanup")
    try:
        L.delete_function(FunctionName=PROBE_NAME)
        log("  ✓ deleted probe Lambda")
    except Exception as e:
        log("  cleanup: " + str(e))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
        for ln in traceback.format_exc().splitlines():
            log("    " + ln)
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_x6f_diagnose_403.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
