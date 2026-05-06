"""Phase X6g — Probe EDGAR full-text search API (efts.sec.gov) in detail.
We need to know its query format, response shape, and how to filter for 13D/13G."""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def main():
    code = r'''
import json, urllib.request, urllib.error, urllib.parse, time

def fetch(url, label):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "JustHodl-AI raafouis@gmail.com",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
            hits = data.get("hits", {})
            total = (hits.get("total") or {}).get("value", 0)
            results = hits.get("hits", [])
            sample = []
            for h in results[:5]:
                src = h.get("_source", {})
                sample.append({
                    "form": src.get("form"),
                    "filing_date": src.get("file_date"),
                    "ciks": src.get("ciks", [])[:3],
                    "display_names": src.get("display_names", [])[:3],
                    "adsh": src.get("adsh"),
                    "all_keys": list(src.keys())[:15],
                })
            return {"label": label, "ok": True, "total": total,
                    "n_hits_in_response": len(results),
                    "sample": sample}
    except urllib.error.HTTPError as e:
        return {"label": label, "ok": False, "status": e.code,
                "body": e.read().decode("utf-8", "replace")[:200]}
    except Exception as e:
        return {"label": label, "ok": False, "error": str(e)}


def lambda_handler(event=None, context=None):
    today = time.strftime("%Y-%m-%d")
    seven_ago = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 7 * 86400))
    thirty_ago = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 30 * 86400))
    
    tests = [
        ("13D last 7d",
         "https://efts.sec.gov/LATEST/search-index?forms=SC%2013D&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        ("13D/A last 7d",
         "https://efts.sec.gov/LATEST/search-index?forms=SC%2013D%2FA&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        ("13G last 7d",
         "https://efts.sec.gov/LATEST/search-index?forms=SC%2013G&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        ("13G/A last 7d",
         "https://efts.sec.gov/LATEST/search-index?forms=SC%2013G%2FA&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        ("All 4 forms via comma",
         "https://efts.sec.gov/LATEST/search-index?forms=SC%2013D,SC%2013D%2FA,SC%2013G,SC%2013G%2FA&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        ("With company text query",
         "https://efts.sec.gov/LATEST/search-index?q=%22schedule+13D%22&forms=SC%2013D&dateRange=custom&startdt=" + thirty_ago + "&enddt=" + today),
        ("With activist filer name (Icahn)",
         "https://efts.sec.gov/LATEST/search-index?q=%22Carl+Icahn%22&forms=SC%2013D,SC%2013D%2FA&dateRange=custom&startdt=" + thirty_ago + "&enddt=" + today),
        ("Pagination test (from=10)",
         "https://efts.sec.gov/LATEST/search-index?forms=SC%2013D,SC%2013G&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today + "&from=10"),
    ]
    
    return {"statusCode": 200, "body": json.dumps([fetch(u, l) for l, u in tests], default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-efts-probe-temp"
    try:
        L.get_function(FunctionName=NAME)
        L.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
        for _ in range(15):
            c = L.get_function_configuration(FunctionName=NAME)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
    except L.exceptions.ResourceNotFoundException:
        L.create_function(FunctionName=NAME, Runtime="python3.12",
                           Handler="lambda_function.lambda_handler",
                           Role=ROLE_ARN, Code={"ZipFile": buf.getvalue()},
                           Timeout=120, MemorySize=256)
        for _ in range(15):
            c = L.get_function_configuration(FunctionName=NAME)
            if c.get("State") == "Active":
                break
            time.sleep(1)

    log("  invoking probe...")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ Lambda error: " + resp["errorMessage"][:300])
    else:
        results = json.loads(resp.get("body", "[]"))
        for res in results:
            if res.get("ok"):
                log("  ✅ {:<40} total={}  in_response={}".format(
                    res["label"][:40], res.get("total"), res.get("n_hits_in_response")))
                for s in (res.get("sample") or [])[:2]:
                    log("     • form={}, date={}, ciks={}, names={}".format(
                        s.get("form"), s.get("filing_date"),
                        s.get("ciks"), [n[:40] for n in s.get("display_names", [])]))
                if res.get("sample"):
                    log("     all source keys: " + str(res["sample"][0].get("all_keys")))
            else:
                log("  ❌ {:<40} status={}  err={}".format(
                    res["label"][:40], res.get("status"), 
                    (res.get("body") or res.get("error") or "")[:120]))

    section("Cleanup")
    try:
        L.delete_function(FunctionName=NAME)
        log("  ✓ deleted")
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
    with open(os.path.join(out, "phase_x6g_efts_probe.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
