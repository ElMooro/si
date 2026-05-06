"""Phase X6h — Test EFTS with correct format from phase_x6f. The key
insight: forms=SC+13D (literal +), q=... (required), no %20."""
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
import json, urllib.request, urllib.error, time

def fetch(url, label):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "JustHodl-AI raafouis@gmail.com",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
            hits = d.get("hits", {})
            total = (hits.get("total") or {}).get("value", 0)
            results = hits.get("hits", [])
            sample = []
            for h in results[:3]:
                src = h.get("_source", {})
                sample.append({
                    "form": src.get("form"),
                    "filing_date": src.get("file_date"),
                    "ciks": src.get("ciks", [])[:3],
                    "display_names": src.get("display_names", [])[:3],
                    "adsh": src.get("adsh"),
                    "all_keys": list(src.keys()),
                })
            return {"label": label, "ok": True, "total": total, "sample": sample}
    except urllib.error.HTTPError as e:
        return {"label": label, "ok": False, "status": e.code,
                "body": e.read().decode("utf-8", "replace")[:200]}
    except Exception as e:
        return {"label": label, "ok": False, "error": str(e)}


def lambda_handler(event=None, context=None):
    today = time.strftime("%Y-%m-%d")
    seven_ago = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 7 * 86400))
    
    # Use + for spaces (literal) — proven to work in phase_x6f
    tests = [
        # The exact pattern that worked before
        ("13D q+forms (no date)",
         "https://efts.sec.gov/LATEST/search-index?q=%22schedule+13D%22&forms=SC+13D"),
        
        # Add date filter
        ("13D q+forms last 7d",
         "https://efts.sec.gov/LATEST/search-index?q=%22schedule+13D%22&forms=SC+13D&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        
        # Try without quotes around q
        ("13D q-no-quotes",
         "https://efts.sec.gov/LATEST/search-index?q=schedule+13D&forms=SC+13D&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        
        # Try q= empty (just date+form filter)
        ("13D no-q just-form",
         "https://efts.sec.gov/LATEST/search-index?q=&forms=SC+13D&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        
        # 13D/A — slash should be encoded
        ("13D-A with %2F",
         "https://efts.sec.gov/LATEST/search-index?q=%22schedule+13D%22&forms=SC+13D%2FA&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        
        # 13G
        ("13G",
         "https://efts.sec.gov/LATEST/search-index?q=%22schedule+13G%22&forms=SC+13G&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        
        # Filer-named search (no form filter, just q)
        ("Icahn 13D q",
         "https://efts.sec.gov/LATEST/search-index?q=%22Icahn%22&forms=SC+13D,SC+13D%2FA&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        
        # Just q="13D" with forms
        ("q=13D forms=SC+13D",
         "https://efts.sec.gov/LATEST/search-index?q=13D&forms=SC+13D&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
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

    log("  invoking probe...")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ " + resp["errorMessage"][:300])
    else:
        results = json.loads(resp.get("body", "[]"))
        for res in results:
            if res.get("ok"):
                tot = res.get("total", 0)
                marker = "🎯" if tot > 0 else "○"
                log("  {} {:<40} total={}".format(marker, res["label"][:40], tot))
                for s in (res.get("sample") or [])[:2]:
                    log("       form={} date={} ciks={} names={}".format(
                        s.get("form"), s.get("filing_date"),
                        s.get("ciks"), [n[:35] for n in s.get("display_names", [])]))
                if res.get("sample") and tot > 0:
                    keys = res["sample"][0].get("all_keys", [])
                    log("       all keys: " + str(keys))
            else:
                log("  ❌ {:<40} {}".format(res["label"][:40],
                    (res.get("body") or res.get("error") or "")[:100]))

    try:
        L.delete_function(FunctionName=NAME)
        log("  ✓ probe deleted")
    except Exception as e:
        log("  cleanup: " + str(e))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_x6h_efts_v2.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
