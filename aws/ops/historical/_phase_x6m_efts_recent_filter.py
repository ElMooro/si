"""Phase X6m — Find the URL pattern that returns RECENT 13D/G filings.
Test: 
  1. Try dateRange=custom + various q variations
  2. Try category=form-type
  3. Try just removing q entirely (forms-only)
  4. Try the EFTS UI's "search by entity" mode
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
import json, urllib.request, urllib.error, urllib.parse, time

def fetch(url, label):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "JustHodl-AI raafouis@gmail.com",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
            hits = (d.get("hits") or {}).get("hits") or []
            sample = []
            for h in hits[:5]:
                src = h.get("_source", {})
                sample.append({
                    "form": src.get("form"),
                    "date": src.get("file_date"),
                    "names": [n[:35] for n in src.get("display_names", [])][:2],
                })
            # Detect date range
            if hits:
                dates = sorted([h.get("_source", {}).get("file_date") for h in hits if h.get("_source", {}).get("file_date")], reverse=True)
                date_range = (dates[0], dates[-1]) if dates else ("?","?")
            else:
                date_range = ("none", "none")
            return {"label": label, "ok": True,
                    "n": len(hits),
                    "total": ((d.get("hits") or {}).get("total") or {}).get("value"),
                    "date_range_in_response": date_range,
                    "sample": sample[:3]}
    except urllib.error.HTTPError as e:
        return {"label": label, "ok": False, "status": e.code,
                "body": e.read().decode("utf-8", "replace")[:200]}
    except Exception as e:
        return {"label": label, "ok": False, "error": str(e)}


def lambda_handler(event=None, context=None):
    today = time.strftime("%Y-%m-%d")
    seven_ago = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 7*86400))
    thirty_ago = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 30*86400))
    
    base = "https://efts.sec.gov/LATEST/search-index"
    
    tests = [
        # 1. forms only (no q)
        ("forms-only 13D",
         base + "?forms=SC+13D"),
        
        # 2. forms + q with simple word
        ("forms 13D + q=schedule",
         base + "?q=schedule&forms=SC+13D"),
        
        # 3. forms + q + dateRange
        ("forms 13D + dateRange last 30d",
         base + "?forms=SC+13D&dateRange=custom&startdt=" + thirty_ago + "&enddt=" + today),
        
        # 4. q + forms + dateRange (combined)
        ("forms+q+dateRange last 30d",
         base + "?q=schedule&forms=SC+13D&dateRange=custom&startdt=" + thirty_ago + "&enddt=" + today),
        
        # 5. UI-style URL (what efts.sec.gov UI uses)
        ("UI-style dateRange last 7d",
         base + "?q=&forms=SC+13D&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today),
        
        # 6. Try with "&hits=10" or similar param
        ("hits=10 last 7d",
         base + "?forms=SC+13D&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today + "&hits=10"),
        
        # 7. Sort param explicitly
        ("sort=desc filings",
         base + "?forms=SC+13D&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today + "&sort=date"),
        
        # 8. Try the full-text-search UI endpoint variant
        ("efts-fts variant",
         "https://efts.sec.gov/LATEST/search-index?q=&forms=SC+13D&dateRange=custom&startdt=" + seven_ago + "&enddt=" + today + "&from=0"),
    ]
    
    return {"statusCode": 200, "body": json.dumps([fetch(u, l) for l, u in tests], default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-efts-diag-temp"
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
        log("  ❌ " + resp["errorMessage"][:400])
    else:
        results = json.loads(resp.get("body", "[]"))
        for res in results:
            mark = "✅" if res.get("ok") else "❌"
            if res.get("ok"):
                dr = res.get("date_range_in_response", ["?","?"])
                tot = res.get("total")
                log("  " + mark + " {:<30} total={:<6} n={:<3} dates={}..{}".format(
                    res["label"][:30], str(tot), res.get("n"), dr[1], dr[0]))
                for s in (res.get("sample") or [])[:2]:
                    log("       {} {} {}".format(s.get("form"), s.get("date"), s.get("names")))
            else:
                log("  " + mark + " {:<30} status={}".format(
                    res["label"][:30], res.get("status")))

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
    with open(os.path.join(out, "phase_x6m_efts_recent.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
