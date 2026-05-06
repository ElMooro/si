"""Phase X6i — Test EFTS sort + check most recent filings."""
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

def fetch(url, label):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "JustHodl-AI raafouis@gmail.com",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
            hits = d.get("hits", {})
            results = hits.get("hits", [])
            sample = []
            for h in results[:8]:
                src = h.get("_source", {})
                sample.append({
                    "form": src.get("form"),
                    "date": src.get("file_date"),
                    "names": [n[:55] for n in src.get("display_names", [])][:3],
                    "ciks": src.get("ciks", [])[:3],
                    "adsh": src.get("adsh"),
                })
            return {"label": label, "ok": True,
                    "total": (hits.get("total") or {}).get("value"),
                    "in_response": len(results),
                    "sample": sample}
    except urllib.error.HTTPError as e:
        return {"label": label, "ok": False, "status": e.code,
                "body": e.read().decode("utf-8", "replace")[:200]}
    except Exception as e:
        return {"label": label, "ok": False, "error": str(e)}


def lambda_handler(event=None, context=None):
    tests = [
        ("13D sort=desc",
         "https://efts.sec.gov/LATEST/search-index?q=%22schedule+13D%22&forms=SC+13D&from=0"),
        ("13D first 100",
         "https://efts.sec.gov/LATEST/search-index?q=%22schedule+13D%22&forms=SC+13D&from=0&size=100"),
        ("13D/A first 100",
         "https://efts.sec.gov/LATEST/search-index?q=%22schedule+13D%22&forms=SC+13D%2FA&from=0&size=100"),
        ("13G first 100",
         "https://efts.sec.gov/LATEST/search-index?q=%22schedule+13G%22&forms=SC+13G&from=0&size=100"),
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
    for _ in range(15):
        c = L.get_function_configuration(FunctionName=NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)

    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("ERR: " + resp["errorMessage"][:200])
    else:
        results = json.loads(resp.get("body", "[]"))
        for res in results:
            log("")
            log("# " + res.get("label", "?"))
            log("  ok=" + str(res.get("ok")) + "  total=" + str(res.get("total")) + "  n_returned=" + str(res.get("in_response")))
            for s in (res.get("sample") or [])[:5]:
                log("    {} {} ciks={} {}".format(
                    s.get("form"), s.get("date"), s.get("ciks"), s.get("names")))
    
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
    with open(os.path.join(out, "phase_x6i_efts_sort.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
