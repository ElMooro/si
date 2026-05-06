"""Phase X6k — Diagnose why 3 of 4 EFTS queries get 500 errors but 13G/A works."""
import io, json, os, time, base64, zipfile
import boto3
from botocore.config import Config

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)
L2 = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=600))
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
            return {"label": label, "url": url, "ok": True, "n": len(hits),
                    "total": ((d.get("hits") or {}).get("total") or {}).get("value")}
    except urllib.error.HTTPError as e:
        return {"label": label, "url": url, "ok": False, "status": e.code,
                "body": e.read().decode("utf-8", "replace")[:200]}
    except Exception as e:
        return {"label": label, "url": url, "ok": False, "error": str(e)}


def lambda_handler(event=None, context=None):
    # Show the EXACT URLs constructed by my code logic
    def build_v3_url(form_type_query, q_text, offset=0):
        forms_encoded = form_type_query.replace(" ", "+").replace("/", "%2F")
        return ("https://efts.sec.gov/LATEST/search-index?"
                "q=" + urllib.parse.quote('"' + q_text + '"') +
                "&forms=" + forms_encoded +
                "&from=" + str(offset))
    
    # And the literal URL we KNOW worked
    def build_known_good():
        return "https://efts.sec.gov/LATEST/search-index?q=%22schedule+13D%22&forms=SC+13D"
    
    tests = [
        ("v3 13D from=0",  build_v3_url("SC 13D", "schedule 13D", 0)),
        ("v3 13D from=100", build_v3_url("SC 13D", "schedule 13D", 100)),
        ("v3 13D/A from=0", build_v3_url("SC 13D/A", "schedule 13D", 0)),
        ("v3 13D/A from=100", build_v3_url("SC 13D/A", "schedule 13D", 100)),
        ("v3 13G from=0",  build_v3_url("SC 13G", "schedule 13G", 0)),
        ("v3 13G/A from=0", build_v3_url("SC 13G/A", "schedule 13G", 0)),
        ("known_good 13D", build_known_good()),
        # Try variant: q='"13D" or simpler q values
        ("13D simple q",   "https://efts.sec.gov/LATEST/search-index?q=13D&forms=SC+13D"),
        ("13D no q",       "https://efts.sec.gov/LATEST/search-index?forms=SC+13D"),
        ("13D ICAHN",      'https://efts.sec.gov/LATEST/search-index?q=' + urllib.parse.quote('"Icahn"') + '&forms=SC+13D'),
        # Try url-encoded comma variants for forms list
        ("13D-13DA-13G concat", "https://efts.sec.gov/LATEST/search-index?q=%22schedule%22&forms=SC+13D,SC+13D%2FA,SC+13G,SC+13G%2FA"),
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
    r = L2.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ " + resp["errorMessage"][:300])
    else:
        results = json.loads(resp.get("body", "[]"))
        for res in results:
            mark = "✅" if res.get("ok") else "❌"
            tot = res.get("total")
            n = res.get("n")
            if res.get("ok"):
                log("  " + mark + " {:<30} total={} n_returned={}".format(
                    res["label"][:30], tot, n))
            else:
                log("  " + mark + " {:<30} status={}  body: {}".format(
                    res["label"][:30], res.get("status"),
                    (res.get("body") or res.get("error") or "")[:120]))
            log("       URL: " + res["url"])

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
    with open(os.path.join(out, "phase_x6k_diagnose_efts.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
