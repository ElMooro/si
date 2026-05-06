"""Phase X6n — Inspect Atom RSS feeds in detail to validate the RSS-only approach.
We need to see:
  1. How many filings each form's RSS returns
  2. What the entry structure looks like (Filer/Subject roles)
  3. Whether CIK mapping works
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
import json, urllib.request, urllib.error, urllib.parse, time, re

def fetch_text(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl-AI raafouis@gmail.com",
    })
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.read().decode("utf-8", "replace")


def parse_atom(xml, form_type_label):
    blocks = re.findall(r"<entry>(.*?)</entry>", xml, flags=re.DOTALL)
    out = []
    for b in blocks:
        title_m = re.search(r"<title>([^<]+)</title>", b)
        link_m = re.search(r'<link[^/]*href="([^"]+)"', b)
        updated_m = re.search(r"<updated>([^<]+)</updated>", b)
        if not title_m: continue
        title = title_m.group(1).strip()
        link = link_m.group(1) if link_m else ""
        updated = updated_m.group(1) if updated_m else ""
        out.append({
            "form_label": form_type_label,
            "title": title,
            "link": link[:200],
            "updated": updated,
        })
    return out


def lambda_handler(event=None, context=None):
    forms = ["SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"]
    results = {}
    
    for ft in forms:
        url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=" + urllib.parse.quote(ft) + "&output=atom&count=40"
        try:
            xml = fetch_text(url)
            entries = parse_atom(xml, ft)
            results[ft] = {
                "n_entries": len(entries),
                "url": url,
                "first_5": entries[:5],
                "last_5": entries[-5:] if len(entries) > 5 else [],
            }
        except urllib.error.HTTPError as e:
            results[ft] = {"error": "HTTP " + str(e.code), "url": url}
        except Exception as e:
            results[ft] = {"error": str(e), "url": url}
    
    # Also try without count=40
    for ft in ["SC 13D"]:
        url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=" + urllib.parse.quote(ft) + "&output=atom"
        try:
            xml = fetch_text(url)
            entries = parse_atom(xml, ft + "_NO_COUNT")
            results[ft + "_no_count"] = {
                "n_entries": len(entries),
                "url": url,
                "first_5": entries[:5],
            }
        except Exception as e:
            results[ft + "_no_count"] = {"error": str(e)}
    
    return {"statusCode": 200, "body": json.dumps(results, default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-rss-diag-temp"
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

    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ " + resp["errorMessage"][:400])
    else:
        results = json.loads(resp.get("body", "{}"))
        for ft, info in results.items():
            log("")
            log("# " + ft)
            if "error" in info:
                log("  ERR: " + info["error"])
                continue
            log("  n_entries: " + str(info["n_entries"]))
            log("  first 5 titles:")
            for e in info.get("first_5", []):
                log("    " + e["title"][:120])
                log("      link: " + e["link"][:100])
                log("      updated: " + e["updated"])

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
    with open(os.path.join(out, "phase_x6n_rss_inspect.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
