"""Phase Z9 — Diagnose where justhodl.ai is actually served from.
The S3 bucket has Bloomberg Terminal V10.3 but CDN serves Operator Console.
Find the actual origin so we can deploy there too."""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

REPORT = []
def log(m):
    print(m)
    REPORT.append(m)


def main():
    log("# Phase Z9 — Diagnose CDN origin\n")
    
    code = r'''
import json, urllib.request, urllib.error

def fetch_with_headers(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            content = r.read()
            return {
                "status": r.status,
                "size": len(content),
                "headers": dict(r.headers),
                "first_2000": content[:2000].decode("utf-8", "replace"),
                "title": (
                    content[content.find(b"<title>")+7:content.find(b"</title>")].decode("utf-8", "replace")
                    if b"<title>" in content else "?"
                ),
                # Check for hosting clues
                "has_github_pages": b"github" in content.lower() or b"github" in str(dict(r.headers)).encode().lower(),
                "server": r.headers.get("Server", "?"),
                "x_amz_cf": r.headers.get("X-Amz-Cf-Id", ""),
                "cf_ray": r.headers.get("Cf-Ray", ""),
                "x_github_request_id": r.headers.get("X-Github-Request-Id", ""),
            }
    except urllib.error.HTTPError as e:
        return {"status": e.code}
    except Exception as e:
        return {"err": str(e)[:200]}


def lambda_handler(event=None, context=None):
    out = {}
    
    # Test main domain to find origin clues
    out["justhodl_ai_root"] = fetch_with_headers("https://justhodl.ai/")
    
    # GitHub Pages would have X-Github-Request-Id, response from username.github.io
    # Cloudflare would have Cf-Ray
    # CloudFront would have X-Amz-Cf-Id
    
    # Try the GitHub Pages URL directly
    out["github_pages"] = fetch_with_headers("https://elmooro.github.io/")
    out["github_pages_si"] = fetch_with_headers("https://elmooro.github.io/si/")
    
    # Try www
    out["www"] = fetch_with_headers("https://www.justhodl.ai/")
    
    # Try S3 website endpoint (if bucket has website hosting enabled)
    out["s3_website"] = fetch_with_headers("http://justhodl-dashboard-live.s3-website-us-east-1.amazonaws.com/")
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-cdn-diag-temp"
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
        d = json.loads(resp.get("body", "{}"))
        for key, info in d.items():
            log("\n## " + key)
            if "err" in info:
                log("  err: " + info["err"][:120])
                continue
            log("  status: " + str(info.get("status")))
            log("  size: " + str(info.get("size")))
            log("  title: " + str(info.get("title")[:80]))
            log("  server: " + str(info.get("server")))
            log("  cf_ray: " + str(info.get("cf_ray", ""))[:30])
            log("  x_github: " + str(info.get("x_github_request_id", ""))[:30])
            log("  x_amz_cf: " + str(info.get("x_amz_cf", ""))[:30])
            log("  first 200: " + str(info.get("first_2000", ""))[:300].replace("\n", " "))

    try:
        L.delete_function(FunctionName=NAME)
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
    with open(os.path.join(out, "phase_z9_diagnose_cdn.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
