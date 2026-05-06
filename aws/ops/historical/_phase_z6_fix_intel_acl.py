"""Phase Z6 — Fix intel HTML 403 by setting public-read ACL.

Also test if the bucket has bucket-policy-based access. If not, we use
ACL=public-read on each object.
"""
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
    log("# Phase Z6 — Fix intel HTML 403\n")
    
    code = r'''
import json, time
import boto3

S3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event=None, context=None):
    BUCKET = "justhodl-dashboard-live"
    out = {"steps": []}
    
    # 1) Check current bucket policy
    try:
        pol = S3.get_bucket_policy(Bucket=BUCKET)
        out["bucket_policy"] = pol.get("Policy", "")[:500]
    except Exception as e:
        out["bucket_policy_err"] = str(e)[:120]
    
    # 2) Check what makes data/*.json public
    try:
        for sample_key in ["data/universe.json", "intel/index.html", "intel.html", "index.html"]:
            try:
                acl = S3.get_object_acl(Bucket=BUCKET, Key=sample_key)
                grants = []
                for g in acl.get("Grants", []):
                    grantee = g.get("Grantee", {})
                    grants.append({
                        "type": grantee.get("Type"),
                        "uri": grantee.get("URI"),
                        "perm": g.get("Permission"),
                    })
                out["acl_" + sample_key] = grants
            except Exception as e:
                out["acl_" + sample_key + "_err"] = str(e)[:120]
    except Exception as e:
        out["check_err"] = str(e)[:120]
    
    # 3) Re-upload intel/index.html with ACL=public-read
    try:
        # First read existing content
        existing = S3.get_object(Bucket=BUCKET, Key="intel/index.html")
        content = existing["Body"].read()
        
        # Re-upload with ACL
        S3.put_object(
            Bucket=BUCKET,
            Key="intel/index.html",
            Body=content,
            ContentType="text/html; charset=utf-8",
            CacheControl="max-age=300",
            ACL="public-read",
        )
        out["steps"].append("re-uploaded intel/index.html with ACL=public-read")
    except Exception as e:
        out["upload_intel_err"] = str(e)[:200]
    
    try:
        existing = S3.get_object(Bucket=BUCKET, Key="intel.html")
        content = existing["Body"].read()
        S3.put_object(
            Bucket=BUCKET,
            Key="intel.html",
            Body=content,
            ContentType="text/html; charset=utf-8",
            CacheControl="max-age=300",
            ACL="public-read",
        )
        out["steps"].append("re-uploaded intel.html with ACL=public-read")
    except Exception as e:
        out["upload_intel_html_err"] = str(e)[:200]
    
    # 4) Test fetches
    import urllib.request
    test_urls = [
        "https://justhodl-dashboard-live.s3.amazonaws.com/intel/index.html",
        "https://justhodl-dashboard-live.s3.amazonaws.com/intel.html",
    ]
    out["fetch_tests"] = {}
    for url in test_urls:
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                out["fetch_tests"][url] = {"status": r.status,
                                            "size": int(r.headers.get("Content-Length", 0))}
        except urllib.error.HTTPError as e:
            out["fetch_tests"][url] = {"status": e.code, "err": "HTTP " + str(e.code)}
        except Exception as e:
            out["fetch_tests"][url] = {"err": str(e)[:120]}
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-acl-fix-temp"
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

    log("  invoking acl fix...")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    log("  status: " + str(resp.get("statusCode")))
    body = json.loads(resp.get("body", "{}"))
    log("\n## Bucket diagnostics:")
    if "bucket_policy" in body:
        log("  policy: " + str(body["bucket_policy"])[:200])
    log("\n## ACL details:")
    for k, v in body.items():
        if k.startswith("acl_") and not k.endswith("_err"):
            log("  " + k + ":")
            for g in v:
                log("    - " + g.get("perm","?") + " for " + (g.get("uri") or g.get("type") or "?"))
    log("\n## Steps applied:")
    for s in body.get("steps", []):
        log("  ✓ " + s)
    log("\n## Fetch tests:")
    for url, result in body.get("fetch_tests", {}).items():
        log("  " + url + " — " + json.dumps(result))

    try:
        L.delete_function(FunctionName=NAME)
        log("\n  ✓ patcher cleaned up")
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
    with open(os.path.join(out, "phase_z6_acl_fix.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
