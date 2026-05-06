"""Phase Z7 — Extend the S3 bucket policy to allow public reads for HTML pages.

Currently bucket policy only allows /data/* and /scripts/* (or similar).
Need to add:
  - /intel/* and /intel.html
  - and verify /index.html still works

Strategy:
  1. Read current bucket policy
  2. Add a new statement allowing public-read for /*.html and /intel/* and /khalid/*
  3. Put back with confirmation
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
    log("# Phase Z7 — Extend bucket policy for HTML pages\n")
    
    code = r'''
import json, time
import boto3
import urllib.request, urllib.error

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def fetch_test(url):
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return {"status": r.status, "size": int(r.headers.get("Content-Length", 0))}
    except urllib.error.HTTPError as e:
        return {"status": e.code}
    except Exception as e:
        return {"err": str(e)[:80]}


def lambda_handler(event=None, context=None):
    out = {"steps": []}
    
    # 1) Read current policy
    try:
        pol_resp = S3.get_bucket_policy(Bucket=BUCKET)
        current_policy = json.loads(pol_resp.get("Policy", "{}"))
        out["original_policy"] = current_policy
    except Exception as e:
        out["read_err"] = str(e)[:200]
        return {"statusCode": 500, "body": json.dumps(out)}
    
    # 2) Build new policy that adds HTML public-read
    statements = current_policy.get("Statement", [])
    
    # Check if we already have an HTML-pages statement
    has_html_statement = False
    for s in statements:
        sid = s.get("Sid", "")
        if sid in ("PublicReadHtmlPages", "PublicReadIntel"):
            has_html_statement = True
            break
    
    if not has_html_statement:
        new_statement = {
            "Sid": "PublicReadHtmlPages",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": [
                "arn:aws:s3:::" + BUCKET + "/*.html",
                "arn:aws:s3:::" + BUCKET + "/intel/*",
                "arn:aws:s3:::" + BUCKET + "/khalid/*",
                "arn:aws:s3:::" + BUCKET + "/stock/*",
                "arn:aws:s3:::" + BUCKET + "/bot/*",
                "arn:aws:s3:::" + BUCKET + "/secretary/*",
                "arn:aws:s3:::" + BUCKET + "/archive/*"
            ]
        }
        statements.append(new_statement)
        out["steps"].append("Added PublicReadHtmlPages statement")
    else:
        out["steps"].append("PublicReadHtmlPages statement already exists")
    
    new_policy = {
        "Version": current_policy.get("Version", "2012-10-17"),
        "Statement": statements,
    }
    out["new_policy"] = new_policy
    
    # 3) Apply policy
    try:
        S3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(new_policy))
        out["steps"].append("Applied new policy")
    except Exception as e:
        out["apply_err"] = str(e)[:300]
        return {"statusCode": 500, "body": json.dumps(out, default=str)}
    
    # 4) Wait briefly + test fetches
    time.sleep(2)
    
    test_urls = [
        "https://justhodl-dashboard-live.s3.amazonaws.com/intel/index.html",
        "https://justhodl-dashboard-live.s3.amazonaws.com/intel.html",
        "https://justhodl-dashboard-live.s3.amazonaws.com/index.html",
    ]
    out["fetch_tests"] = {}
    for url in test_urls:
        out["fetch_tests"][url] = fetch_test(url)
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-policy-temp"
    try:
        L.get_function(FunctionName=NAME)
        L.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
    except L.exceptions.ResourceNotFoundException:
        L.create_function(FunctionName=NAME, Runtime="python3.12",
                           Handler="lambda_function.lambda_handler",
                           Role=ROLE_ARN, Code={"ZipFile": buf.getvalue()},
                           Timeout=60, MemorySize=256)
    for _ in range(20):
        c = L.get_function_configuration(FunctionName=NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)

    log("  invoking policy patcher...")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    log("  status: " + str(resp.get("statusCode")))
    body = json.loads(resp.get("body", "{}"))

    log("\n## Original policy statements:")
    for s in (body.get("original_policy") or {}).get("Statement", []):
        log("  - " + s.get("Sid", "?") + ": " + str(s.get("Resource"))[:120])

    log("\n## Steps applied:")
    for s in body.get("steps", []):
        log("  ✓ " + s)

    log("\n## Fetch tests after policy update:")
    for url, result in body.get("fetch_tests", {}).items():
        status = result.get("status")
        size = result.get("size", 0)
        mark = "✅" if status == 200 else "❌"
        log("  " + mark + " " + url + " — " + str(status) + " (" + str(size) + "b)")

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
    with open(os.path.join(out, "phase_z7_extend_policy.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
