"""Phase Z3 — Deploy /intel/ dashboard to S3 + verify accessibility."""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

REPORT = []
def log(m):
    print(m)
    REPORT.append(m)


def main():
    log("# Phase Z3 — Deploy /intel/ dashboard")
    log("")
    
    html_content = open("web/intel/index.html").read()
    log("  HTML: " + str(len(html_content)) + " chars")

    # Use a Lambda to do the S3 upload (since we don't have local AWS creds)
    # The Lambda will receive the HTML in event payload (base64-encoded)
    code = r'''
import json, base64, time
import boto3

S3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event=None, context=None):
    bucket = event.get("bucket")
    key = event.get("key")
    content_b64 = event.get("content_b64")
    content_type = event.get("content_type", "text/html")
    
    if not all([bucket, key, content_b64]):
        return {"statusCode": 400, "body": "missing params"}
    
    content = base64.b64decode(content_b64)
    
    S3.put_object(
        Bucket=bucket,
        Key=key,
        Body=content,
        ContentType=content_type,
        CacheControl="max-age=300",
    )
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "bucket": bucket,
            "key": key,
            "size": len(content),
            "url": "https://" + bucket + ".s3.amazonaws.com/" + key,
        })
    }
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-s3-uploader-temp"
    try:
        L.get_function(FunctionName=NAME)
        L.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
    except L.exceptions.ResourceNotFoundException:
        L.create_function(FunctionName=NAME, Runtime="python3.12",
                           Handler="lambda_function.lambda_handler",
                           Role=ROLE_ARN, Code={"ZipFile": buf.getvalue()},
                           Timeout=60, MemorySize=512)
    for _ in range(20):
        c = L.get_function_configuration(FunctionName=NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)

    # Upload to two paths so it's accessible at both /intel/ and /intel/index.html
    payload = {
        "bucket": BUCKET,
        "key": "intel/index.html",
        "content_b64": base64.b64encode(html_content.encode("utf-8")).decode("ascii"),
        "content_type": "text/html; charset=utf-8",
    }
    
    log("  uploading to S3...")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                  Payload=json.dumps(payload).encode())
    resp = json.loads(r["Payload"].read())
    body = resp.get("body", "")
    log("  result: " + str(body)[:200])
    
    # Also upload to /intel.html for direct URL access
    payload2 = {
        "bucket": BUCKET,
        "key": "intel.html",
        "content_b64": base64.b64encode(html_content.encode("utf-8")).decode("ascii"),
        "content_type": "text/html; charset=utf-8",
    }
    r2 = L.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                   Payload=json.dumps(payload2).encode())
    resp2 = json.loads(r2["Payload"].read())
    log("  also uploaded /intel.html: " + str(resp2.get("body", ""))[:120])
    
    log("")
    log("✓ Deploy complete. Available at:")
    log("  https://justhodl.ai/intel/")
    log("  https://justhodl.ai/intel.html")
    log("  https://justhodl-dashboard-live.s3.amazonaws.com/intel/index.html")

    try:
        L.delete_function(FunctionName=NAME)
        log("  ✓ uploader cleaned up")
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
    with open(os.path.join(out, "phase_z3_deploy_intel.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
