"""Phase Z1 — Audit S3 bucket for HTML pages + JSON feeds + identify what's
missing on website."""
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
import json, time
import boto3

S3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event=None, context=None):
    paginator = S3.get_paginator("list_objects_v2")
    htmls = []
    jsons = []
    other = []
    for page in paginator.paginate(Bucket="justhodl-dashboard-live"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]
            modified = obj["LastModified"].strftime("%Y-%m-%d %H:%M")
            entry = {"key": key, "size": size, "modified": modified}
            if key.endswith(".html"):
                htmls.append(entry)
            elif key.endswith(".json"):
                jsons.append(entry)
            else:
                other.append(entry)
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "htmls": sorted(htmls, key=lambda x: x["key"]),
            "jsons": sorted(jsons, key=lambda x: x["key"]),
            "other_count": len(other),
        }, default=str)
    }
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-s3-audit-temp"
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

    log("  invoking audit...")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ " + resp["errorMessage"][:300])
    else:
        d = json.loads(resp.get("body", "{}"))
        htmls = d.get("htmls", [])
        jsons = d.get("jsons", [])
        
        log("")
        log("# HTML pages on website (" + str(len(htmls)) + ")")
        for h in htmls:
            log("  {:<55} {:>10,}b  {}".format(h["key"], h["size"], h["modified"]))
        
        log("")
        log("# JSON data feeds (" + str(len(jsons)) + ")")
        for j in jsons:
            log("  {:<55} {:>10,}b  {}".format(j["key"], j["size"], j["modified"]))
    
    try:
        L.delete_function(FunctionName=NAME)
        log("  ✓ probe deleted")
    except Exception:
        pass


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
    with open(os.path.join(out, "phase_z1_audit_pages.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
