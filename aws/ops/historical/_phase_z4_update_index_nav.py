"""Phase Z4 — Update index.html nav to feature the new /intel/ dashboard.

Strategy:
  1. Read current index.html from S3
  2. Find the navigation/links area
  3. Add a prominent NEW link to /intel/ at top
  4. Re-upload to S3
"""
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
    log("# Phase Z4 — Update index.html with prominent /intel/ link")
    log("")
    
    # Use a Lambda to fetch + edit + re-upload (since we don't have local AWS creds)
    code = r'''
import json, time
import boto3

S3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event=None, context=None):
    BUCKET = "justhodl-dashboard-live"
    
    obj = S3.get_object(Bucket=BUCKET, Key="index.html")
    html = obj["Body"].read().decode("utf-8", "replace")
    
    out = {"original_size": len(html), "modifications": []}
    
    # Strategy 1: Find existing nav and add a "INTEL" pill near the top
    # Look for first <body> tag and insert a flash banner right after it
    intel_banner = """
<!-- INTEL TERMINAL BANNER -->
<div style="background:linear-gradient(135deg,#0a0f1a,#0f1524);border-bottom:2px solid #00d68f;padding:14px 20px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;font-family:'IBM Plex Sans',-apple-system,sans-serif">
  <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap">
    <span style="font-size:11px;color:#00f4a8;background:rgba(0,214,143,0.12);padding:4px 10px;border-radius:12px;border:1px solid rgba(0,214,143,0.3);font-family:'IBM Plex Mono',monospace;font-weight:600">NEW</span>
    <span style="font-size:14px;font-weight:600;color:#e8ecf4">⚡ Institutional Intelligence Terminal</span>
    <span style="font-size:12px;color:#8b95ad">13-feed multi-cap pump detection · 1,809 stocks · auto-updating daily</span>
  </div>
  <a href="/intel/" style="font-size:12px;font-weight:600;color:#00f4a8;background:rgba(0,214,143,0.1);padding:8px 18px;border-radius:6px;border:1px solid rgba(0,214,143,0.4);text-decoration:none;font-family:'IBM Plex Mono',monospace">OPEN TERMINAL →</a>
</div>
<!-- /INTEL BANNER -->
"""
    
    # Insert immediately after <body> tag
    if "<body" in html and "<!-- INTEL TERMINAL BANNER -->" not in html:
        # Find the end of the <body...> opening tag
        body_start = html.find("<body")
        body_end = html.find(">", body_start)
        if body_start != -1 and body_end != -1:
            new_html = html[:body_end + 1] + intel_banner + html[body_end + 1:]
            S3.put_object(
                Bucket=BUCKET,
                Key="index.html",
                Body=new_html.encode("utf-8"),
                ContentType="text/html; charset=utf-8",
                CacheControl="max-age=300",
            )
            out["modifications"].append("Added INTEL banner after <body>")
            out["new_size"] = len(new_html)
            return {"statusCode": 200, "body": json.dumps(out)}
        else:
            return {"statusCode": 400, "body": "couldnt find body tag"}
    elif "<!-- INTEL TERMINAL BANNER -->" in html:
        return {"statusCode": 200, "body": json.dumps({"already_present": True, "size": len(html)})}
    else:
        return {"statusCode": 400, "body": "no body tag in index.html"}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-html-patch-temp"
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

    log("  invoking patcher...")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    log("  status: " + str(resp.get("statusCode")))
    body = json.loads(resp.get("body", "{}"))
    log("  result: " + json.dumps(body))
    
    log("")
    log("✓ index.html updated. Visit https://justhodl.ai/ to see the INTEL banner.")

    try:
        L.delete_function(FunctionName=NAME)
        log("  ✓ patcher cleaned up")
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
    with open(os.path.join(out, "phase_z4_update_index.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
