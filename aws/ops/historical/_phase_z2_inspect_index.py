"""Phase Z2 — Inspect current index.html structure to understand what we're augmenting."""
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
    code = r'''
import boto3
S3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event=None, context=None):
    files_to_inspect = ["index.html", "intelligence.html", "stocks.html"]
    out = {}
    for k in files_to_inspect:
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key=k)
            content = obj["Body"].read().decode("utf-8", "replace")
            out[k] = {
                "size": len(content),
                "first_500": content[:500],
                "last_300": content[-300:],
                "has_nav_links": [
                    l for l in [
                        "stocks.html", "screener", "intelligence",
                        "ath.html", "valuations", "khalid", "stock/", "bot/"
                    ] if l in content
                ],
                "div_count": content.count("<div"),
                "section_count": content.count("<section"),
            }
        except Exception as e:
            out[k] = {"error": str(e)}
    
    return {"statusCode": 200, "body": __import__("json").dumps(out, default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-html-inspect-temp"
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

    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("ERR: " + resp["errorMessage"][:300])
    else:
        d = json.loads(resp.get("body", "{}"))
        for k, info in d.items():
            log("\n# " + k)
            for kk, vv in info.items():
                if kk in ("first_500", "last_300"):
                    log("  " + kk + ":")
                    for ln in str(vv).splitlines()[:10]:
                        log("    " + ln[:140])
                else:
                    log("  " + kk + ": " + str(vv))

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
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "phase_z2_inspect_html.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
