"""ops 2758 — HK Southbound go-live with robust gzip/charset fetch.
Deploys apac-flows (robust _http_text for HKEX), invokes, reports HK block +
bridge. If HKEX still serves obfuscated content to AWS, the note shows it.
Report: 2758_hk_live.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
REGION, BUCKET, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-apac-flows"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2758, "ts": datetime.now(timezone.utc).isoformat()}
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn; buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files: z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"): z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, b=200):
    t0 = time.time()
    while time.time() - t0 < b:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        except ClientError: pass
        time.sleep(5)
print("settling 15s…"); time.sleep(15)
for i in range(6):
    try:
        wait_ok(FN); lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN)); wait_ok(FN); break
    except ClientError: time.sleep(15)
resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
pay = json.loads(resp["Payload"].read() or b"{}"); assert not resp.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/apac-flows.json")["Body"].read())
hk = d.get("hongkong", {})
print("SOURCES:", json.dumps(d.get("sources")))
print("HONG KONG:", json.dumps(hk, ensure_ascii=False)[:500])
hb = next((b for b in d.get("bridges", []) if b.get("hk_southbound_net") is not None), None)
print("HK bridge:", json.dumps(hb, default=str)[:220] if hb else "NONE (Southbound not live)")
R["hongkong"] = hk; R["sources"] = d.get("sources"); R["hk_bridge"] = bool(hb)
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2758_hk_live.json", "w"), indent=1, ensure_ascii=False, default=str)
print("HK STATUS:", hk.get("status"), "| net:", hk.get("southbound_net_total"))
print("OPS 2758 COMPLETE")
