"""ops 2761 — HK Southbound via Eastmoney (HKEX Akamai-blocked from AWS).
Deploy + invoke + dump Eastmoney structure so extractor can be finalized.
Report: 2761_eastmoney_hk.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
REGION, BUCKET, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-apac-flows"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2761, "ts": datetime.now(timezone.utc).isoformat()}
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
print("HK status:", hk.get("status"), "| source:", hk.get("source"), "| net:", hk.get("southbound_net_total"))
print("HK markets:", json.dumps(hk.get("markets"), ensure_ascii=False))
print("EASTMONEY DUMP:", json.dumps(hk.get("_dump"), ensure_ascii=False)[:900])
R["hongkong"] = {k: hk.get(k) for k in ("status", "source", "southbound_net_total", "markets", "unit", "note")}
R["dump"] = hk.get("_dump")
R["sources"] = d.get("sources")
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2761_eastmoney_hk.json", "w"), indent=1, ensure_ascii=False, default=str)
print("OPS 2761 COMPLETE")
