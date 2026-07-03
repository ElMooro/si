"""ops 2770 — richer lead-lag headline (follow-through + contrarian). Deploy +
invoke + confirm both directional signals surface. Report: 2770_read.json."""
import os, io, json, time, zipfile
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
REGION, BUCKET, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-apac-leadlag"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
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
    import time as _t; t0 = _t.time()
    while _t.time() - t0 < b:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        except ClientError: pass
        _t.sleep(5)
time.sleep(15)
for i in range(6):
    try:
        wait_ok(FN); lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN)); wait_ok(FN); break
    except ClientError: time.sleep(15)
resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
assert not resp.get("FunctionError"), resp["Payload"].read()[:200]
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/apac-leadlag.json")["Body"].read())
print("series_days:", json.dumps(d.get("series_days")))
print("proven:", json.dumps(d.get("proven_leads"), ensure_ascii=False))
print("READ:", d.get("read"))
R = {"ops": 2770, "series_days": d.get("series_days"), "proven": d.get("proven_leads"), "read": d.get("read")}
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2770_read.json", "w"), indent=1, ensure_ascii=False, default=str)
print("OPS 2770 COMPLETE")
