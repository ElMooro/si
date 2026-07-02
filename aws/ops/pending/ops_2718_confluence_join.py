"""ops 2718 — CONFLUENCE JOIN FIX: publish full book, close the last shallow join.

equity-confluence published only confluence_book[:30] + proven_book[:20]; the
full per-name `book` (names_with_any_signal) never left the Lambda. Now emits
compact xray_map over the whole book; X-Ray reads it first. Deploy both,
invoke confluence -> xray, assert ec joins scale with the feed's own count.
Report: aws/ops/reports/2718_confluence_join.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2718, "ts": datetime.now(timezone.utc).isoformat()}
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"): z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        time.sleep(5)
def retry(call, what, tries=6):
    for i in range(tries):
        try: return call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"): time.sleep(18)
            else: raise
    raise RuntimeError(what)

print("settling 30s…"); time.sleep(30)
for fn in ("justhodl-equity-confluence", "justhodl-stock-xray"):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn); print("synced", fn)
r = lam.invoke(FunctionName="justhodl-equity-confluence", InvocationType="RequestResponse")
print("confluence ->", (r["Payload"].read() or b"")[:160].decode("utf-8", "ignore"))
assert not r.get("FunctionError")
ecdoc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/equity-confluence.json")["Body"].read())
xm = ecdoc.get("xray_map") or {}
book_n = (ecdoc.get("counts") or {}).get("names_with_any_signal") or 0
R["xray_map_n"], R["book_n"] = len(xm), book_n
print("xray_map:", len(xm), "of book", book_n)
assert len(xm) >= max(30, int(0.9 * book_n)), "xray_map thin: %d vs book %d" % (len(xm), book_n)
r = lam.invoke(FunctionName="justhodl-stock-xray", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("xray ->", json.dumps(pay)[:240])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/stock-xray.json")["Body"].read())
JN = d["joins"]; nv = (d["cards"] or {}).get("NVDA") or {}
R["joins_v4"], R["NVDA_confl"] = JN, nv.get("confl")
print("joins v4:", json.dumps(JN), "| NVDA confl:", nv.get("confl"))
assert JN.get("ec", 0) >= max(30, int(0.5 * min(book_n, 3000))), "ec still shallow: %s vs book %d" % (JN, book_n)
assert JN.get("fm", 0) >= 1500 and JN.get("dp", 0) >= 500, "regression: %s" % JN
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2718_confluence_join.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2718 COMPLETE — last shallow join closed")
