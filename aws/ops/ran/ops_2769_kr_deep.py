"""ops 2769 — Korea deep history in lead-lag (frgn.naver ~100d). Deploy + invoke +
verify KR series_days jumps from ~10 to ~90 and KR memory→US pairs get real n +
significance testing. Confirms finance.naver.com reachable from Lambda.
Report: 2769_kr_deep.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
REGION, BUCKET, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-apac-leadlag"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2769, "ts": datetime.now(timezone.utc).isoformat()}
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
pay = json.loads(resp["Payload"].read() or b"{}")
print("invoke ->", json.dumps(pay, default=str)[:260])
assert not resp.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/apac-leadlag.json")["Body"].read())
sd = d.get("series_days", {})
print("series_days:", json.dumps(sd))
print("status:", d.get("status"), "| proven:", len(d.get("proven_leads", [])))
for p in d.get("pairs", []):
    b = p.get("best")
    print("  %-26s overlap=%d best=%s" % (p["name"], p["overlap_days"],
          ("r=%+.2f h=%dd n=%d sig=%s" % (b["r"], b["horizon"], b["n"], b["significant"]) if b else "—")))
print("READ:", d.get("read"))
R["series_days"] = sd; R["proven"] = d.get("proven_leads"); R["read"] = d.get("read")
R["pairs"] = [{"name": p["name"], "overlap": p["overlap_days"], "best": p.get("best")} for p in d.get("pairs", [])]
# KR now must be deep (frgn.naver reachable from Lambda) — expect >>10
assert sd.get("kr_memory", 0) >= 45, ("KR history still thin (finance.naver blocked from Lambda?)", sd.get("kr_memory"))
kr_pairs = [p for p in d.get("pairs", []) if "Korea" in p["name"]]
assert all(p.get("best") and p["best"]["n"] >= 40 for p in kr_pairs), ("KR pairs lack sample size", [(p["name"], (p.get("best") or {}).get("n")) for p in kr_pairs])
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2769_kr_deep.json", "w"), indent=1, ensure_ascii=False, default=str)
print("OPS 2769 COMPLETE — Korea now Taiwan-grade depth; memory→US rigorously testable")
