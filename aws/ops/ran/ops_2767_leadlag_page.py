"""ops 2767 — lead-lag: deeper HK backfill + correct sign read + page section.
Deploy leadlag adapter, invoke, verify HK series deeper + read sign correct;
verify page LEAD-LAG section at edge. Report: 2767_leadlag_page.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-apac-leadlag"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2767, "ts": datetime.now(timezone.utc).isoformat()}
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
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/apac-leadlag.json")["Body"].read())
print("series_days:", json.dumps(d.get("series_days")))
print("status:", d.get("status"), "| proven:", len(d.get("proven_leads", [])))
for p in d.get("pairs", []):
    b = p.get("best")
    print("  %-26s overlap=%d best=%s" % (p["name"], p["overlap_days"],
          ("r=%+.2f h=%dd n=%d sig=%s" % (b["r"], b["horizon"], b["n"], b["significant"]) if b else "—")))
print("READ:", d.get("read"))
R["series_days"] = d.get("series_days"); R["proven"] = d.get("proven_leads"); R["read"] = d.get("read")
R["pairs"] = [{"name": p["name"], "overlap": p["overlap_days"], "best": p.get("best")} for p in d.get("pairs", [])]
assert sum(d.get("series_days", {}).values()) > 40, d.get("series_days")
print("== page section at edge ==")
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as rr: return rr.read()
okp = False
for a in range(6):
    time.sleep(40)
    try: okp = b"LEAD-LAG \xe2\x80\x94 DOES ASIAN" in pub("apac.html")
    except Exception: pass
    print("  page attempt %d: %s" % (a + 1, "LIVE" if okp else "pending"))
    if okp: break
assert okp, "lead-lag page section not at edge"
R["page"] = "LIVE"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2767_leadlag_page.json", "w"), indent=1, ensure_ascii=False, default=str)
print("OPS 2767 COMPLETE — lead-lag on the page")
