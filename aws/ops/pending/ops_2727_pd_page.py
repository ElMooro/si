"""ops 2727 — polish rev2: PD net via dealer-survey.latest_survey + page verify.

Previous run (2726) went green and proved 13F $ extraction (value_usd: KO
$30.4B, GOOGL $15.6B) but left PD null (data lives under latest_survey, keys
unprinted) and the page marker stale (Pages build superseded). Footprint
v1.1.2 walks latest_survey; page rebuilt via comment bump; marker retried.
Report: aws/ops/reports/2727_pd_page.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2727, "ts": datetime.now(timezone.utc).isoformat()}
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
try:
    ds = json.loads(s3.get_object(Bucket=BUCKET, Key="data/dealer-survey.json")["Body"].read())
    ls = ds.get("latest_survey")
    R["latest_survey_keys"] = sorted(ls.keys())[:16] if isinstance(ls, dict) else str(type(ls))
    print("  latest_survey keys:", R["latest_survey_keys"])
    if isinstance(ls, dict):
        num = {k: v for k, v in list(ls.items())[:20] if isinstance(v, (int, float))}
        print("  numeric sample:", json.dumps(num, default=str)[:260])
except Exception as e:
    print("  dealer-survey read:", str(e)[:70])

retry(lambda: (wait_ok("justhodl-institutional-footprint"), lam.update_function_code(FunctionName="justhodl-institutional-footprint", ZipFile=zip_fn("justhodl-institutional-footprint")))[-1], "fp")
wait_ok("justhodl-institutional-footprint")
r = lam.invoke(FunctionName="justhodl-institutional-footprint", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/institutional-footprint.json")["Body"].read())
R["pd"] = {"net": d.get("primary_dealer_net"), "note": d.get("primary_dealer_note")}
print("  PD:", json.dumps(R["pd"]))
assert d.get("primary_dealer_note") and d["primary_dealer_note"] != "feed absent"
SU = d.get("stocks_usd_13f") or {}
nn = [b for b in (SU.get("buys") or []) if b.get("usd_m") is not None]
R["f13_usd_confirm"] = nn[:4]
print("  13F $ confirm:", json.dumps(nn[:4], default=str))
assert len(nn) >= 3
assert d["version"] == "1.1.2"

R["page"] = "pending"
for attempt in range(3):
    time.sleep(75)
    try:
        with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/institutional-footprint.html?v=%d" % attempt,
                                    headers={"User-Agent": "jh/1", "Cache-Control": "no-cache"}), timeout=20) as rr:
            if "DARK 1d" in rr.read().decode("utf-8", "ignore"):
                R["page"] = "LIVE"; break
            R["page"] = "no_marker_attempt_%d" % (attempt + 1)
    except Exception as e:
        R["page"] = "propagating: " + str(e)[:50]
print("  page:", R["page"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2727_pd_page.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2727 COMPLETE")
