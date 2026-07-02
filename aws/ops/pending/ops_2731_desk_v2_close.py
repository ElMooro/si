"""ops 2731 — DESK v2 close: dossier output-contract + patient page proof.

2730 truths: LLM back online but leaked its reasoning scaffold as the dossier
(5.7KB "Analyze the Request…") — _clean_brief() now enforces the contract
(plain prose, <=700 chars, sentence-complete; deterministic fallback else).
Page probe raced CF-Pages' superseded-build cycle (ops auto-commit cancels
in-flight builds) — proof loop widened to 6x50s. Feed strict-JSON stays the
hard gate. Report: aws/ops/reports/2731_desk_v2_close.json.
"""
import os, io, json, time, zipfile, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2731, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "Mozilla/5.0 Chrome/126", "Cache-Control": "no-cache"}
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
def fetch(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=25) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as he:
        return he.code, he.read()[:200]
    except Exception as e:
        return None, str(e)[:100].encode()

print("settling 30s…"); time.sleep(30)
retry(lambda: (wait_ok("justhodl-institutional-footprint"), lam.update_function_code(FunctionName="justhodl-institutional-footprint", ZipFile=zip_fn("justhodl-institutional-footprint")))[-1], "fp")
wait_ok("justhodl-institutional-footprint")
r = lam.invoke(FunctionName="justhodl-institutional-footprint", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/institutional-footprint.json")["Body"].read())
br = d.get("ai_dossier") or ""
R["dossier"] = {"src": d.get("ai_dossier_src"), "len": len(br),
                "clean": br.rstrip().endswith((".", "!", "?")) and "Analyze the Request" not in br and "**" not in br,
                "text": br[:400]}
print("  dossier[%s|%d]: %s" % (R["dossier"]["src"], len(br), br[:220]))
assert 90 <= len(br) <= 760 and R["dossier"]["clean"], "dossier contract violated"

okp = False
for attempt in range(6):
    time.sleep(50)
    st, b = fetch("https://justhodl.ai/institutional-footprint.html?v=c%d" % attempt)
    okp = st == 200 and b"PD TREASURY CURVE" in b
    print("  page attempt %d: %s %s" % (attempt + 1, st, "v2 LIVE" if okp else "old"))
    if okp: break
st2, b2 = fetch("https://justhodl.ai/data/institutional-footprint.json?v=c9")
json.loads(b2.decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
R["feed_strict"], R["page"] = st2 == 200, "LIVE_v2" if okp else "STALE"
assert R["feed_strict"]
assert okp, "page v2 not at edge after 5min"
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2731_desk_v2_close.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2731 COMPLETE")
# rev2 page-deployed
