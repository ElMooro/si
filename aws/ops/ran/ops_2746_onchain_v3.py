"""ops 2746 — ON-CHAIN DESK v3 (Khalid: 2010 charts, click-enlarge, master AI).

Engine v2.1: series feed at full resolution (CQ 730d lines, twins+BTC ~1200pt
2010->present) + MASTER AI BRIEF scanning every metric (LLM contract-v4 over
a full-catalog digest; deterministic synthesis fallback computed from the
data so the top-of-page read is always real). Page v3: charts default to the
LONGEST real window (2010-> twins primary, CQ 1y overlaid; honest accruing
label otherwise), click-to-enlarge modal with axes/year gridlines/BTC scale,
master brief on top. Asserts: master brief present+clean+src, series depths,
page v3 marker at edge, feeds strict. Report: 2746_onchain_v3.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=890, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2746, "ts": datetime.now(timezone.utc).isoformat()}
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

print("settling 25s…"); time.sleep(25)
print("== 1/3 engine v2.1 + full run ==")
for i in range(6):
    try:
        wait_ok("justhodl-cryptoquant")
        lam.update_function_code(FunctionName="justhodl-cryptoquant", ZipFile=zip_fn("justhodl-cryptoquant")); break
    except ClientError: time.sleep(18)
wait_ok("justhodl-cryptoquant")
r = lam.invoke(FunctionName="justhodl-cryptoquant", InvocationType="RequestResponse",
               Payload=json.dumps({}).encode())
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:260])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
mb, msrc = d.get("ai_master_brief") or "", d.get("ai_master_src")
print("  master[%s|%d]: %s" % (msrc, len(mb), mb[:170]))
assert 120 <= len(mb) <= 950 and mb[:1].isalpha() and mb[:1].isupper() and chr(34) not in mb \
       and "[" not in mb and mb.rstrip().endswith((".", "!", "?")), "master brief contract"
assert d["n_metrics"] >= 45
R["master"] = {"src": msrc, "len": len(mb), "head": mb[:220]}
sr = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-series.json")["Body"].read())
btcn = len(sr.get("btc", {}).get("d") or [])
twn = {k: len(v["d"]) for k, v in (sr.get("twins") or {}).items()}
sln = sorted(len(v["d"]) for v in sr["series"].values())
print("  btc pts:", btcn, "| twins:", twn, "| series len min/med/max:", sln[0], sln[len(sln)//2], sln[-1])
assert btcn >= 900 and min(twn.values()) >= 900 and sln[len(sln)//2] >= 300
R["series"] = {"btc": btcn, "twins": twn, "series_median": sln[len(sln)//2]}

print("== 2/3 page v3 at edge ==")
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as rr:
        return rr.read()
okp = False
for a in range(6):
    time.sleep(40)
    try: okp = b"ONCHAIN DESK v3" in pub("onchain.html")
    except Exception: pass
    print("  attempt %d: %s" % (a + 1, "v3 LIVE" if okp else "pending"))
    if okp: break
assert okp, "page v3 not at edge"
for f in ("data/cryptoquant-onchain.json", "data/cryptoquant-series.json"):
    json.loads(pub(f).decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
R["page"] = "LIVE_v3"

print("== 3/3 report ==")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2746_onchain_v3.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2746 COMPLETE — every metric scanned, every decade drawn")
