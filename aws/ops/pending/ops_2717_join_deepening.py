"""ops 2717 — STOCK X-RAY JOIN DEEPENING (from ops-2716's shape-probe map).

Upstream publishes: factor-returns now emits full decile membership lists
(5 factors x ~300/side); est-revisions emits direction_map over all tracked.
X-Ray v3 extractors: master-ranker top_tickers (25 by design -> TOP25 tag),
deciles -> factor memberships in the thousands, direction_map, confluence
container keys, supply-graph by_ticker -> peers + laggards board unlock.
Invoke chain: factors -> est-revisions -> stock-xray, then prove joins.
Report: aws/ops/reports/2717_join_deepening.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2717, "ts": datetime.now(timezone.utc).isoformat()}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)
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

sect("1/3 DEPLOY chain + refresh upstream feeds")
print("  settling 30s…"); time.sleep(30)
for fn in ("justhodl-factor-returns", "justhodl-estimate-revisions", "justhodl-stock-xray"):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn); print("  synced", fn)
for fn in ("justhodl-factor-returns", "justhodl-estimate-revisions"):
    r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
    print("  %s -> %s%s" % (fn, "ERR " if r.get("FunctionError") else "", (r["Payload"].read() or b"")[:120].decode("utf-8", "ignore")))
fd = json.loads(s3.get_object(Bucket=BUCKET, Key="data/factor-returns.json")["Body"].read())
dec = fd.get("deciles") or {}
R["deciles"] = {k: {"long": len(v.get("long") or []), "short": len(v.get("short") or [])} for k, v in dec.items()}
print("  deciles:", R["deciles"])
assert sum(x["long"] + x["short"] for x in R["deciles"].values()) >= 1500, "deciles thin: %s" % R["deciles"]
er = json.loads(s3.get_object(Bucket=BUCKET, Key="data/estimate-revisions.json")["Body"].read())
R["direction_map_n"] = len(er.get("direction_map") or {})
print("  direction_map:", R["direction_map_n"])
assert R["direction_map_n"] >= 300, "direction_map thin: %d" % R["direction_map_n"]

sect("2/3 X-RAY v3 — run + prove deepened joins")
r = lam.invoke(FunctionName="justhodl-stock-xray", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:280])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/stock-xray.json")["Body"].read())
JN, B, C = d["joins"], d["boards"], d["cards"]
peers_n = sum(1 for c in C.values() if c.get("peers"))
R["joins_v3"] = {**JN, "peers_cards": peers_n}
R["boards_v3"] = {k: (len(v), v[:6]) for k, v in B.items()}
nv = C.get("NVDA") or {}
R["NVDA_v3"] = {k: nv.get(k) for k in ("rank", "factors", "peers", "est_rev", "confl")}
print("  joins:", json.dumps(R["joins_v3"]))
print("  boards:", json.dumps(R["boards_v3"], default=str)[:420])
print("  NVDA:", json.dumps(R["NVDA_v3"], default=str)[:260])
assert JN.get("fm", 0) >= 1500, "fm still thin: %s" % JN
assert JN.get("er", 0) >= 300, "er still thin: %s" % JN
assert JN.get("mr", 0) >= 15, "mr top25 missing: %s" % JN
assert peers_n >= 80, "peers join failed: %d" % peers_n
assert len(B.get("laggards_watch") or []) >= 1 or peers_n < 100, "laggards dead despite peers"

sect("3/3 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2717_join_deepening.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2717 COMPLETE — X-Ray joins at institutional depth")
