"""ops 2740 — CRYPTOQUANT GOES LIVE (Khalid subscribed; the on-chain seat fills).

Creates the vendor adapter justhodl-cryptoquant (spec-driven endpoints,
SSM-gated Bearer auth, 5y backfill, 365d z / percentile / WoW, composite
on-chain risk z, PROVISIONAL grading banner) + registers the onchain signal
family in engine-signal-map. DUAL-MODE proof: token present -> full LIVE
gates (>=6 metrics, >=1200-day histories, <=3d staleness, strict feed at
domain); token absent -> honest GATED verdict recorded, self-activates on
next scheduled run after Khalid's SSM put. Report: 2740_cryptoquant.json.
"""
import os, io, json, time, zipfile, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
R = {"ops": 2740, "ts": datetime.now(timezone.utc).isoformat()}
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
    tok = ssm.get_parameter(Name="/justhodl/cryptoquant/token", WithDecryption=True)["Parameter"]["Value"]
    R["token"] = "PRESENT(len=%d)" % len(tok.strip())
except Exception:
    R["token"] = "ABSENT"
print("  SSM token:", R["token"])

print("== 1/3 create adapter + schedule ==")
name = "justhodl-cryptoquant"
cfg = json.load(open("aws/lambdas/%s/config.json" % name)); zb = zip_fn(name)
try:
    lam.get_function(FunctionName=name); wait_ok(name)
    retry(lambda: lam.update_function_code(FunctionName=name, ZipFile=zb), name); wait_ok(name)
except lam.exceptions.ResourceNotFoundException:
    retry(lambda: lam.create_function(FunctionName=name, Runtime=cfg["runtime"], Role=cfg["role"],
          Handler=cfg["handler"], Code={"ZipFile": zb}, Timeout=cfg["timeout"], MemorySize=cfg["memory"],
          Architectures=cfg["architectures"], Description=cfg["description"][:250]), "create")
    wait_ok(name); print("  CREATED", name)
sch = cfg["schedule"]
ra = ev.put_rule(Name=sch["name"], ScheduleExpression=sch["expression"], State="ENABLED",
                 Description=sch["description"])["RuleArn"]
try:
    lam.add_permission(FunctionName=name, StatementId="evt-" + sch["name"], Action="lambda:InvokeFunction",
                       Principal="events.amazonaws.com", SourceArn=ra)
except lam.exceptions.ResourceConflictException: pass
ev.put_targets(Rule=sch["name"], Targets=[{"Id": "1", "Arn": "arn:aws:lambda:%s:857687956942:function:%s" % (REGION, name)}])
r = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
               Payload=json.dumps({"backfill": True}).encode())
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:280])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
R["status"] = d["status"]
if d["status"] == "LIVE":
    M = d["metrics"]
    hist = json.loads(s3.get_object(Bucket=BUCKET, Key="data/history/cryptoquant.json")["Body"].read())
    depth = {k: len(v) for k, v in hist.items()}
    R["live"] = {"n_metrics": len(M), "depth": depth, "composite": d["composite_onchain_risk_z"],
                 "staleness_d": d["max_staleness_days"],
                 "sample": {k: M[k] for k in list(M)[:4]}, "errors": d.get("errors")}
    print("  metrics:", json.dumps({k: (M[k]["value"], M[k]["z365"]) for k in M}, default=str)[:300])
    print("  depth:", depth)
    print("  composite risk z:", d["composite_onchain_risk_z"], "| read:", d.get("read"))
    assert len(M) >= 6, "metrics thin: %s" % list(M)
    assert min(depth.values()) >= 1200, "backfill shallow: %s" % depth
    assert d["max_staleness_days"] <= 3, "stale: %sd" % d["max_staleness_days"]
    assert all(M[k]["z365"] is not None for k in M), "z gaps"
else:
    R["gated_note"] = d.get("note"); R["armed"] = d.get("armed_metrics")
    print("  GATED — armed metrics:", R["armed"])

print("== 2/3 signal-map registration ==")
retry(lambda: (wait_ok("justhodl-engine-signal-map"), lam.update_function_code(FunctionName="justhodl-engine-signal-map", ZipFile=zip_fn("justhodl-engine-signal-map")))[-1], "map")
wait_ok("justhodl-engine-signal-map")
r = lam.invoke(FunctionName="justhodl-engine-signal-map", InvocationType="RequestResponse")
assert not r.get("FunctionError"), r["Payload"].read()[:180]
sm = json.loads(s3.get_object(Bucket=BUCKET, Key="data/engine-signal-map.json")["Body"].read())
blob = json.dumps(sm)
assert "onchain_composite_risk" in blob and "cryptoquant" in blob, "registration missing from map feed"
print("  onchain family registered in signal map")

print("== 3/3 public feed strict ==")
okf = False
for a in range(3):
    time.sleep(20)
    try:
        req = urllib.request.Request("https://justhodl.ai/data/cryptoquant-onchain.json?v=%d" % a,
                                     headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=25) as rr:
            json.loads(rr.read().decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        okf = True; break
    except Exception as e:
        print("  attempt %d: %s" % (a + 1, str(e)[:70]))
R["public_feed"] = okf
assert okf
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2740_cryptoquant.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2740 COMPLETE — the on-chain seat is %s" % ("FILLED" if d["status"] == "LIVE" else "armed and waiting"))
