"""ops 2720 — FLOW FLEET REVIVAL (Khalid's capital-flow mandate, phase 1).

Audit found ~27 flow-family engines with schedule:None — including
capital-flow-radar (7 readers), sector-rotation (13 readers), sovereign-fiscal
(TIC), tic-flows, theme-rotation, crypto-etf-flows, stablecoin-flow. Paying
for providers while feeds fossilize. This op: (1) creates EventBridge rules
for 17 revival engines from aws/ops/flow_revival_sched.json (staggered);
(2) deploys + invokes each once, tolerant, recording ok/err + feed freshness;
(3) deploys etf-true-flows with the new 20-name COUNTRY category and verifies
country tickers appear in the snapshot — the hot-money world map substrate.
Phase 2 (ops 2721) builds the Global Flow Desk on top.
Report: aws/ops/reports/2720_flow_fleet_revival.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2720, "ts": datetime.now(timezone.utc).isoformat(), "engines": {}}
SCHED = json.load(open("aws/ops/flow_revival_sched.json"))
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
def wait_ok(fn, budget=200):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        time.sleep(4)
def retry(call, what, tries=5):
    for i in range(tries):
        try: return call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"): time.sleep(15)
            else: raise
    raise RuntimeError(what)

print("settling 30s…"); time.sleep(30)
ok = 0
for fn, expr in SCHED.items():
    rec = {"sched": expr}
    try:
        try:
            lam.get_function(FunctionName=fn)
        except lam.exceptions.ResourceNotFoundException:
            rec["status"] = "NO_LAMBDA"; R["engines"][fn] = rec; print(fn, "-> NO_LAMBDA"); continue
        retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
        wait_ok(fn)
        ra = ev.put_rule(Name=fn + "-sched", ScheduleExpression=expr, State="ENABLED",
                         Description="Flow-fleet revival (ops 2720)")["RuleArn"]
        try:
            lam.add_permission(FunctionName=fn, StatementId="evt-" + fn + "-sched",
                               Action="lambda:InvokeFunction", Principal="events.amazonaws.com", SourceArn=ra)
        except lam.exceptions.ResourceConflictException: pass
        ev.put_targets(Rule=fn + "-sched", Targets=[{"Id": "1", "Arn":
                       "arn:aws:lambda:%s:857687956942:function:%s" % (REGION, fn)}])
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
        body = (r["Payload"].read() or b"")[:150].decode("utf-8", "ignore")
        if r.get("FunctionError"):
            rec["status"] = "INVOKE_ERR"; rec["err"] = body
        else:
            rec["status"] = "REVIVED"; rec["resp"] = body; ok += 1
        print(fn, "->", rec["status"], body[:90])
    except Exception as e:
        rec["status"] = "OPS_ERR"; rec["err"] = str(e)[:110]; print(fn, "-> OPS_ERR", rec["err"])
    R["engines"][fn] = rec
    time.sleep(2)
R["revived_ok"] = ok
print("REVIVED OK:", ok, "/", len(SCHED))
assert ok >= 11, "revival too weak: %d" % ok

print("\n── etf-true-flows COUNTRY verify ──")
retry(lambda: (wait_ok("justhodl-etf-true-flows"), lam.update_function_code(FunctionName="justhodl-etf-true-flows", ZipFile=zip_fn("justhodl-etf-true-flows")))[-1], "tf")
wait_ok("justhodl-etf-true-flows")
r = lam.invoke(FunctionName="justhodl-etf-true-flows", InvocationType="RequestResponse")
assert not r.get("FunctionError"), r["Payload"].read()[:200]
tf = json.loads(s3.get_object(Bucket=BUCKET, Key="data/etf-true-flows.json")["Body"].read())
blob = json.dumps(tf)
present = [t for t in ("MCHI", "EWG", "EWY", "EWT", "EWU", "EWC", "EWW", "EZA", "TUR", "VNM", "THD", "EWQ") if t in blob]
R["country_present"] = present
print("  country tickers in feed:", len(present), present)
assert len(present) >= 9, "country expansion thin"

print("\n── key feed freshness after revival ──")
for k in ("data/capital-flow-radar.json", "data/sector-rotation.json", "data/sovereign-fiscal.json",
          "data/theme-rotation.json", "data/crypto-etf-flows.json", "data/stablecoin-flow.json",
          "data/tic-flows.json", "data/fx-intelligence.json"):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=k)
        age = (datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 60
        R.setdefault("freshness_min", {})[k] = round(age, 1)
        print("  %-38s %6.1f min" % (k, age))
    except Exception:
        R.setdefault("freshness_min", {})[k] = None
        print("  %-38s MISSING" % k)
rad = json.loads(s3.get_object(Bucket=BUCKET, Key="data/capital-flow-radar.json")["Body"].read())
R["radar"] = {"version": rad.get("version"), "n_complexes": rad.get("n_complexes")}
print("  radar:", R["radar"])
assert (rad.get("n_complexes") or 0) >= 40

os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2720_flow_fleet_revival.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2720 COMPLETE — the flow fleet breathes again")
