"""ops 2721 — GLOBAL FLOW DESK v1 (phase 2 of the capital-flow mandate).

Fuses the revived fleet into one product: asset-class $ ladder (equity /
treasuries / credit / TIPS / real estate / gold / silver / commodities /
crypto / cash), 11-sector rotation ranking, institutional-vs-retail
divergence gauge, and the country HOT-MONEY map (country ETFs + TIC + FX).
Deploy engine + rule, invoke, assert every pillar, verify page + board.
Report: aws/ops/reports/2721_global_flow_desk.json.
"""
import os, io, json, time, zipfile, re, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2721, "ts": datetime.now(timezone.utc).isoformat()}
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
def ensure_fn(name):
    cfg = json.load(open("aws/lambdas/%s/config.json" % name)); zb = zip_fn(name)
    try:
        lam.get_function(FunctionName=name); wait_ok(name)
        retry(lambda: lam.update_function_code(FunctionName=name, ZipFile=zb), name); wait_ok(name)
    except lam.exceptions.ResourceNotFoundException:
        retry(lambda: lam.create_function(FunctionName=name, Runtime=cfg.get("runtime", "python3.12"),
              Role=cfg.get("role") or "arn:aws:iam::857687956942:role/lambda-execution-role",
              Handler=cfg.get("handler", "lambda_function.lambda_handler"), Code={"ZipFile": zb},
              Timeout=int(cfg.get("timeout") or 180), MemorySize=int(cfg.get("memory") or 512),
              Architectures=["x86_64"], Description=(cfg.get("description") or "")[:250]), name)
        wait_ok(name); print("  CREATED", name)
    sch = cfg.get("schedule")
    ra = ev.put_rule(Name=sch["name"], ScheduleExpression=sch["expression"], State="ENABLED",
                     Description=sch.get("description", ""))["RuleArn"]
    try:
        lam.add_permission(FunctionName=name, StatementId="evt-" + sch["name"],
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com", SourceArn=ra)
    except lam.exceptions.ResourceConflictException: pass
    ev.put_targets(Rule=sch["name"], Targets=[{"Id": "1", "Arn":
                   "arn:aws:lambda:%s:857687956942:function:%s" % (REGION, name)}])

print("settling 30s…"); time.sleep(30)
ensure_fn("justhodl-global-flow-desk")
r = lam.invoke(FunctionName="justhodl-global-flow-desk", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("invoke ->", json.dumps(pay)[:340])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/global-flow-desk.json")["Body"].read())
AC, HOT, IR, SEC = d["asset_classes"], d["hot_money"], d["inst_vs_retail"], d["sectors"]
R["classes"] = {k: v.get("net_5d_usd_m") for k, v in AC.items()}
R["populated_classes"] = sum(1 for v in AC.values() if v.get("n"))
R["hot"] = {"n": HOT["n_scored"], "in": HOT["top_inflows"], "out": HOT["top_outflows"]}
R["inst_retail"] = IR
R["sectors"] = {"leaders": SEC["leaders"], "laggards": SEC["laggards"], "n": len(SEC["ranked"])}
R["ai"] = bool(d.get("ai_brief"))
R["ctry_sample"] = {c: HOT["countries"][c] for c in list(HOT["countries"])[:3]}
print(json.dumps({k: R[k] for k in ("populated_classes", "hot", "inst_retail", "sectors", "ai")}, default=str)[:800])
assert R["populated_classes"] >= 9, "class ladder thin: %s" % R["classes"]
print("  src_counts:", HOT.get("src_counts"), "| warming:", len(HOT.get("warming_etfs") or []), HOT.get("warming_etfs"))
assert HOT["n_scored"] >= 6, "country map thin: %d" % HOT["n_scored"]
assert HOT["n_scored"] + len(HOT.get("warming_etfs") or []) >= 20, "silent country loss: scored %d + warming %d" % (HOT["n_scored"], len(HOT.get("warming_etfs") or []))
IR = d.get("inst_vs_retail") or {}
print("  inst_retail v2:", json.dumps(IR, default=str)[:220])
assert IR.get("retail") is not None or IR.get("aaii_spread") is None, "retail leg still null with aaii present"
assert len(SEC["ranked"]) >= 9, "sector ranking thin"
assert IR.get("institutional") is not None, "institutional composite missing"
retry(lambda: (wait_ok("justhodl-signal-board"), lam.update_function_code(FunctionName="justhodl-signal-board", ZipFile=zip_fn("justhodl-signal-board")))[-1], "board")
wait_ok("justhodl-signal-board")
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
assert "GLOBAL FLOWS" in sb
m = re.search(r"\{[^{}]*Global Flows[^{}]*\}", sb)
R["board"] = m.group(0)[:220] if m else "present"
print("board:", R["board"])
time.sleep(70)
try:
    with urllib.request.urlopen("https://justhodl.ai/global-flow-desk.html", timeout=20) as rr:
        R["page"] = "LIVE" if "GLOBAL FLOW DESK" in rr.read().decode("utf-8", "ignore") else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("page:", R["page"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2721_global_flow_desk.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2721 COMPLETE — the desk knows where money is going")

# rev2
