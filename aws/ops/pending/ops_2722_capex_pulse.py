"""ops 2722 — CAPEX PULSE: the real corporate-spend engine (Khalid's mandate).

Portfolio flows (global-flow-desk) told us where INVESTOR money goes; this
measures where CORPORATE money goes. FMP /stable quarterly cash-flows over
the stock-xray top-160 + guaranteed hyperscalers -> TTM capex $ + yoy per
name, sector aggregates, the AI-buildout hyperscaler tile, accelerator/
cutter boards, market totals with history. Fused into global-flow-desk
(capex-pulse primary, mentions fallback), signal-board row, page tile.
Report: aws/ops/reports/2722_capex_pulse.json.
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
R = {"ops": 2722, "ts": datetime.now(timezone.utc).isoformat()}
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
def ensure_fn(name):
    cfg = json.load(open("aws/lambdas/%s/config.json" % name)); zb = zip_fn(name)
    try:
        lam.get_function(FunctionName=name); wait_ok(name)
        retry(lambda: lam.update_function_code(FunctionName=name, ZipFile=zb), name); wait_ok(name)
        retry(lambda: lam.update_function_configuration(FunctionName=name, Timeout=int(cfg["timeout"]), MemorySize=int(cfg["memory"])), name + " cfg"); wait_ok(name)
    except lam.exceptions.ResourceNotFoundException:
        retry(lambda: lam.create_function(FunctionName=name, Runtime=cfg.get("runtime", "python3.12"),
              Role=cfg.get("role") or "arn:aws:iam::857687956942:role/lambda-execution-role",
              Handler=cfg.get("handler", "lambda_function.lambda_handler"), Code={"ZipFile": zb},
              Timeout=int(cfg.get("timeout") or 180), MemorySize=int(cfg.get("memory") or 512),
              Architectures=cfg.get("architectures") or ["x86_64"],
              Description=(cfg.get("description") or "")[:250]), name + " create")
        wait_ok(name); print("  CREATED", name)
    sch = cfg.get("schedule")
    if sch:
        arn = "arn:aws:lambda:%s:857687956942:function:%s" % (REGION, name)
        ra = ev.put_rule(Name=sch["name"], ScheduleExpression=sch["expression"], State="ENABLED",
                         Description=sch.get("description", ""))["RuleArn"]
        try:
            lam.add_permission(FunctionName=name, StatementId="evt-" + sch["name"], Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com", SourceArn=ra)
        except lam.exceptions.ResourceConflictException: pass
        ev.put_targets(Rule=sch["name"], Targets=[{"Id": "1", "Arn": arn}])
        print("  schedule:", sch["expression"])

sect("1/4 CREATE capex-pulse + RUN (~160 FMP calls)")
print("  settling 30s…"); time.sleep(30)
ensure_fn("justhodl-capex-pulse")
r = lam.invoke(FunctionName="justhodl-capex-pulse", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:240])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/capex-pulse.json")["Body"].read())
HY, MK, SC, BD = d["hyperscalers"], d["market"], d["sectors"], d["boards"]
R["capex"] = {"n": d["n"], "fails": d["fails"],
              "market": MK, "hyperscalers": {k: HY.get(k) for k in ("total_ttm_b", "yoy_pct")},
              "hyp_rows": [(x["ticker"], x["capex_ttm_b"], x["yoy_pct"]) for x in HY["rows"]],
              "sectors_n": len(SC),
              "top_sector_yoy": sorted(((n, v["yoy_pct"]) for n, v in SC.items() if v.get("yoy_pct") is not None),
                                       key=lambda kv: kv[1], reverse=True)[:3],
              "accelerators": [(x["ticker"], x["yoy_pct"]) for x in BD["top_accelerators"][:6]],
              "cutters": [(x["ticker"], x["yoy_pct"]) for x in BD["top_cutters"][:6]]}
print(json.dumps(R["capex"], indent=1, default=str)[:1100])
assert d["n"] >= 120, "coverage thin: %d" % d["n"]
assert len(SC) >= 8, "sectors thin: %d" % len(SC)
assert len(HY["rows"]) >= 7 and all(isinstance(x.get("capex_ttm_b"), (int, float)) for x in HY["rows"]), "hyperscalers broken"
assert isinstance(HY.get("total_ttm_b"), (int, float)) and HY["total_ttm_b"] >= 150, "hyperscaler ttm insane: %s" % HY.get("total_ttm_b")
print("  excluded_outliers:", [(e["ticker"], e["capex_ttm_b"]) for e in (d.get("excluded_outliers") or [])][:10])
assert isinstance(MK.get("capex_ttm_b"), (int, float)) and 500 <= MK["capex_ttm_b"] <= 3500, "market ttm insane: %s" % MK.get("capex_ttm_b")
assert len(d.get("excluded_outliers") or []) <= 25, "outlier gate runaway"

sect("2/4 FUSE — gfd v1.0.2 + board row")
for fn in ("justhodl-global-flow-desk", "justhodl-signal-board"):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn); print("  synced", fn)
r = lam.invoke(FunctionName="justhodl-global-flow-desk", InvocationType="RequestResponse")
assert not r.get("FunctionError"), r["Payload"].read()[:200]
g = json.loads(s3.get_object(Bucket=BUCKET, Key="data/global-flow-desk.json")["Body"].read())
CX = g.get("capex") or {}
R["gfd_capex"] = CX
print("  gfd capex:", json.dumps(CX, default=str)[:260])
assert CX.get("status") == "OK" and CX.get("source") == "capex-pulse", "gfd fusion failed: %s" % CX
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
m = re.search(r"\{[^{}]*CapEx Pulse[^{}]*\}", sb)
assert m, "board row missing"
R["board"] = m.group(0)[:220]; print("  board:", R["board"])

sect("3/4 PAGE")
time.sleep(70)
try:
    with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/global-flow-desk.html",
                                headers={"User-Agent": "jh/1"}), timeout=20) as rr:
        R["page"] = "LIVE" if "CAPEX PULSE" in rr.read().decode("utf-8", "ignore") else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])

sect("4/4 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2722_capex_pulse.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2722 COMPLETE — corporate capex measured in dollars")

# rev2
