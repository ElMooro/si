"""ops 2716 — STOCK X-RAY: the per-name institutional umbrella (Khalid's directive).

Everything the fleet knows about a name, ONE card, ONE page: FinViz 151-col
backbone (MA stack 20/50/200 + Weinstein stage + 52w + momentum 12-1 +
sector-relative valuation percentile + growth + TURNING-PROFITABLE +
ownership/short-float) joined nightly with master-rank, equity-confluence,
resilience, dark-pool xray_map (v2.2 adds compact all-936 map), factor-decile
memberships, estimate-revisions, best-setups, backlog/RPO, supply-chain peers.
Derived boards: multibaggers, turning-profitable, accumulation leaders, DIS
warnings, full-stack highs, laggards-with-strong-peers. dossier.html rebuilt
as the Stock X-Ray renderer with chart-pro deeplink.
Report: aws/ops/reports/2716_stock_xray.json.
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
R = {"ops": 2716, "ts": datetime.now(timezone.utc).isoformat()}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)
def get(url, timeout=22):
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh/1"}), timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")
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
        ra = ev.put_rule(Name=sch["name"], ScheduleExpression=sch["expression"], State="ENABLED", Description=sch.get("description", ""))["RuleArn"]
        try:
            lam.add_permission(FunctionName=name, StatementId="evt-" + sch["name"], Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com", SourceArn=ra)
        except lam.exceptions.ResourceConflictException: pass
        ev.put_targets(Rule=sch["name"], Targets=[{"Id": "1", "Arn": arn}])

sect("1/4 DARK-POOL v2.2 (xray_map) + refresh feed")
print("  settling 30s…"); time.sleep(30)
ensure_fn("justhodl-dark-pool")
r = lam.invoke(FunctionName="justhodl-dark-pool", InvocationType="RequestResponse")
assert not r.get("FunctionError"), r["Payload"].read()[:200]
dpx = (json.loads(s3.get_object(Bucket=BUCKET, Key="data/dark-pool.json")["Body"].read()).get("xray_map") or {})
print("  xray_map names:", len(dpx))
assert len(dpx) >= 800, "xray_map thin"
R["dark_xray_map_n"] = len(dpx)

sect("2/4 STOCK X-RAY — create + run + prove")
ensure_fn("justhodl-stock-xray")
r = lam.invoke(FunctionName="justhodl-stock-xray", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:260])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/stock-xray.json")["Body"].read())
C, B, JN = d["cards"], d["boards"], d["joins"]
R["xray"] = {"n_cards": d["n_cards"], "joins": JN, "boards": {k: v[:8] for k, v in B.items()},
             "board_sizes": {k: len(v) for k, v in B.items()}}
samp = C.get("NVDA") or C.get("AAPL") or list(C.values())[0]
st = samp
R["sample_NVDA"] = {k: st.get(k) for k in ("px", "mc_b", "ma", "stage", "pos52", "momo_12_1", "val", "profit", "dark", "rank", "confl", "factors", "peers")}
print(json.dumps(R["xray"], indent=1, default=str)[:900])
print("  NVDA card:", json.dumps(R["sample_NVDA"], default=str)[:700])
assert d["n_cards"] >= 2200, "cards thin: %d" % d["n_cards"]
assert JN.get("dp", 0) >= 500, "dp join thin: %s" % JN
assert JN.get("fm", 0) >= 40 or JN.get("mr", 0) >= 300, "factor+rank joins broken: %s" % JN
print("  soft joins ec/er/rs:", {k: JN.get(k) for k in ("ec", "er", "rs")})
assert samp.get("ma") and "stack" in samp["ma"] and samp.get("stage") and samp.get("val")
for bname in ("multibagger_candidates", "turning_profitable", "full_stack_highs", "accumulation_leaders"):
    assert len(B.get(bname) or []) >= 3, bname + " empty"
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")

sect("3/4 BOARD + PAGE")
retry(lambda: (wait_ok("justhodl-signal-board"), lam.update_function_code(FunctionName="justhodl-signal-board", ZipFile=zip_fn("justhodl-signal-board")))[-1], "board")
wait_ok("justhodl-signal-board")
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
assert "X-RAY" in sb
m = re.search(r"\{[^{}]*Stock X-Ray[^{}]*\}", sb)
R["board"] = m.group(0)[:200] if m else "present"
print("  board:", R["board"])
time.sleep(70)
try:
    R["page"] = "LIVE" if "STOCK X-RAY" in get("https://justhodl.ai/dossier.html") else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])

sect("4/4 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2716_stock_xray.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2716 COMPLETE — every stock, one X-Ray")

# rev2
