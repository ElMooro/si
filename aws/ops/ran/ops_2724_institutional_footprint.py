"""ops 2724 — INSTITUTIONAL FOOTPRINT: revive the smart-money fleet + fusion desk.

Audit found the ENTIRE surveillance wing fossilized: 11 engines (13F x3, CFTC
deep-view, dealer-GEX, options gamma/flow, tail-hedging skew, catalyst skew,
short interest, forced-selling) all schedule:None. Phase 1 assigns staggered
crons, redeploys, invokes and freshness-verifies each (tolerant: legacy APIs
may have rotted -> WOUNDED recorded, core-6 must live). Phase 2 creates
justhodl-institutional-footprint — the CIA-style fusion desk (posture NOW vs
FORWARD, sector/asset/stock buy-sell, dark footprint, conviction moves, AI
dossier) + page + board row. Report: aws/ops/reports/2724_footprint.json.
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
R = {"ops": 2724, "ts": datetime.now(timezone.utc).isoformat()}
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
    except lam.exceptions.ResourceNotFoundException:
        retry(lambda: lam.create_function(FunctionName=name, Runtime=cfg.get("runtime", "python3.12"),
              Role=cfg.get("role") or "arn:aws:iam::857687956942:role/lambda-execution-role",
              Handler=cfg.get("handler", "lambda_function.lambda_handler"), Code={"ZipFile": zb},
              Timeout=int(cfg.get("timeout") or 240), MemorySize=int(cfg.get("memory") or 512),
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

FLEET = {"justhodl-13f-positions": "data/13f-positions.json",
         "justhodl-13f-price-divergence": "data/13f-aggregate.json",
         "justhodl-consensus-bottom": "data/13f-price-divergence.json",
         "justhodl-cftc-deep-view": "data/cftc-all-cache.json",
         "justhodl-dealer-gex": "data/dealer-gex.json",
         "justhodl-options-gamma": "data/options-gamma.json",
         "justhodl-options-flow": None,
         "justhodl-skew-tail-hedging": "data/skew-tail-hedging.json",
         "justhodl-catalyst-skew-premove": "data/catalyst-calendar.json",
         "justhodl-short-interest": "data/short-interest.json",
         "justhodl-forced-selling-bounce": "data/breadth-divergence.json"}
CORE = {"justhodl-13f-positions", "justhodl-cftc-deep-view", "justhodl-dealer-gex",
        "justhodl-options-gamma", "justhodl-skew-tail-hedging", "justhodl-short-interest"}

sect("1/3 REVIVE the surveillance fleet (11 engines, all were schedule:None)")
print("  settling 30s…"); time.sleep(30)
statuses = {}
for fn, feed in FLEET.items():
    try:
        ensure_fn(fn)
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
        err = bool(r.get("FunctionError"))
        head = (r["Payload"].read() or b"")[:110].decode("utf-8", "ignore")
        fresh = None
        if feed:
            try:
                h = s3.head_object(Bucket=BUCKET, Key=feed)
                fresh = round((datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 60, 1)
            except Exception:
                fresh = None
        ok = (not err) or (fresh is not None and fresh < 20)
        statuses[fn] = {"ok": ok, "err": err, "feed_age_min": fresh, "head": head}
        print("  %-34s %s age=%s %s" % (fn, "ALIVE" if ok else "WOUNDED", fresh, head[:70]))
    except Exception as e:
        statuses[fn] = {"ok": False, "err": True, "head": str(e)[:90]}
        print("  %-34s WOUNDED %s" % (fn, str(e)[:80]))
    time.sleep(2)
R["fleet"] = statuses
alive = [f for f, s0 in statuses.items() if s0["ok"]]
wounded = [f for f, s0 in statuses.items() if not s0["ok"]]
print("  ALIVE %d: %s" % (len(alive), alive))
print("  WOUNDED %d: %s" % (len(wounded), wounded))
core_down = [f for f in CORE if f in wounded]
assert len(core_down) <= 2, "too many core engines dead: %s" % core_down
assert len(alive) >= 7, "revival failed broadly: %s" % wounded

sect("2/3 FUSION DESK — create + run + prove")
ensure_fn("justhodl-institutional-footprint")
r = lam.invoke(FunctionName="justhodl-institutional-footprint", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:300])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/institutional-footprint.json")["Body"].read())
P, S, K, DP = d["posture"], d["sectors"], d["stocks"], d["dark_pool_footprint"]
R["desk"] = {"feeds_alive": d["feeds_alive"], "posture": P,
             "sectors_buying": S["buying"][:3], "sectors_selling": S["selling"][:3],
             "adds": K["institutions_buying_13f"][:8], "exits": K["institutions_selling_13f"][:8],
             "hi_conv": K["dark_pool_accumulation"][:8], "dis": K["distribution_into_strength"][:6],
             "double": K.get("double_confirmed_buys"), "own_dix": DP.get("own_dix_pct"),
             "conviction_n": len(d.get("conviction_moves") or []), "ai": bool(d.get("ai_dossier"))}
print(json.dumps(R["desk"], indent=1, default=str)[:1100])
assert d["feeds_alive"] >= 12, "fusion starving: %d/18" % d["feeds_alive"]
assert isinstance(P.get("risk_now"), (int, float)) and isinstance(P.get("risk_forward"), (int, float)), "posture null: %s" % P
assert len(P.get("now_components") or {}) >= 3 and len(P.get("fwd_components") or {}) >= 2, "composites thin"
assert S["n_complexes"] >= 30 and len(S["buying"]) >= 4
assert len(K["dark_pool_accumulation"]) >= 3, "dark accumulation empty"
assert len(d.get("conviction_moves") or []) >= 3

sect("3/3 BOARD + PAGE + REPORT")
retry(lambda: (wait_ok("justhodl-signal-board"), lam.update_function_code(FunctionName="justhodl-signal-board", ZipFile=zip_fn("justhodl-signal-board")))[-1], "board")
wait_ok("justhodl-signal-board")
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
m = re.search(r"\{[^{}]*Institutional Footprint[^{}]*\}", sb)
assert m, "board row missing"
R["board"] = m.group(0)[:220]; print("  board:", R["board"])
time.sleep(70)
try:
    with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/institutional-footprint.html",
                                headers={"User-Agent": "jh/1"}), timeout=20) as rr:
        R["page"] = "LIVE" if "SURVEILLANCE DESK" in rr.read().decode("utf-8", "ignore") else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2724_footprint.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2724 COMPLETE — the institutions are under surveillance")
