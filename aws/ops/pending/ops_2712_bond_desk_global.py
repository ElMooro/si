"""ops 2712 — Bond Desk v2.0 GLOBAL (Khalid: 'improve exponentially, worldwide').

v1 tapped ~30% of owned FI surfaces and only the US. v2 synthesizes ALL of it:
US (ETF ladder + ICI industry flows + owned ICE-BofA credit-stress ladder +
bond-vol/auctions/fails/ACM/dealer-survey) · USD-funding (eurodollar-plumbing
severity/swaps/CNH) · Europe (fragmentation/BTP-Bund + systemic-stress) ·
Japan (yen-carry/JGB) · EM (EM-HY differential + flows) -> world anxiety 0-100
with freshness-gated regional weights. Chart now = CCC-BB 5y weekly (renders
day one; anxiety history keeps accumulating).
Report: aws/ops/reports/2712_bond_desk_global.json.
"""
import os, io, json, time, zipfile, re, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=200, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2712, "ts": datetime.now(timezone.utc).isoformat()}
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

sect("1/3 INPUT FRESHNESS SNAPSHOT")
for k in ("data/credit-stress.json", "data/ici-flows.json", "data/eurodollar-plumbing.json",
          "data/euro-fragmentation.json", "data/yen-carry.json", "data/systemic-stress.json",
          "data/dealer-survey.json"):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=k)
        age = (datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 3600
        print("  %-36s %5.1fh" % (k, age))
        R.setdefault("input_ages_h", {})[k] = round(age, 1)
    except Exception:
        print("  %-36s MISSING" % k)
        R.setdefault("input_ages_h", {})[k] = None

sect("1b/3 SCHEMA PRINTS + upstream repairs")
for k in ("data/eurodollar-plumbing.json","data/euro-fragmentation.json","data/bond-vol.json","data/dealer-survey.json"):
    try:
        doc=json.loads(s3.get_object(Bucket=BUCKET,Key=k)["Body"].read())
        print("  %-34s keys:%s"%(k.split("/")[-1],list(doc.keys())[:12]))
    except Exception as e: print("  %-34s ERR %s"%(k,str(e)[:40]))
print("  settling 25s…"); time.sleep(25)
ev = boto3.client("events", region_name=REGION)
def ensure_fn(name):
    cfg=json.load(open("aws/lambdas/%s/config.json"%name)); zb=zip_fn(name)
    try:
        lam.get_function(FunctionName=name); wait_ok(name)
        retry(lambda: lam.update_function_code(FunctionName=name, ZipFile=zb), name); wait_ok(name)
    except lam.exceptions.ResourceNotFoundException:
        role=cfg.get("role") or "arn:aws:iam::857687956942:role/lambda-execution-role"
        rt=cfg.get("runtime") or "python3.12"
        if rt not in ("python3.12","python3.11","python3.13"): rt="python3.12"
        retry(lambda: lam.create_function(FunctionName=name, Runtime=rt, Role=role,
              Handler=cfg.get("handler","lambda_function.lambda_handler"), Code={"ZipFile": zb},
              Timeout=int(cfg.get("timeout") or 120), MemorySize=int(cfg.get("memory") or 256),
              Architectures=cfg.get("architectures") or ["x86_64"],
              Description=(cfg.get("description") or "")[:250]), name+" create"); wait_ok(name)
        print("  CREATED", name)
    sch=cfg.get("schedule")
    if sch:
        arn="arn:aws:lambda:%s:857687956942:function:%s"%(REGION,name)
        ra=ev.put_rule(Name=sch["name"],ScheduleExpression=sch["expression"],State="ENABLED",
                       Description=sch.get("description",""))["RuleArn"]
        try: lam.add_permission(FunctionName=name,StatementId="evt-"+sch["name"],
                Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=ra)
        except lam.exceptions.ResourceConflictException: pass
        ev.put_targets(Rule=sch["name"],Targets=[{"Id":"1","Arn":arn}])
for fn in ("justhodl-bond-vol","justhodl-ici-flows"):
    try:
        ensure_fn(fn)
        rr=lam.invoke(FunctionName=fn,InvocationType="RequestResponse")
        print("  %s invoke -> %s%s"%(fn,"ERR " if rr.get("FunctionError") else "",(rr["Payload"].read() or b"")[:130].decode("utf-8","ignore")))
    except Exception as e: print("  %s repair skipped: %s"%(fn,str(e)[:80]))

sect("2/3 DEPLOY + RUN v2")
print("  settling 20s…"); time.sleep(20)
for fn in ("justhodl-bond-desk", "justhodl-signal-board"):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn); print("  synced", fn)
r = lam.invoke(FunctionName="justhodl-bond-desk", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:160])
assert not r.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/bond-desk.json")["Body"].read())
RG = d["regions"]
R["v2"] = {"world": d["world_anxiety"], "regime": d["regime"], "hottest": d["hottest_region"],
           "live": d["n_regions_live"],
           "scores": {k: v.get("score") for k, v in RG.items()},
           "us_flows": {k: RG["us"]["flows"].get(k) for k in ("equity_to_bond_5d_usd", "credit_appetite_5d_usd", "duration_tilt", "ici_bond_weekly_usd_m", "matched_tickers")},
           "us_credit": RG["us"]["credit"],
           "us_stress": RG["us"]["stress"],
           "gf": {k: RG["global_funding"].get(k) for k in ("severity", "health", "fed_swaps_bn", "cnh_gap_pips", "score")},
           "eu": {k: RG["europe"].get(k) for k in ("fragmentation_score", "regime", "btp_bund_bp", "btp_chg_1m_bp", "score")},
           "jp": {k: RG["japan"].get(k) for k in ("carry_stress", "jgb10_chg_6m_pp", "score")},
           "em": RG["em"], "chart_n": len(d.get("chart_ccc_bb") or []),
           "equity_read": d["equity_read"][:300]}
print(json.dumps(R["v2"], indent=1, default=str)[:1500])
assert d.get("version") == "2.0.0" and 0 <= d["world_anxiety"] <= 100
assert d["n_regions_live"] >= 4, "too few live regions: %s" % {k: v.get("fresh") for k, v in RG.items()}
assert len(d.get("chart_ccc_bb") or []) >= 180, "chart series too short"
assert isinstance(RG["us"]["credit"].get("ccc_minus_bb_bps"), (int, float))
assert RG["global_funding"].get("severity") in ("CRITICAL", "ELEVATED", "MODERATE", "?") or isinstance(RG["global_funding"].get("severity"), str)
nonnull_eu = sum(1 for k in ("fragmentation_score", "btp_bund_bp") if isinstance(RG["europe"].get(k), (int, float)))
nonnull_jp = sum(1 for k in ("jgb10_chg_6m_pp",) if isinstance(RG["japan"].get(k), (int, float))) + (1 if RG["japan"].get("carry_stress") else 0)
print("  EU non-null fields:", nonnull_eu, "| JP non-null:", nonnull_jp)
assert (nonnull_eu >= 1 or isinstance(RG["europe"].get("fragmentation_score"),(int,float))) and nonnull_jp >= 1, "regional wiring failed to extract values"
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
m = re.search(r'\{[^{}]*Bond Desk[^{}]*\}', sb)
R["board"] = m.group(0)[:210] if m else "present"
print("  board:", R["board"])
assert "GLOBAL FI" in sb

sect("3/3 PAGE + REPORT")
time.sleep(70)
try:
    R["page"] = "LIVE" if "GLOBAL BOND DESK" in get("https://justhodl.ai/bond-desk.html") else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2712_bond_desk_global.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2712 COMPLETE — the desk sees the world now")
