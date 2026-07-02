"""ops 2715 — SHORT-Z ARM + FACTOR-RETURNS DESK (Khalid's two named builds).

A) Dark-pool v2.0.1: finra-short's daily rows already ship z_score — the
broken history-parser is deleted and short_z now maps straight from the feed,
arming HIGH-conviction + DISTRIBUTION_INTO_STRENGTH flags. dix extractor
handles the nested current/dix shape.
B) NEW justhodl-factor-returns: daily long-short decile returns (MOM 12-1,
VALUE, QUALITY, SIZE, LOWVOL) on the full FinViz custom-export cross-section
via shared FV.build_universe() — crowding, rotation regime, self-accumulating
history. Distinct from factor-risk/smart-beta (they score names; this
measures the factors). Board rows + page.
Report: aws/ops/reports/2715_shortz_factors.json.
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
R = {"ops": 2715, "ts": datetime.now(timezone.utc).isoformat()}
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
        role = cfg.get("role") or "arn:aws:iam::857687956942:role/lambda-execution-role"
        retry(lambda: lam.create_function(FunctionName=name, Runtime=cfg.get("runtime", "python3.12"), Role=role,
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
        print("  rule", sch["name"], sch["expression"])

sect("1/4 DARK-POOL v2.0.1 — arm short_z from feed's z_score")
print("  settling 30s…"); time.sleep(30)
retry(lambda: (wait_ok("justhodl-dark-pool"), lam.update_function_code(FunctionName="justhodl-dark-pool", ZipFile=zip_fn("justhodl-dark-pool")))[-1], "dp")
wait_ok("justhodl-dark-pool")
r = lam.invoke(FunctionName="justhodl-dark-pool", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:160])
assert not r.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/dark-pool.json")["Body"].read())
brd = d.get("board") or []
z_n = sum(1 for x in brd if isinstance(x.get("daily_short_z"), (int, float)))
hi = [x["ticker"] for x in brd if x.get("conviction") == "HIGH"][:10]
dis = [x["ticker"] for x in brd if x.get("flag") == "DISTRIBUTION_INTO_STRENGTH"][:10]
R["dark_pool"] = {"z_joined": z_n, "of": len(brd), "high_conviction": hi, "dist_into_strength": dis,
                  "own_dix": (d.get("dix") or {}).get("own_dix_pct"), "sq_dix": (d.get("dix") or {}).get("squeezemetrics_dix")}
print(json.dumps(R["dark_pool"], indent=1)[:500])
fh = json.loads(s3.get_object(Bucket=BUCKET, Key="data/finra-short-history.json")["Body"].read())
tk0 = next(iter((fh.get("tickers") or {}).items()), ("?", None))
print("  store sample:", tk0[0], type(tk0[1]).__name__,
      repr(tk0[1][:3] if isinstance(tk0[1], list) else (list(tk0[1].items())[:3] if isinstance(tk0[1], dict) else tk0[1]))[:260])
def _rep_svr(rec):
    if isinstance(rec, list):
        out = []
        for x in rec:
            if isinstance(x, (int, float)): out.append(float(x))
            elif isinstance(x, dict):
                v = x.get("svr") if isinstance(x.get("svr"), (int, float)) else x.get("svr_pct")
                if isinstance(v, (int, float)): out.append(v)
        return out
    if isinstance(rec, dict):
        for k in ("svr", "svr_history", "series", "values"):
            if isinstance(rec.get(k), list): return _rep_svr(rec[k])
    return []
rep_ok = sum(1 for v in (fh.get("tickers") or {}).values() if len(_rep_svr(v)) >= 15)
print("  ops-side extraction: names with >=15 svr obs =", rep_ok)
lens = sorted(len(v) if isinstance(v, list) else 0 for v in (fh.get("tickers") or {}).values())
import statistics as _s
R["dark_pool"]["history_store"] = {"tickers": len(lens), "median_days": lens[len(lens)//2] if lens else 0,
                                   "max_days": lens[-1] if lens else 0, "ge15": sum(1 for x in lens if x >= 15)}
print("  history store:", R["dark_pool"]["history_store"])
assert z_n >= min(40, max(10, R["dark_pool"]["history_store"]["ge15"] // 3)), "short_z dormant beyond store age: %d" % z_n
assert isinstance(R["dark_pool"]["sq_dix"], (int, float)) or R["dark_pool"]["sq_dix"] is None

sect("2/4 FACTOR-RETURNS — create + env from finviz-signals + run")
src_env = (lam.get_function_configuration(FunctionName="justhodl-finviz-signals").get("Environment") or {}).get("Variables") or {}
print("  finviz env keys:", sorted(src_env.keys()))
ensure_fn("justhodl-factor-returns")
if src_env:
    wait_ok("justhodl-factor-returns")
    retry(lambda: lam.update_function_configuration(FunctionName="justhodl-factor-returns", Environment={"Variables": src_env}), "env")
    wait_ok("justhodl-factor-returns")
r = lam.invoke(FunctionName="justhodl-factor-returns", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:260])
assert not r.get("FunctionError"), pay
f = json.loads(s3.get_object(Bucket=BUCKET, Key="data/factor-returns.json")["Body"].read())
FA, RG = f["factors"], f["regime"]
ok = {k: v for k, v in FA.items() if v.get("status") == "OK"}
R["factors"] = {"universe": f["universe_n"], "ok_factors": {k: v["ls_ret_1d_pct"] for k, v in ok.items()},
                "thin": [k for k, v in FA.items() if v.get("status") != "OK"],
                "mom_crowding": (FA.get("MOMENTUM") or {}).get("crowding_dollar_share_pct"),
                "mom_pe_vs_univ": [(FA.get("MOMENTUM") or {}).get("top_decile_median_pe"), (FA.get("MOMENTUM") or {}).get("universe_median_pe")],
                "regime": RG.get("read"), "flags": RG.get("flags"),
                "top_mom_names": (FA.get("MOMENTUM") or {}).get("top_names")}
print(json.dumps(R["factors"], indent=1, default=str)[:800])
assert f["universe_n"] >= 2500, "universe thin: %d" % f["universe_n"]
assert len(ok) >= 4, "factors OK<4: %s" % list(FA.keys())
for k, v in ok.items():
    assert isinstance(v.get("ls_ret_1d_pct"), (int, float)) and abs(v["ls_ret_1d_pct"]) < 8
assert isinstance((FA.get("MOMENTUM") or {}).get("crowding_dollar_share_pct"), (int, float))
assert RG.get("read") and RG.get("leader")
h = json.loads(s3.get_object(Bucket=BUCKET, Key="data/history/factor-returns.json")["Body"].read())
assert len(h) >= 1

sect("3/4 BOARD + PAGES")
retry(lambda: (wait_ok("justhodl-signal-board"), lam.update_function_code(FunctionName="justhodl-signal-board", ZipFile=zip_fn("justhodl-signal-board")))[-1], "board")
wait_ok("justhodl-signal-board")
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
for mk in ("Factor Returns", "Dark Pool"):
    m = re.search(r"\{[^{}]*" + mk + r"[^{}]*\}", sb)
    assert m, mk
    R.setdefault("board", {})[mk] = m.group(0)[:190]
    print("  board:", R["board"][mk])
time.sleep(70)
try:
    R["page"] = "LIVE" if "FACTOR RETURNS" in get("https://justhodl.ai/factor-returns.html") else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])

sect("4/4 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2715_shortz_factors.json", "w") as f2:
    json.dump(R, f2, indent=1, default=str)
print("OPS 2715 COMPLETE — flags armed + the style desk is live")

# rev2

# rev3
