"""ops 2713 — Bond Desk v3.0: WORLD MAP + CRISIS ANALOGS + AI INTERPRETATION.

Khalid: more data, anxiety for every part of the world, AI that compares to
prior crises and projects asset-class impact. v3 adds: 14-tile world map
(rich regions from owned engines; rest-of-DM from FRED OECD 10y 6m repricing
— the JGB duration-shock logic applied everywhere), a deterministic crisis-
analog engine (today's 6-dim fingerprint vs 10 named crises 2008->2024 on
FULL-history FRED series, with MEASURED forward returns SPY/TLT/GLD/HYG/BTC
at +21/63/126d from FMP, cached 30d), and the llm_router tier=reason AI brief
(interpretation, crisis comparison, per-asset projections) — with the known
provider outage handled honestly (ai_status) and env keys cloned from
cycle-clock (new lambdas don't inherit them).
Report: aws/ops/reports/2713_bond_desk_v3.json.
"""
import os, io, json, time, zipfile, re, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2713, "ts": datetime.now(timezone.utc).isoformat()}
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

sect("1/4 ENV KEYS — clone LLM env from cycle-clock (new lambdas don't inherit)")
src_env = (lam.get_function_configuration(FunctionName="justhodl-cycle-clock").get("Environment") or {}).get("Variables") or {}
cur_env = (lam.get_function_configuration(FunctionName="justhodl-bond-desk").get("Environment") or {}).get("Variables") or {}
merged = {**src_env, **cur_env}
print("  cycle-clock env keys:", sorted(src_env.keys()))
wait_ok("justhodl-bond-desk")
retry(lambda: lam.update_function_configuration(FunctionName="justhodl-bond-desk",
        Environment={"Variables": merged}, Timeout=280, MemorySize=512), "env")
wait_ok("justhodl-bond-desk")
R["env_keys"] = sorted(merged.keys())
try: s3.delete_object(Bucket=BUCKET, Key="data/history/bond-crisis-analogs.json")
except Exception: pass
print("  env merged + timeout 280 + analog cache purged")

sect("2/4 DEPLOY + RUN v3")
print("  settling 25s…"); time.sleep(25)
retry(lambda: lam.update_function_code(FunctionName="justhodl-bond-desk", ZipFile=zip_fn("justhodl-bond-desk")), "code")
wait_ok("justhodl-bond-desk")
r = lam.invoke(FunctionName="justhodl-bond-desk", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:200])
assert not r.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/bond-desk.json")["Body"].read())
tiles = d.get("world_map") or []
AN = d.get("crisis_analogs") or {}
live_tiles = [t for t in tiles if isinstance(t.get("score"), (int, float))]
R["v3"] = {"world": d["world_anxiety"], "regime": d["regime"], "version": d.get("version"),
           "tiles_live": len(live_tiles), "tiles": [{k: t.get(k) for k in ("code", "score", "metric")} for t in tiles],
           "analog_status": AN.get("status"), "analog_top": AN.get("top"),
           "analog_n": len(AN.get("all") or []), "current_vector": AN.get("current_vector"),
           "ai_status": d.get("ai_status"), "ai_brief": d.get("ai_brief"),
           "chart_n": len(d.get("chart_ccc_bb") or [])}
print(json.dumps({k: R["v3"][k] for k in ("world", "regime", "version", "tiles_live", "analog_status", "analog_n", "ai_status", "chart_n")}, indent=1))
print("  TOP ANALOGS:", json.dumps(AN.get("top"), default=str)[:600])
if d.get("ai_brief"): print("  AI BRIEF:", json.dumps(d["ai_brief"], default=str)[:500])
assert d.get("version") == "3.0.0"
assert len(live_tiles) >= 11, "tiles thin: %d" % len(live_tiles)
assert AN.get("status") == "OK" and len(AN.get("all") or []) >= 8, "analogs failed: %s" % AN.get("status")
top = (AN.get("top") or [{}])[0]
assert isinstance(top.get("similarity_pct"), (int, float)) and len(top.get("fwd") or {}) >= 3
assert sum(1 for a in AN["all"] if len(a.get("fwd") or {}) >= 3) >= 6, "fwd returns sparse"
assert d.get("ai_status") in ("LIVE", "PROVIDER_DOWN", "ROUTER_MISSING")
if d["ai_status"] == "LIVE":
    assert isinstance(d["ai_brief"], dict) and "projections" in d["ai_brief"]
assert len(d.get("chart_ccc_bb") or []) >= 150

sect("3/4 BOARD SANITY + PAGE")
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
assert "GLOBAL FI" in sb
time.sleep(70)
try:
    pg = get("https://justhodl.ai/bond-desk.html")
    R["page"] = "LIVE" if ("WORLD MAP" in pg and "CRISIS ANALOGS" in pg) else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])

sect("4/4 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2713_bond_desk_v3.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2713 COMPLETE — world map + crisis DNA + AI layer")
