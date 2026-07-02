"""ops 2708 — the two queued Leverage Monitor refinements, closed.

(1) spec-ETF layer now reads the radar's per-complex bull/bear LEVERAGED 5d
flows directly (no z existed for that slice — engine accumulates its own daily
net-tilt history, z activates at 40 obs) + crypto layer aggregates the actual
per-asset OKX perp rows (funding_z med/max + honestly-scoped OI sum).
(2) leverage.html registered in the universal jh-nav-drawer manifest (all 338
pages) + directory.html Macro & Liquidity (count 67→68).
Report: aws/ops/reports/2708_leverage_refinements.json.
"""
import os, io, json, time, zipfile, re, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=170, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2708, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) jh/1"}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)
def get(url, timeout=20):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")

sect("1/3 DEPLOY engine v2.1")
print("  settling 30s…"); time.sleep(30)
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            for f in files:
                z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"):
                z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
            return
        time.sleep(5)
FN = "justhodl-margin-lending"
for _try in range(6):
    try:
        wait_ok(FN); lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN)); break
    except ClientError as e:
        if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
            time.sleep(18)
        else: raise
wait_ok(FN); print("  synced", FN)

sect("2/3 RUN + PROVE precise layers")
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
assert not r.get("FunctionError"), (r["Payload"].read() or b"")[:200]
doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/leverage-monitor.json")["Body"].read())
LM = doc["leverage_monitor"]; E = LM["layers"]["spec_etf"]; C = LM["layers"]["crypto"]
R["spec_etf"] = {k: E.get(k) for k in ("status","net_lev_5d_usd","pct_complexes_bull","n_complexes","tilt_z","tilt_history_n","provisional","top3")}
R["crypto"]   = {k: C.get(k) for k in ("status","assets_n","funding_z_med","funding_z_max","okx_perp_oi_usd_b")}
R["cycle"] = {"score": LM["cycle_score"], "phase": LM["phase"], "layers_live": LM["n_layers_live"]}
print(json.dumps(R, indent=1, default=str)[:900])
assert E.get("status") == "OK" and isinstance(E.get("net_lev_5d_usd"), (int, float)), "spec-ETF layer bad: %s" % E
assert (E.get("n_complexes") or 0) >= 30 and (E.get("tilt_history_n") or 0) >= 1, "tilt history not accumulating: %s" % E
th = json.loads(s3.get_object(Bucket=BUCKET, Key="data/history/lev-etf-tilt.json")["Body"].read())
assert len(th) >= 1, "tilt history file missing"
assert C.get("status") == "OK" and (C.get("assets_n") or 0) >= 3, "crypto rows bad: %s" % C
assert isinstance(C.get("funding_z_med"), (int, float)), "funding z missing"
assert (C.get("okx_perp_oi_usd_b") or 0) >= 1, "OI implausible: %s" % C
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
assert "Leverage Cycle" in sb

sect("3/3 NAV REGISTRATION LIVE")
raw = get("https://raw.githubusercontent.com/ElMooro/si/main/nav-manifest.json")
assert "/leverage.html" in raw, "nav-manifest commit missing leverage"
R["nav_manifest_repo"] = "OK"
time.sleep(75)
for url, key in (("https://justhodl.ai/nav-manifest.json", "nav_live"),
                 ("https://justhodl.ai/directory.html", "directory_live")):
    try:
        R[key] = "LIVE" if "leverage" in get(url).lower() else "200_no_marker"
    except Exception as e:
        R[key] = "propagating: " + str(e)[:50]
    print(" ", key, R[key])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2708_leverage_refinements.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2708 COMPLETE")
