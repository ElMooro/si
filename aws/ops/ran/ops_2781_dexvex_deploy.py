"""ops 2781 — deploy DEX/VEX enhancement to justhodl-dealer-gex + invoke + verify.
Adds total_delta_dollars (DEX) + total_vega_dollars (VEX) to each underlying —
genuinely new dealer-positioning greeks (delta/vega exposure) not previously computed.
Read+deploy+verify. Report: 2781_dexvex_deploy.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-dealer-gex"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=780, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2781, "ts": datetime.now(timezone.utc).isoformat()}
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn; buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files: z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        if os.path.isdir("aws/shared"):
            for f in sorted(os.listdir("aws/shared")):
                if f.endswith(".py"): z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, b=180):
    t0 = time.time()
    while time.time() - t0 < b:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        except ClientError: pass
        time.sleep(5)
print("settling 12s…"); time.sleep(12)
print("== deploy dealer-gex (DEX/VEX) ==")
for i in range(6):
    try:
        wait_ok(FN); lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN)); wait_ok(FN); break
    except ClientError as e:
        print("  retry", str(e)[:60]); time.sleep(12)
print("  deployed")
print("== invoke (full model run) ==")
t0 = time.time()
resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
raw = resp["Payload"].read()
R["invoke_error"] = resp.get("FunctionError")
R["invoke_head"] = raw[:200].decode("utf-8", "ignore")
R["invoke_secs"] = round(time.time() - t0, 1)
print("  invoke %s in %.1fs: %s" % (("ERROR" if resp.get("FunctionError") else "OK"), R["invoke_secs"], raw[:160].decode("utf-8", "ignore")))
assert not resp.get("FunctionError"), "dealer-gex errored after DEX/VEX edit: " + raw[:300].decode("utf-8", "ignore")
print("== verify new fields in feed ==")
time.sleep(3)
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/dealer-gex.json")["Body"].read())
und = d.get("underlyings") or {}
R["underlyings"] = {}
for sym in ("SPY", "QQQ", "IWM"):
    u = und.get(sym) or {}
    R["underlyings"][sym] = {
        "spot": u.get("spot"),
        "net_gex_b": u.get("total_dealer_gex_billions"),
        "vanna_$": u.get("total_vanna_dollars"),
        "charm_$/day": u.get("total_charm_dollars_per_day"),
        "DEX_delta_$": u.get("total_delta_dollars"),
        "VEX_vega_$": u.get("total_vega_dollars"),
        "has_DEX": u.get("total_delta_dollars") is not None,
        "has_VEX": u.get("total_vega_dollars") is not None,
    }
    print("  %s: DEX=%s VEX=%s (spot %s, net_gex %sB)" % (
        sym, u.get("total_delta_dollars"), u.get("total_vega_dollars"), u.get("spot"), u.get("total_dealer_gex_billions")))
spy = und.get("SPY") or {}
assert spy.get("total_delta_dollars") is not None, "DEX missing from SPY after deploy"
assert spy.get("total_vega_dollars") is not None, "VEX missing from SPY after deploy"
R["status"] = "DEX/VEX LIVE in dealer-gex.json"
R["feed_generated_at"] = d.get("generated_at")
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2781_dexvex_deploy.json", "w"), indent=1, default=str)
print("\nOPS 2781 COMPLETE —", R["status"])
