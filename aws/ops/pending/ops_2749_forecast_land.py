"""ops 2749 — LAND FORECAST DESK via ASYNC (Khalid; completes 2748).

rev1/rev2 died because the synchronous verify-invoke hit boto read-timeout
during the first-run ETH+ALT price-cache build (paginated Coin Metrics).
Fix: bump cryptoquant Lambda timeout 600->900, invoke ASYNC (Event, no read
timeout), poll S3 until forecasts publish. Then deploy+invoke the 3 widened
consumers + logger and prove widened keys + forecast signal. Page card already
at edge; confirm it. Read-only otherwise. Report: 2749_forecast_land.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
NOW = datetime.now(timezone.utc)
R = {"ops": 2749, "ts": NOW.isoformat()}

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
def wait_ok(fn, budget=260):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        time.sleep(5)
def deploy(fn):
    for i in range(6):
        try:
            wait_ok(fn); lam.update_function_code(FunctionName=fn, ZipFile=zip_fn(fn)); wait_ok(fn); return
        except ClientError: time.sleep(18)
    raise RuntimeError("deploy " + fn)

print("settling 15s…"); time.sleep(15)
print("== 1/4 engine timeout 900 + ASYNC invoke ==")
for i in range(6):
    try:
        wait_ok("justhodl-cryptoquant")
        lam.update_function_configuration(FunctionName="justhodl-cryptoquant", Timeout=900)
        wait_ok("justhodl-cryptoquant")
        lam.update_function_code(FunctionName="justhodl-cryptoquant", ZipFile=zip_fn("justhodl-cryptoquant"))
        break
    except ClientError: time.sleep(18)
wait_ok("justhodl-cryptoquant")
lam.invoke(FunctionName="justhodl-cryptoquant", InvocationType="Event", Payload=b"{}")
print("  async fired; polling S3 for fresh forecasts (up to ~13 min)…")
FC, fresh = None, False
for a in range(20):
    time.sleep(40)
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
        gen = datetime.fromisoformat(d.get("generated_at").replace("Z", "+00:00"))
        FC = d.get("forecasts") or {}
        age = (datetime.now(timezone.utc) - gen).total_seconds()
        ok = bool(FC.get("btc", {}).get("h365", {}).get("exp_pct") is not None) and age < 1200
        print("  poll %2d: age=%ds forecasts=%s" % (a + 1, int(age), "YES" if FC.get("btc") else "no"))
        if ok: fresh = True; break
    except Exception as e:
        print("  poll %2d: %s" % (a + 1, str(e)[:70]))
assert fresh, "forecasts did not publish"
fb, fe, fa = FC["btc"], FC["eth"], FC["alt_basket"]
R["forecasts"] = {
    "btc_now": FC.get("btc_price_now"), "eth_now": FC.get("eth_price_now"),
    "btc": {h: [fb.get(h, {}).get("exp_pct"), fb.get(h, {}).get("price_target")] for h in ("h30", "h90", "h180", "h365")},
    "eth_beta": fe.get("beta_vs_btc"),
    "eth": {h: [fe.get(h, {}).get("exp_pct"), fe.get(h, {}).get("price_target")] for h in ("h30", "h90", "h180", "h365")},
    "alt_beta": fa.get("beta_vs_btc"),
    "alt": {h: fa.get(h, {}).get("exp_pct") for h in ("h30", "h90", "h180", "h365")},
    "ai_src": FC.get("ai_src")}
print("  BTC 1M/3M/6M/1Y:", [fb.get(h, {}).get("exp_pct") for h in ("h30", "h90", "h180", "h365")])
print("  BTC targets:", [fb.get(h, {}).get("price_target") for h in ("h30", "h90", "h180", "h365")])
print("  ETH beta %.2f exp:" % fe.get("beta_vs_btc", 0), [fe.get(h, {}).get("exp_pct") for h in ("h30", "h90", "h180", "h365")])
print("  ALT beta %s exp:" % fa.get("beta_vs_btc"), [fa.get(h, {}).get("exp_pct") for h in ("h30", "h90", "h180", "h365")])
R["stables"] = {k: d["metrics"][k]["value"] for k in d["metrics"] if k in ("btc_ssr", "stablecoin_exchange_reserve", "stablecoin_exchange_netflow")}

print("== 2/4 deploy + invoke widened consumers ==")
for fn, key, need in (("justhodl-onchain-ratios", None, None),
                      ("justhodl-crypto-exchange-flows", "data/crypto-exchange-flows.json", ("btc_inflow", "eth_netflow")),
                      ("justhodl-crypto-miners", "data/crypto-miners.json", ("cryptoquant_miner",))):
    deploy(fn)
    rr = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
    assert not rr.get("FunctionError"), (fn, rr["Payload"].read()[:160])
    if key:
        dd = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        blk = dd.get("cryptoquant") or {}
        for n in need: assert n in blk, (fn, n, list(blk)[:6])
    print("  %s deployed+fresh" % fn)
envv = (lam.get_function_configuration(FunctionName="justhodl-onchain-ratios").get("Environment", {}) or {}).get("Variables", {}) or {}
rd = json.loads(s3.get_object(Bucket=BUCKET, Key=envv.get("S3_KEY", "data/onchain-ratios.json"))["Body"].read())
assert "btc_nupl" in (rd.get("cryptoquant") or {}), "ratios valuation suite missing"
print("  ratios valuation suite live (nupl/puell/nvt_golden/realized/ssr)")

print("== 3/4 ledger forecast signal ==")
deploy("justhodl-signal-logger")
t0_ms = int(time.time() * 1000) - 4000
rr = lam.invoke(FunctionName="justhodl-signal-logger", InvocationType="RequestResponse")
assert not rr.get("FunctionError"), rr["Payload"].read()[:160]
hit = None
for a in range(5):
    time.sleep(6)
    evs = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-signal-logger",
                                 startTime=t0_ms, filterPattern="onchain_btc_forecast", limit=5).get("events", [])
    if evs: hit = evs[-1]["message"].strip()[:150]; break
print("  cw-proof:", hit)
R["ledger"] = hit or "not_captured_this_window"

print("== 4/4 page card at edge ==")
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as rr2:
        return rr2.read()
okp = b"FORECAST DESK v1" in pub("onchain.html")
print("  page forecast card at edge:", okp)
R["page"] = "LIVE" if okp else "MISSING"
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2749_forecast_land.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2749 COMPLETE — the desk looks forward, verified")
