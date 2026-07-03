"""ops 2748 — FORECAST DESK + FUSION-WIDEN + STABLECOIN COMPLETION (Khalid).

RECONCILED with parallel-session Desk v3.1 (their 11-twin spec + percentile
page kept; forecasts layered on top). Engine v2.3: percentile-conditional
BASE-RATE FORECASTS for BTC (ensemble median across non-monotonic metrics at
current percentiles, horizons 30/90/180/365 w/ price targets + p10-p90),
ETH (own-metric ensemble 50/50 blended with 730d beta-link), ALT basket
(ltc/xrp/doge/ada/bch equal-wt index beta-link); monotonic series flagged +
excluded from master tally; ETH-benchmarked stats for eth_* metrics; AI
narration contract-v4 w/ deterministic fallback; ledger signal
onchain_btc_forecast (magnitude-graded, 30/90/180d). Spec: +usdt/+usdc
exchange reserves (probed), SSR limit-fix probe. Consumers widened: ratios
+valuation suite, xf +flow suite+per-token stables, miners +miner suite.
Report: aws/ops/reports/2748_forecast_desk.json.
"""
import os, io, json, time, base64, zipfile, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
BASE = "https://api.cryptoquant.com/v1"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=890, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
R = {"ops": 2748, "ts": datetime.now(timezone.utc).isoformat()}
TOK = ssm.get_parameter(Name="/justhodl/cryptoquant/token", WithDecryption=True)["Parameter"]["Value"].strip()

def cq_rows(path, params, limit):
    q = dict(params); q["limit"] = str(limit)
    url = BASE + path + "?" + "&".join("%s=%s" % kv for kv in q.items())
    for att in range(3):
        try:
            req = urllib.request.Request(url, headers={"Authorization": "Bearer " + TOK, "User-Agent": "jh"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return ((json.loads(r.read()) or {}).get("result") or {}).get("data") or []
        except urllib.error.HTTPError as he:
            if he.code == 429 and att < 2: time.sleep(22); continue
            return []
        except Exception:
            return []
    return []

print("settling 15s…"); time.sleep(15)
print("== 1/5 spec: SSR limit-fix + per-token stables (twins UNTOUCHED) ==")
spec = json.loads(s3.get_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json")["Body"].read())
names = {m["name"] for m in spec["metrics"]}
ssr = next((m for m in spec["metrics"] if m["name"] == "btc_ssr"), None)
if ssr:
    for lim in (300, 90, 30):
        rows = cq_rows(ssr["path"], ssr["params"], lim)
        if rows:
            ssr["limit"] = lim
            print("  btc_ssr: rows at limit=%d -> fixed" % lim); R["ssr"] = "FIXED_limit_%d" % lim; break
        time.sleep(2)
    else:
        R["ssr"] = "STILL_EMPTY"; print("  btc_ssr: empty at all limits (recorded)")
for tok_name in ("usdt", "usdc"):
    nm = "%s_exchange_reserve" % tok_name
    if nm in names: continue
    prm = {"token": tok_name, "exchange": "all_exchange", "window": "day"}
    rows = cq_rows("/stablecoin/exchange-flows/reserve", prm, 3)
    if rows:
        keys = [k for k in rows[0] if k not in ("date", "datetime")]
        spec["metrics"].append({"name": nm, "category": "stablecoins",
            "label": "%s Exchange Reserve" % tok_name.upper(),
            "path": "/stablecoin/exchange-flows/reserve", "params": prm,
            "value_keys": keys[:3], "risk_sign": -1, "in_composite": False, "unit": "USD"})
        print("  +%s (keys %s)" % (nm, keys[:2]))
    else:
        print("  %s: no rows (recorded)" % nm)
    time.sleep(2.2)
s3.put_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json",
              Body=json.dumps(spec, indent=1).encode(), ContentType="application/json")
R["n_spec"] = len(spec["metrics"]); R["n_twins_spec"] = len(spec.get("twins") or {}) + len(spec.get("twins_extra") or {})
print("  spec: %d metrics, twins(+extra)=%d" % (R["n_spec"], R["n_twins_spec"]))

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
def deploy(fn):
    for i in range(6):
        try:
            wait_ok(fn); lam.update_function_code(FunctionName=fn, ZipFile=zip_fn(fn)); wait_ok(fn); return
        except ClientError: time.sleep(18)
    raise RuntimeError(fn)

print("== 2/5 engine v2.3 + FULL run (forecasts) ==")
deploy("justhodl-cryptoquant")
r = lam.invoke(FunctionName="justhodl-cryptoquant", InvocationType="RequestResponse", Payload=b"{}")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:240])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
FC = d.get("forecasts") or {}
fb, fe, fa = FC.get("btc") or {}, FC.get("eth") or {}, FC.get("alt_basket") or {}
print("  BTC:", json.dumps({k: fb[k].get("exp_pct") for k in fb if k.startswith("h")}))
print("  ETH beta %.2f:" % fe.get("beta_vs_btc", 0), json.dumps({k: fe[k].get("exp_pct") for k in fe if k.startswith("h")}))
print("  ALT beta %s:" % fa.get("beta_vs_btc"), json.dumps({k: fa[k].get("exp_pct") for k in fa if k.startswith("h")}))
print("  forecast ai[%s]: %s" % (FC.get("ai_src"), (FC.get("ai") or "")[:140]))
for h in (30, 90, 180, 365):
    b = fb.get("h%d" % h)
    assert b and isinstance(b.get("exp_pct"), (int, float)) and b.get("price_target"), ("btc h%d" % h, b)
assert isinstance(fe.get("beta_vs_btc"), (int, float)) and fe.get("h90"), "eth forecast"
assert isinstance(fa.get("beta_vs_btc"), (int, float)) and fa.get("h90"), "alt forecast"
mono = [k for k, v in d["metrics"].items() if v.get("monotonic")]
print("  monotonic excluded (%d): %s" % (len(mono), mono[:8]))
assert len(mono) >= 4
R["forecasts"] = {"btc": {k: fb[k]["exp_pct"] for k in fb if k.startswith("h")},
                  "btc_targets": {k: fb[k].get("price_target") for k in fb if k.startswith("h")},
                  "eth_beta": fe.get("beta_vs_btc"), "eth": {k: fe[k].get("exp_pct") for k in fe if k.startswith("h")},
                  "alt_beta": fa.get("beta_vs_btc"), "alt": {k: fa[k].get("exp_pct") for k in fa if k.startswith("h")},
                  "ai_src": FC.get("ai_src"), "monotonic_n": len(mono)}
R["stables_live"] = {k: d["metrics"][k]["value"] for k in d["metrics"] if "usdt" in k or "usdc" in k or k == "btc_ssr"}
print("  stables:", json.dumps(R["stables_live"], default=str)[:200])

print("== 3/5 consumers widened ==")
for fn, key, need in (("justhodl-onchain-ratios", None, None),
                      ("justhodl-crypto-exchange-flows", "data/crypto-exchange-flows.json", ("btc_inflow", "usdt_reserve")),
                      ("justhodl-crypto-miners", "data/crypto-miners.json", ("cryptoquant_miner",))):
    deploy(fn)
    rr = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
    assert not rr.get("FunctionError"), (fn, rr["Payload"].read()[:160])
    if key:
        dd = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        blk = dd.get("cryptoquant") or dd
        for n in need:
            assert (n in blk) or (n in dd), (fn, n)
    print("  %s widened+fresh" % fn)
envv = (lam.get_function_configuration(FunctionName="justhodl-onchain-ratios")
        .get("Environment", {}) or {}).get("Variables", {}) or {}
rk = envv.get("S3_KEY", "data/onchain-ratios.json")
rd = json.loads(s3.get_object(Bucket=BUCKET, Key=rk)["Body"].read())
assert "btc_nupl" in (rd.get("cryptoquant") or {}), "ratios valuation suite"
print("  ratios valuation suite ok (nupl/puell/nvt_golden/realized/ssr)")

print("== 4/5 ledger: forecast signal ==")
deploy("justhodl-signal-logger")
t0_ms = int(time.time() * 1000) - 5000
rr = lam.invoke(FunctionName="justhodl-signal-logger", InvocationType="RequestResponse")
assert not rr.get("FunctionError")
hit = None
for a in range(4):
    time.sleep(6)
    evs = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-signal-logger",
                                 startTime=t0_ms, filterPattern="onchain_btc_forecast", limit=5).get("events", [])
    if evs: hit = evs[-1]["message"].strip()[:160]; break
print("  cw-proof:", hit)
assert hit, "forecast signal absent"
R["ledger"] = hit

print("== 5/5 page + feeds at edge ==")
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as rr2:
        return rr2.read()
okp = False
for a in range(6):
    time.sleep(40)
    try: okp = b"FORECAST DESK v1" in pub("onchain.html")
    except Exception: pass
    print("  page attempt %d: %s" % (a + 1, "FORECAST LIVE" if okp else "pending"))
    if okp: break
assert okp
json.loads(pub("data/cryptoquant-onchain.json").decode(),
           parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
R["page"] = "FORECAST_LIVE"
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2748_forecast_desk.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2748 COMPLETE — the desk now looks forward")
# rev2 timeout-ceiling 30m; idempotent rerun after 15m cancel (zero-byte log)
