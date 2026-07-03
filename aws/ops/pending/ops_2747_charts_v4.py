"""ops 2747 — CHARTS v4: 2010 context everywhere + %-shared-axis (Khalid).

Twin fleet expanded via spec (asset-aware + computed): btc mvrv/tx/addresses/
fees-total/fees-mean/hashrate/difficulty/supply/velocity/blockreward/
tokens-transferred/nvt/realized-price(CapRealUSD/SplyCur) + eth addresses/tx
(2015->). Engine v2.2 deployed + run. Page v4: FULL mode draws BTC-2010 log
context under EVERY metric (non-twins marked where data begins), PCT mode
rebases metric+BTC to one shared %-axis; both toggleable per-card and in the
enlarge modal. Asserts: twins >=11 incl eth + computed realized-price, page
v4 marker at edge, feeds strict. Report: 2747_charts_v4.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=890, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2747, "ts": datetime.now(timezone.utc).isoformat()}

print("settling 25s…"); time.sleep(25)
print("== 1/4 spec: expanded twins ==")
spec = json.loads(s3.get_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json")["Body"].read())
spec["twins"] = {
    "btc_mvrv": "CapMVRVCur", "btc_tx_count": "TxCnt", "btc_addresses_active": "AdrActCnt",
    "btc_fees_total": "FeeTotNtv", "btc_fees_tx_mean": "FeeMeanNtv",
    "btc_hashrate": "HashRate", "btc_difficulty": "DiffMean", "btc_supply_total": "SplyCur",
    "btc_velocity": "VelCur1yr", "btc_blockreward": "IssContNtv",
    "btc_tokens_transferred": "TxTfrValAdjNtv", "btc_nvt": "NVTAdj",
    "btc_realized_price": "CapRealUSD/SplyCur",
    "eth_addresses_active": "eth:AdrActCnt", "eth_tx_count": "eth:TxCnt",
}
s3.put_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json",
              Body=json.dumps(spec, indent=1).encode(), ContentType="application/json")
print("  twins spec ->", len(spec["twins"]))

print("== 2/4 engine v2.2 + run ==")
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
for i in range(6):
    try:
        wait_ok("justhodl-cryptoquant")
        lam.update_function_code(FunctionName="justhodl-cryptoquant", ZipFile=zip_fn("justhodl-cryptoquant")); break
    except ClientError: time.sleep(18)
wait_ok("justhodl-cryptoquant")
r = lam.invoke(FunctionName="justhodl-cryptoquant", InvocationType="RequestResponse", Payload=b"{}")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:240])
assert not r.get("FunctionError") and pay.get("ok"), pay
sr = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-series.json")["Body"].read())
tw = {k: len(v["d"]) for k, v in (sr.get("twins") or {}).items()}
print("  twins delivered:", json.dumps(tw))
assert len(tw) >= 11, tw
assert "btc_realized_price" in tw and tw["btc_realized_price"] >= 900, "computed twin"
assert "eth_addresses_active" in tw and tw["eth_addresses_active"] >= 700, "eth twin"
R["twins"] = tw
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
t2010 = [k for k, v in d["metrics"].items() if "2010" in (v.get("stats_window") or "")]
print("  metrics on 2010-window stats:", len(t2010))
assert len(t2010) >= 10
R["stats_2010_count"] = len(t2010)
R["realized_read"] = (d["metrics"].get("btc_realized_price") or {}).get("hist_read")
print("  realized-price read:", R["realized_read"])

print("== 3/4 page v4 at edge ==")
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as rr:
        return rr.read()
okp = False
for a in range(6):
    time.sleep(40)
    try: okp = b"ONCHAIN DESK v4" in pub("onchain.html")
    except Exception: pass
    print("  attempt %d: %s" % (a + 1, "v4 LIVE" if okp else "pending"))
    if okp: break
assert okp, "page v4 not at edge"
for f in ("data/cryptoquant-onchain.json", "data/cryptoquant-series.json"):
    json.loads(pub(f).decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
R["page"] = "LIVE_v4"

print("== 4/4 report ==")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2747_charts_v4.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2747 COMPLETE — every chart wears its full history")
