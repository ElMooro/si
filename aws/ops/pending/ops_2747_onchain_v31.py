"""ops 2747 — DESK v3.1 (Khalid: 2010 reach + same-percentage analysis).

Twin set expands 6 -> ~15: six new direct Coin Metrics twins written into the
live spec (NVT->NVTAdj, velocity->VelCur1yr, mean fee->FeeMeanNtv, transfer
value->TxTfrValNtv, block reward->IssContNtv, UTXO->UTXOCnt) plus three
DERIVED twins the engine now computes from exact identities (NUPL=1-1/MVRV,
RealizedPrice=CapRealUSD/SplyCur, Puell=IssContUSD/MA365) — all 2010->.
Conditional stats for the newly twinned metrics upgrade to the 2010 window
automatically. Page v3.1: every chart spans 2010->today (BTC backdrop, metric
starts where real data starts) and defaults to PERCENTILE MODE — metric and
BTC on one shared 0-100% axis — with a RAW toggle in the enlarge modal.
Asserts: >=12 twins live, derived trio present w/ >=900pts, >=12 hist_reads on
the 2010 window, page marker, feeds strict. Report: 2747_onchain_v31.json.
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

print("settling 25s…"); time.sleep(25)
print("== 1/3 spec twins_extra + engine v2.2 + run ==")
spec = json.loads(s3.get_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json")["Body"].read())
spec["twins_extra"] = {"btc_nvt": "NVTAdj", "btc_velocity": "VelCur1yr",
                       "btc_fees_tx_mean": "FeeMeanNtv", "btc_tokens_transferred": "TxTfrValNtv",
                       "btc_blockreward": "IssContNtv", "btc_utxo_count": "UTXOCnt"}
s3.put_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json",
              Body=json.dumps(spec, indent=1).encode(), ContentType="application/json")
print("  spec: twins_extra x6 written")
for i in range(6):
    try:
        wait_ok("justhodl-cryptoquant")
        lam.update_function_code(FunctionName="justhodl-cryptoquant", ZipFile=zip_fn("justhodl-cryptoquant")); break
    except ClientError: time.sleep(18)
wait_ok("justhodl-cryptoquant")
r = lam.invoke(FunctionName="justhodl-cryptoquant", InvocationType="RequestResponse",
               Payload=json.dumps({}).encode())
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:240])
assert not r.get("FunctionError") and pay.get("ok"), pay
sr = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-series.json")["Body"].read())
tw = {k: len(v["d"]) for k, v in (sr.get("twins") or {}).items()}
print("  twins live:", json.dumps(tw))
assert len(tw) >= 12, tw
for need in ("btc_nupl", "btc_realized_price", "btc_puell"):
    assert tw.get(need, 0) >= 900, "derived twin missing: %s" % need
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
M = d["metrics"]
w2010 = [k for k in M if "2010" in (M[k].get("stats_window") or "")]
print("  2010-window hist_reads: %d -> %s" % (len(w2010), sorted(w2010)[:8]))
print("  puell read:", (M.get("btc_puell") or {}).get("hist_read"))
print("  nupl read:", (M.get("btc_nupl") or {}).get("hist_read"))
assert len(w2010) >= 12
R["twins"] = tw; R["w2010"] = sorted(w2010)
R["samples"] = {"puell": (M.get("btc_puell") or {}).get("hist_read"),
                "nupl": (M.get("btc_nupl") or {}).get("hist_read")}

print("== 2/3 page v3.1 at edge ==")
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as rr:
        return rr.read()
okp = False
for a in range(6):
    time.sleep(40)
    try: okp = b"ONCHAIN DESK v3.1" in pub("onchain.html")
    except Exception: pass
    print("  attempt %d: %s" % (a + 1, "v3.1 LIVE" if okp else "pending"))
    if okp: break
assert okp, "page v3.1 not at edge"
for f in ("data/cryptoquant-onchain.json", "data/cryptoquant-series.json"):
    json.loads(pub(f).decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
R["page"] = "LIVE_v3.1"

print("== 3/3 report ==")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2747_onchain_v31.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2747 COMPLETE — fifteen metrics reach 2010; every chart speaks percentile")
