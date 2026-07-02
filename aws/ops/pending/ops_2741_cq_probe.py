"""ops 2741 — CRYPTOQUANT ENDPOINT DISCOVERY + FULL GO-LIVE.

Token proven (btc netflow/reserve returned real rows); six paths 400'd.
This ops reads the vendor's own 400 error bodies, walks candidate
path/param variants per failing metric until real daily rows return,
writes the resolved truth into data/config/cryptoquant-spec.json (the
spec-driven design paying off), then reruns the adapter with full 5y
backfill under LIVE gates. Report: aws/ops/reports/2741_cq_probe.json.
"""
import os, io, json, time, zipfile, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
BASE = "https://api.cryptoquant.com/v1"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
R = {"ops": 2741, "ts": datetime.now(timezone.utc).isoformat()}
TOK = ssm.get_parameter(Name="/justhodl/cryptoquant/token", WithDecryption=True)["Parameter"]["Value"].strip()

def hit(path, params):
    q = dict(params); q["limit"] = "5"
    url = BASE + path + "?" + "&".join("%s=%s" % kv for kv in q.items())
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + TOK, "User-Agent": "JustHodl/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            doc = json.loads(r.read())
        rows = ((doc or {}).get("result") or {}).get("data") or (doc or {}).get("data") or []
        return 200, rows, None
    except urllib.error.HTTPError as he:
        return he.code, [], (he.read() or b"")[:160].decode("utf-8", "ignore")
    except Exception as e:
        return None, [], str(e)[:120]

CANDIDATES = {
    "eth_exchange_reserve": [
        ("/eth/exchange-flows/reserve", {"exchange": "all_exchange", "window": "day"}),
        ("/eth/exchange-flows/reserve", {"exchange": "binance", "window": "day"}),
        ("/eth/exchange-flows/reserve", {"window": "day"}),
    ],
    "btc_mpi": [
        ("/btc/flow-indicator/mpi", {"window": "day"}),
        ("/btc/miner-flows/mpi", {"window": "day"}),
        ("/btc/flow-indicator/mpi", {"miner": "all_miner", "window": "day"}),
        ("/btc/network-indicator/mpi", {"window": "day"}),
    ],
    "btc_whale_ratio": [
        ("/btc/flow-indicator/exchange-whale-ratio", {"exchange": "all_exchange", "window": "day"}),
        ("/btc/flow-indicator/exchange-whale-ratio", {"exchange": "binance", "window": "day"}),
        ("/btc/flow-indicator/whale-ratio", {"exchange": "all_exchange", "window": "day"}),
        ("/btc/exchange-flows/exchange-whale-ratio", {"exchange": "all_exchange", "window": "day"}),
    ],
    "btc_mvrv": [
        ("/btc/market-indicator/mvrv", {"window": "day"}),
        ("/btc/network-indicator/mvrv", {"window": "day"}),
        ("/btc/market-data/mvrv", {"window": "day"}),
        ("/btc/network-indicator/mvrv-ratio", {"window": "day"}),
    ],
    "btc_sopr": [
        ("/btc/market-indicator/sopr", {"window": "day"}),
        ("/btc/network-indicator/sopr", {"window": "day"}),
        ("/btc/market-indicator/sopr-ratio", {"window": "day"}),
        ("/btc/network-indicator/sopr-ratio", {"window": "day"}),
    ],
    "stablecoin_exchange_reserve": [
        ("/stablecoin/exchange-flows/reserve", {"exchange": "all_exchange", "window": "day"}),
        ("/stablecoin/exchange-flows/reserve", {"token": "all_token", "exchange": "all_exchange", "window": "day"}),
        ("/usdt/exchange-flows/reserve", {"exchange": "all_exchange", "window": "day"}),
        ("/stablecoin/exchange-flows/reserve", {"token": "usdt", "exchange": "all_exchange", "window": "day"}),
    ],
}

print("settling 25s…"); time.sleep(25)
print("== 1/3 PROBE: vendor error bodies + candidate walk ==")
spec = json.loads(s3.get_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json")["Body"].read())
by_name = {m["name"]: m for m in spec["metrics"]}
resolved, unresolved = {}, {}
for name, cands in CANDIDATES.items():
    st0, _, err0 = hit(*cands[0])
    print("  %s baseline -> %s %s" % (name, st0, (err0 or "")[:110]))
    R.setdefault("baseline_errors", {})[name] = "%s %s" % (st0, (err0 or "")[:120])
    ok = None
    for path, params in cands:
        st, rows, err = hit(path, params)
        if st == 200 and rows:
            keys = sorted(rows[0].keys())
            ok = {"path": path, "params": params, "sample_keys": keys}
            print("    RESOLVED %s -> %s %s keys=%s" % (name, path, params, keys[:6]))
            break
        time.sleep(0.25)
    if ok:
        resolved[name] = ok
        m = by_name[name]
        m["path"], m["params"] = ok["path"], {k: v for k, v in ok["params"].items()}
        vk = [k for k in ok["sample_keys"] if k not in ("date", "datetime")]
        m["value_keys"] = list(dict.fromkeys([k for k in m["value_keys"] if k in ok["sample_keys"]] + vk))[:4]
    else:
        unresolved[name] = True
        print("    UNRESOLVED", name)
R["resolved"], R["unresolved"] = resolved, sorted(unresolved)
s3.put_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json",
              Body=json.dumps(spec, indent=1).encode(), ContentType="application/json")
assert len(resolved) >= 4, "probe resolved too few: %s" % list(resolved)

print("== 1.5/3 PARAM MATRIX (backfill-only 400 isolation, FULL bodies) ==")
def hit_raw(path, params):
    url = BASE + path + "?" + "&".join("%s=%s" % kv for kv in params.items())
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + TOK, "User-Agent": "JustHodl/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            doc = json.loads(r.read())
        rows = ((doc or {}).get("result") or {}).get("data") or []
        return 200, len(rows), ""
    except urllib.error.HTTPError as he:
        return he.code, 0, (he.read() or b"")[:300].decode("utf-8", "ignore")
MATRIX = [
    ("limit1000", {"window": "day", "limit": "1000"}),
    ("from_plain", {"window": "day", "limit": "1000", "from": "20210601"}),
    ("from_T", {"window": "day", "limit": "1000", "from": "20210601T000000"}),
    ("from_dash", {"window": "day", "limit": "1000", "from": "2021-06-01"}),
    ("limit300", {"window": "day", "limit": "300"}),
]
ffmt = "none"
for tag, prm in MATRIX:
    st, n, body = hit_raw("/btc/market-indicator/mvrv", prm)
    print("  mvrv[%s] -> %s rows=%s %s" % (tag, st, n, body[:170]))
    R.setdefault("matrix", {})[tag] = {"status": st, "rows": n, "body": body[:200]}
    if st == 200 and n > 500 and tag == "from_plain": ffmt = "plain"
    elif st == 200 and n > 500 and tag == "from_T" and ffmt == "none": ffmt = "T"
    elif st == 200 and n > 500 and tag == "from_dash" and ffmt == "none": ffmt = "dash"
    time.sleep(0.3)
if ffmt == "dash": ffmt = "plain"  # engine supports plain/T/none; dash unexpected
lim_ok = R["matrix"].get("limit1000", {}).get("status") == 200
if ffmt == "none" and not lim_ok:
    print("  NOTE: even bare limit=1000 400s — indicator endpoints likely cap limit; engine single-page limit300x? keeping none")
spec["from_format"] = ffmt
R["from_format"] = ffmt
s3.put_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json",
              Body=json.dumps(spec, indent=1).encode(), ContentType="application/json")
print("  from_format resolved:", ffmt)

print("== 2/3 redeploy + FULL BACKFILL run ==")
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
    except ClientError:
        time.sleep(18)
wait_ok("justhodl-cryptoquant")
r = lam.invoke(FunctionName="justhodl-cryptoquant", InvocationType="RequestResponse",
               Payload=json.dumps({"backfill": True}).encode())
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:300])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
assert d["status"] == "LIVE"
M = d["metrics"]
hist = json.loads(s3.get_object(Bucket=BUCKET, Key="data/history/cryptoquant.json")["Body"].read())
depth = {k: len(v) for k, v in hist.items()}
R["live"] = {"n_metrics": len(M), "depth": depth, "composite": d["composite_onchain_risk_z"],
             "staleness_d": d["max_staleness_days"], "read": d.get("read"),
             "values": {k: (M[k]["value"], M[k]["z365"], M[k]["pctl_1y"]) for k in M},
             "errors": d.get("errors")}
print("  LIVE metrics:", json.dumps(R["live"]["values"], default=str)[:340])
print("  depth:", depth)
print("  composite:", d["composite_onchain_risk_z"], "|", d.get("read"))
assert len(M) >= 6, "still thin: %s errors=%s" % (list(M), d.get("errors"))
min_hist = 900 if R.get("from_format") == "none" else 1200
assert min(depth[k] for k in M) >= min_hist, "backfill shallow (fmt=%s): %s" % (R.get("from_format"), depth)
assert d["max_staleness_days"] <= 3

print("== 3/3 public feed strict ==")
okf = False
for a in range(3):
    time.sleep(18)
    try:
        req = urllib.request.Request("https://justhodl.ai/data/cryptoquant-onchain.json?v=%d" % a,
                                     headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=25) as rr:
            json.loads(rr.read().decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        okf = True; break
    except Exception as e:
        print("  attempt %d: %s" % (a + 1, str(e)[:70]))
assert okf
R["public_feed"] = True
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2741_cq_probe.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2741 COMPLETE — on-chain seat FILLED with vendor-verified endpoints")

# rev2 param-matrix from_format
