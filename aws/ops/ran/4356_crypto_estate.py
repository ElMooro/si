"""ops 4356 — crypto estate audit (read-mostly; one conditional invoke).

Feeds the crypto-intel v5 + crypto page overhaul design with facts:
 1. SSM /justhodl/* parameter NAMES (never values) — which provider keys exist.
 2. S3 freshness table for every crypto-relevant feed (data/ + root).
 3. CryptoQuant cluster state: cq-catalog proven paths, cq-feed.json age +
    n_metrics, cryptoquant-onchain.json age + shape.
 4. Cadence truth: pull cadence-manifest.json from S3, extract crypto engines.
 5. Live probe of all 15 direct endpoints crypto-intel fetches (status/ms) —
    from AWS egress, same network reality the Lambda sees.
 6. If cq-feed.json is >26h old, invoke justhodl-cq-feed once and re-head.
Report -> aws/ops/reports/4356_crypto_estate.{json,md}
"""
import json, os, time, urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
t0 = datetime.now(timezone.utc)
R = {"ops": 4356, "started": t0.isoformat()}

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=320, retries={"max_attempts": 0}))

# ---------- 1. SSM names ----------
names, tok = [], None
try:
    while True:
        kw = dict(Path="/justhodl/", Recursive=True, MaxResults=10)
        if tok: kw["NextToken"] = tok
        r = ssm.get_parameters_by_path(**kw)
        names += [p["Name"] for p in r.get("Parameters", [])]
        tok = r.get("NextToken")
        if not tok: break
except Exception as e:
    names = [f"ERR {type(e).__name__}: {e}"]
R["ssm_names"] = sorted(names)

# ---------- 2. crypto feed freshness ----------
PAT = ("crypto", "cq-", "altseason", "rainbow", "gex", "narrat", "confluence",
       "etf", "dex", "onchain", "dvol", "funding", "stablecoin", "coinbase",
       "cycle", "whale", "btc", "eth-")
feeds = {}
for prefix in ("data/", ""):
    tok = None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=prefix, MaxKeys=1000, Delimiter="/")
        if tok: kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            k = o["Key"]
            if k.endswith(".json") and any(p in k.lower() for p in PAT):
                age = round((t0 - o["LastModified"]).total_seconds() / 3600, 1)
                feeds[k] = {"age_h": age, "kb": round(o["Size"] / 1024, 1)}
        tok = r.get("NextContinuationToken")
        if not tok: break
R["feeds"] = dict(sorted(feeds.items(), key=lambda kv: kv[1]["age_h"]))

# ---------- 3. CQ cluster ----------
cq = {}
try:
    cat = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cq-catalog.json")["Body"].read())
    paths = list((cat.get("catalog") or {}).keys())
    cq["catalog_paths"] = len(paths)
    cq["catalog_sample"] = paths[:40]
except Exception as e:
    cq["catalog_err"] = f"{type(e).__name__}: {e}"
try:
    fd = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cq-feed.json")["Body"].read())
    cq["feed_generated_at"] = fd.get("generated_at")
    cq["feed_n_metrics"] = fd.get("n_metrics")
    cq["feed_metric_slugs"] = sorted((fd.get("metrics") or {}).keys())[:40]
except Exception as e:
    cq["feed_err"] = f"{type(e).__name__}: {e}"
try:
    oc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
    cq["onchain_generated_at"] = oc.get("generated_at") or oc.get("ts")
    cq["onchain_keys"] = sorted(oc.keys())[:30]
except Exception as e:
    cq["onchain_err"] = f"{type(e).__name__}: {e}"
R["cryptoquant"] = cq

# ---------- 4. cadence truth ----------
try:
    man = json.loads(s3.get_object(Bucket=BUCKET, Key="cadence-manifest.json")["Body"].read())
    body = man.get("functions") or man.get("engines") or man
    R["cadence_crypto"] = {k: v for k, v in body.items()
                          if any(p in k.lower() for p in PAT)} if isinstance(body, dict) else str(type(body))
except Exception as e:
    R["cadence_err"] = f"{type(e).__name__}: {e}"

# ---------- 5. endpoint probes ----------
PROBES = {
    "stablecoins.llama": "https://stablecoins.llama.fi/stablecoins?includePrices=true",
    "llama.chains": "https://api.llama.fi/v2/chains",
    "llama.dexs": "https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume",
    "llama.yields": "https://yields.llama.fi/pools",
    "okx.funding": "https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP",
    "bybit.funding": "https://api.bybit.com/v5/market/funding/history?category=linear&symbol=BTCUSDT&limit=1",
    "bybit.oi": "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=BTCUSDT&intervalTime=1d",
    "coingecko.global": "https://api.coingecko.com/api/v3/global",
    "coingecko.markets": "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=25&sparkline=false",
    "blockchain.charts": "https://api.blockchain.info/charts/n-transactions?timespan=30days&format=json",
    "alternative.fng": "https://api.alternative.me/fng/?limit=31",
    "etherscan.gas": "https://api.etherscan.io/api?module=gastracker&action=gasoracle",
    "beaconchain.gas": "https://beaconcha.in/api/v1/execution/gasnow",
    "owlracle.gas": "https://api.owlracle.info/v4/eth/gas",
    "blockchain.unconf": "https://blockchain.info/unconfirmed-transactions?format=json",
    "blockchain.mcap": "https://api.blockchain.info/charts/market-cap?timespan=365days&format=json",
}
pr = {}
for nm, url in PROBES.items():
    st = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=12) as resp:
            b = resp.read(400)
            pr[nm] = {"http": resp.status, "ms": int((time.time() - st) * 1000),
                      "head": b[:80].decode(errors="replace")}
    except urllib.error.HTTPError as e:
        pr[nm] = {"http": e.code, "ms": int((time.time() - st) * 1000), "err": str(e)[:90]}
    except Exception as e:
        pr[nm] = {"http": None, "ms": int((time.time() - st) * 1000),
                  "err": f"{type(e).__name__}: {e}"[:90]}
    time.sleep(0.3)
R["endpoint_probes"] = pr

# ---------- 6. cq-feed live-fire if stale ----------
try:
    ga = cq.get("feed_generated_at")
    stale = True
    if ga:
        dt = datetime.fromisoformat(ga.replace("Z", "+00:00"))
        stale = (t0 - dt).total_seconds() > 26 * 3600
    if stale:
        inv = lam.invoke(FunctionName="justhodl-cq-feed",
                         InvocationType="RequestResponse", Payload=b"{}")
        R["cq_feed_invoke"] = {"status": inv.get("StatusCode"),
                               "error": inv.get("FunctionError"),
                               "payload": inv["Payload"].read().decode()[:400]}
        time.sleep(3)
        fd = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cq-feed.json")["Body"].read())
        R["cq_feed_after"] = {"generated_at": fd.get("generated_at"),
                              "n_metrics": fd.get("n_metrics")}
except Exception as e:
    R["cq_feed_invoke_err"] = f"{type(e).__name__}: {e}"

R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/4356_crypto_estate.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
fresh = {k: v for k, v in R["feeds"].items() if v["age_h"] < 48}
md = [f"# ops 4356 — crypto estate audit",
      f"- SSM /justhodl names: {len(R['ssm_names'])}",
      f"- crypto feeds on S3: {len(R['feeds'])} ({len(fresh)} fresh<48h)",
      f"- CQ: catalog={cq.get('catalog_paths')} paths, feed n={cq.get('feed_n_metrics')} "
      f"@ {cq.get('feed_generated_at')}, onchain @ {cq.get('onchain_generated_at')}",
      f"- probes: " + ", ".join(f"{k}={v.get('http')}" for k, v in pr.items())]
with open("aws/ops/reports/4356_crypto_estate.md", "w") as f:
    f.write("\n".join(md) + "\n")
print(json.dumps(R, indent=1, default=str))
