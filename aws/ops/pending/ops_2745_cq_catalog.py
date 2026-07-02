"""ops 2745 — FULL CRYPTOQUANT CATALOG (Khalid: every metric, money's worth).

PROBE first, guess never: ~60 candidate endpoints across BTC/ETH/stablecoin x
{exchange-flows, flow-indicator, market-indicator, network-indicator,
miner-flows, network-data, market-data} hit with limit=3; every 200-with-rows
becomes a spec entry (detected numeric keys recorded). Curated core-8 keeps
v1 names/signs (downstream fusions + ledger unchanged). Coin Metrics twins
wired for 2010->present conditional stats (MVRV, NVT, active addresses, tx
count, fees, hashrate, difficulty). Deploys engine v2 (timeout 600/mem 1024),
full run, asserts, domain checks incl page v2 marker.
Report: aws/ops/reports/2745_cq_catalog.json.
"""
import os, io, json, time, zipfile, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
BASE = "https://api.cryptoquant.com/v1"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=890, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
R = {"ops": 2745, "ts": datetime.now(timezone.utc).isoformat()}
TOK = ssm.get_parameter(Name="/justhodl/cryptoquant/token", WithDecryption=True)["Parameter"]["Value"].strip()

AE = {"exchange": "all_exchange", "window": "day"}
AM = {"miner": "all_miner", "window": "day"}
W = {"window": "day"}
ST = {"token": "all_token", "exchange": "all_exchange", "window": "day"}
STW = {"token": "all_token", "window": "day"}

CAND = [
 # name, category, label, path, params, risk_sign, in_composite, unit
 ("btc_exchange_netflow","exchange_flows","BTC Exchange Netflow","/btc/exchange-flows/netflow",AE,1,True,"BTC/d"),
 ("btc_exchange_inflow","exchange_flows","BTC Exchange Inflow","/btc/exchange-flows/inflow",AE,1,False,"BTC/d"),
 ("btc_exchange_outflow","exchange_flows","BTC Exchange Outflow","/btc/exchange-flows/outflow",AE,-1,False,"BTC/d"),
 ("btc_exchange_reserve","exchange_flows","BTC Exchange Reserve","/btc/exchange-flows/reserve",AE,1,True,"BTC"),
 ("btc_exchange_addr_in","exchange_flows","BTC Depositing Addresses","/btc/exchange-flows/addresses-count-inflow",AE,1,False,"addr/d"),
 ("btc_exchange_addr_out","exchange_flows","BTC Withdrawing Addresses","/btc/exchange-flows/addresses-count-outflow",AE,-1,False,"addr/d"),
 ("btc_mpi","flow_indicator","Miner Position Index","/btc/flow-indicator/mpi",W,1,True,"idx"),
 ("btc_whale_ratio","flow_indicator","Exchange Whale Ratio","/btc/flow-indicator/exchange-whale-ratio",AE,1,True,"ratio"),
 ("btc_fund_flow_ratio","flow_indicator","Fund Flow Ratio","/btc/flow-indicator/fund-flow-ratio",AE,0,False,"ratio"),
 ("btc_stablecoins_ratio","flow_indicator","Exchange Stablecoins Ratio","/btc/flow-indicator/exchange-stablecoins-ratio",AE,0,False,"ratio"),
 ("btc_exchange_supply_ratio","flow_indicator","Exchange Supply Ratio","/btc/flow-indicator/exchange-supply-ratio",AE,1,False,"ratio"),
 ("btc_mvrv","market_indicator","MVRV Ratio","/btc/market-indicator/mvrv",W,1,True,"ratio"),
 ("btc_sopr","market_indicator","SOPR","/btc/market-indicator/sopr",W,1,True,"ratio"),
 ("btc_sopr_ratio","market_indicator","SOPR Ratio (LTH/STH)","/btc/market-indicator/sopr-ratio",W,0,False,"ratio"),
 ("btc_nupl","market_indicator","Net Unrealized P/L (NUPL)","/btc/market-indicator/nupl",W,1,False,"frac"),
 ("btc_realized_price","market_indicator","Realized Price","/btc/market-indicator/realized-price",W,0,False,"USD"),
 ("btc_ssr","market_indicator","Stablecoin Supply Ratio","/btc/market-indicator/stablecoin-supply-ratio",W,1,False,"ratio"),
 ("btc_nvt","network_indicator","NVT","/btc/network-indicator/nvt",W,1,False,"ratio"),
 ("btc_nvt_golden","network_indicator","NVT Golden Cross","/btc/network-indicator/nvt-golden-cross",W,1,False,"idx"),
 ("btc_nvm","network_indicator","NVM","/btc/network-indicator/nvm",W,0,False,"idx"),
 ("btc_puell","network_indicator","Puell Multiple","/btc/network-indicator/puell-multiple",W,1,False,"idx"),
 ("btc_stock_to_flow","network_indicator","Stock-to-Flow Deviation","/btc/network-indicator/stock-to-flow",W,0,False,"idx"),
 ("btc_miner_netflow","miner_flows","Miner Netflow","/btc/miner-flows/netflow",AM,1,False,"BTC/d"),
 ("btc_miner_outflow","miner_flows","Miner Outflow","/btc/miner-flows/outflow",AM,1,False,"BTC/d"),
 ("btc_miner_reserve","miner_flows","Miner Reserve","/btc/miner-flows/reserve",AM,-1,False,"BTC"),
 ("btc_tx_count","network_data","Transactions Count","/btc/network-data/transactions-count",W,0,False,"tx/d"),
 ("btc_addresses_active","network_data","Active Addresses","/btc/network-data/addresses-count-active",W,-1,False,"addr/d"),
 ("btc_fees_total","network_data","Total Fees","/btc/network-data/fees",W,0,False,"BTC/d"),
 ("btc_fees_tx_mean","network_data","Mean Fee per Tx","/btc/network-data/fees-transaction",W,0,False,"BTC"),
 ("btc_blockreward","network_data","Block Reward","/btc/network-data/blockreward",W,0,False,"BTC/d"),
 ("btc_difficulty","network_data","Mining Difficulty","/btc/network-data/difficulty",W,0,False,"idx"),
 ("btc_hashrate","network_data","Hashrate","/btc/network-data/hashrate",W,0,False,"H/s"),
 ("btc_utxo_count","network_data","UTXO Count","/btc/network-data/utxo-count",W,0,False,"utxo"),
 ("btc_velocity","network_data","Velocity","/btc/network-data/velocity",W,0,False,"idx"),
 ("btc_tokens_transferred","network_data","Tokens Transferred","/btc/network-data/tokens-transferred-total",W,0,False,"BTC/d"),
 ("btc_supply_total","network_data","Supply Total","/btc/network-data/supply-total",W,0,False,"BTC"),
 ("btc_open_interest","market_data","Futures Open Interest","/btc/market-data/open-interest",AE,1,False,"USD"),
 ("btc_funding_rates","market_data","Funding Rates","/btc/market-data/funding-rates",AE,1,False,"%"),
 ("btc_liquidations","market_data","Liquidations","/btc/market-data/liquidations",AE,0,False,"USD/d"),
 ("btc_taker_ratio","market_data","Taker Buy/Sell Ratio","/btc/market-data/taker-buy-sell-stats",AE,-1,False,"ratio"),
 ("btc_coinbase_premium","market_data","Coinbase Premium Index","/btc/market-data/coinbase-premium-index",W,-1,False,"idx"),
 ("btc_leverage_ratio","market_data","Estimated Leverage Ratio","/btc/market-data/estimated-leverage-ratio",AE,1,False,"ratio"),
 ("eth_exchange_netflow","eth","ETH Exchange Netflow","/eth/exchange-flows/netflow",AE,1,False,"ETH/d"),
 ("eth_exchange_inflow","eth","ETH Exchange Inflow","/eth/exchange-flows/inflow",AE,1,False,"ETH/d"),
 ("eth_exchange_outflow","eth","ETH Exchange Outflow","/eth/exchange-flows/outflow",AE,-1,False,"ETH/d"),
 ("eth_exchange_reserve","eth","ETH Exchange Reserve","/eth/exchange-flows/reserve",AE,1,True,"ETH"),
 ("eth_addresses_active","eth","ETH Active Addresses","/eth/network-data/addresses-count-active",W,-1,False,"addr/d"),
 ("eth_tx_count","eth","ETH Transactions Count","/eth/network-data/transactions-count",W,0,False,"tx/d"),
 ("eth_open_interest","eth","ETH Futures Open Interest","/eth/market-data/open-interest",AE,1,False,"USD"),
 ("eth_funding_rates","eth","ETH Funding Rates","/eth/market-data/funding-rates",AE,1,False,"%"),
 ("eth_leverage_ratio","eth","ETH Estimated Leverage Ratio","/eth/market-data/estimated-leverage-ratio",AE,1,False,"ratio"),
 ("stablecoin_exchange_reserve","stablecoins","Stablecoin Exchange Reserve","/stablecoin/exchange-flows/reserve",ST,-1,True,"USD"),
 ("stablecoin_exchange_netflow","stablecoins","Stablecoin Exchange Netflow","/stablecoin/exchange-flows/netflow",ST,-1,False,"USD/d"),
 ("stablecoin_exchange_inflow","stablecoins","Stablecoin Exchange Inflow","/stablecoin/exchange-flows/inflow",ST,-1,False,"USD/d"),
 ("stablecoin_exchange_outflow","stablecoins","Stablecoin Exchange Outflow","/stablecoin/exchange-flows/outflow",ST,1,False,"USD/d"),
 ("stablecoin_supply_total","stablecoins","Stablecoin Supply Total","/stablecoin/network-data/supply-total",STW,0,False,"USD"),
]
TWINS = {"btc_mvrv": "CapMVRVCur", "btc_nvt": "NVTAdj", "btc_addresses_active": "AdrActCnt",
         "btc_tx_count": "TxCnt", "btc_fees_total": "FeeTotNtv", "btc_hashrate": "HashRate",
         "btc_difficulty": "DiffMean", "btc_supply_total": "SplyCur"}

def hit(path, params):
    q = dict(params); q["limit"] = "3"
    url = BASE + path + "?" + "&".join("%s=%s" % kv for kv in q.items())
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + TOK, "User-Agent": "JustHodl/2.0"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            doc = json.loads(r.read())
        rows = ((doc or {}).get("result") or {}).get("data") or []
        keys = [k for k in rows[0] if k not in ("date", "datetime")] if rows else []
        return 200, keys
    except urllib.error.HTTPError as he:
        return he.code, []
    except Exception:
        return None, []

print("settling 25s…"); time.sleep(25)
print("== 1/4 CATALOG PROBE (%d candidates) ==" % len(CAND))
live, dead = [], []
for name, cat, label, path, params, sign, incore, unit in CAND:
    st, keys = hit(path, params)
    if st == 200 and keys:
        live.append({"name": name, "category": cat, "label": label, "path": path,
                     "params": dict(params), "value_keys": keys[:4], "risk_sign": sign,
                     "in_composite": incore, "unit": unit})
        print("  LIVE %-30s keys=%s" % (name, keys[:3]))
    else:
        dead.append({"name": name, "status": st})
        print("  dead %-30s %s" % (name, st))
    time.sleep(1.1)
R["probe"] = {"live": len(live), "dead": [d["name"] for d in dead]}
assert len(live) >= 30, "catalog thin: %d" % len(live)
core = [m["name"] for m in live if m["in_composite"]]
assert len(core) == 8, "core-8 broken: %s" % core
spec = {"base": BASE, "from_format": "none", "plan_window_days": 365,
        "plan_note": "Professional tier: 1y API window; series accrue daily toward 2000d; 2010+ context via Coin Metrics twins",
        "twins": TWINS, "metrics": live}
s3.put_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json",
              Body=json.dumps(spec, indent=1).encode(), ContentType="application/json")
print("  spec v2 written: %d metrics, core-8 intact" % len(live))

print("== 2/4 deploy engine v2 + full run ==")
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
        lam.update_function_configuration(FunctionName="justhodl-cryptoquant", Timeout=600, MemorySize=1024)
        wait_ok("justhodl-cryptoquant")
        lam.update_function_code(FunctionName="justhodl-cryptoquant", ZipFile=zip_fn("justhodl-cryptoquant"))
        break
    except ClientError: time.sleep(18)
wait_ok("justhodl-cryptoquant")
r = lam.invoke(FunctionName="justhodl-cryptoquant", InvocationType="RequestResponse",
               Payload=json.dumps({"backfill": True}).encode())
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:300])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
M = d["metrics"]
R["live_run"] = {"n": d["n_metrics"], "categories": d["categories"],
                 "composite": d["composite_onchain_risk_z"], "errors": d.get("errors")}
print("  categories:", d["categories"])
hr = [k for k in M if M[k].get("hist_read")]
tw = [k for k in M if "2010" in (M[k].get("stats_window") or "")]
cr = [k for k in M if M[k].get("corr_1y") is not None]
print("  hist_read coverage %d/%d | 2010-window %d | corr %d" % (len(hr), len(M), len(tw), len(cr)))
print("  sample:", M.get("btc_mvrv", {}).get("hist_read"))
assert d["n_metrics"] >= 30 and len(hr) >= int(0.7 * len(M)) and len(tw) >= 4 and len(cr) >= int(0.7 * len(M))
assert all(k in M for k in core), "core-8 missing from run"
sr = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-series.json")["Body"].read())
assert len(sr.get("series") or {}) >= 30 and len(sr.get("btc", {}).get("d") or []) >= 400 and len(sr.get("twins") or {}) >= 4
R["series"] = {"n": len(sr["series"]), "btc_points": len(sr["btc"]["d"]), "twins": sorted(sr["twins"])}
print("  series feed: %d series, btc %d pts (2010+), twins %s" % (len(sr["series"]), len(sr["btc"]["d"]), sorted(sr["twins"])[:4]))

print("== 3/4 domain checks ==")
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as rr:
        return rr.read()
for f in ("data/cryptoquant-onchain.json", "data/cryptoquant-series.json"):
    json.loads(pub(f).decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    print("  strict:", f)
okp = False
for a in range(5):
    time.sleep(40)
    try: okp = b"ONCHAIN DESK v2" in pub("onchain.html")
    except Exception: pass
    print("  page attempt %d: %s" % (a + 1, "v2 LIVE" if okp else "pending"))
    if okp: break
assert okp, "page v2 not at edge"
R["page"] = "LIVE_v2"

print("== 4/4 report ==")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2745_cq_catalog.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2745 COMPLETE — the whole catalog, probed and proven")
