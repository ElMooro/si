"""ops 4362 — CryptoQuant entitlement expansion. Probes a curated superset of
CQ v1 paths with the real key; every path returning data joins the proven
catalog (merge, never remove). cq-feed then pulls it on every run and the
engine's full-fidelity layer + the page's auto-renderer surface it with zero
further code. Denied paths recorded honestly with their status codes."""
import json, os, time, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"
s3=boto3.client("s3",region_name=REGION); ssm=boto3.client("ssm",region_name=REGION)
lam=boto3.client("lambda",region_name=REGION)
R={"ops":4362,"started":datetime.now(timezone.utc).isoformat()}

CANDIDATES=[
 "btc/exchange-flows/inflow","btc/exchange-flows/outflow",
 "btc/exchange-flows/transactions-count","btc/exchange-flows/addresses-count",
 "btc/flow-indicator/fund-flow-ratio","btc/flow-indicator/exchange-supply-ratio",
 "btc/miner-flows/inflow","btc/miner-flows/outflow","btc/miner-flows/reserve",
 "btc/inter-entity-flows/miner-to-exchange",
 "btc/network-indicator/nvt","btc/network-indicator/nvt-golden-cross",
 "btc/network-indicator/nvm","btc/network-indicator/puell-multiple",
 "btc/network-indicator/stock-to-flow","btc/network-indicator/stock-to-flow-reversion",
 "btc/market-data/estimated-leverage-ratio","btc/market-data/open-interest",
 "btc/market-data/funding-rates","btc/market-data/liquidations",
 "btc/market-data/taker-buy-sell-stats","btc/market-data/coinbase-premium-index",
 "btc/network-data/addresses-count","btc/network-data/transactions-count",
 "btc/network-data/tokens-transferred","btc/network-data/fees",
 "btc/network-data/difficulty","btc/network-data/block-interval",
 "btc/network-data/utxo-count","btc/network-data/supply",
 "btc/market-indicator/thermo-cap","btc/market-indicator/delta-cap",
 "eth/exchange-flows/reserve","eth/exchange-flows/netflow",
]

try:
    key=ssm.get_parameter(Name="/justhodl/cryptoquant_api",WithDecryption=True)["Parameter"]["Value"]
except Exception as e:
    key=None; R["key_err"]=str(e)[:100]

try:
    cat=json.loads(s3.get_object(Bucket=BUCKET,Key="data/cq-catalog.json")["Body"].read())
except Exception:
    cat={"catalog":{}}
catalog=cat.get("catalog") or {}
R["catalog_before"]=len(catalog)

entitled,denied=[],[]
if key:
    for path in CANDIDATES:
        if path in catalog:
            continue
        url="https://api.cryptoquant.com/v1/"+path+"?window=day&limit=2"
        try:
            req=urllib.request.Request(url,headers={"Authorization":"Bearer "+key,
                                                    "User-Agent":"Mozilla/5.0"})
            with urllib.request.urlopen(req,timeout=12) as r:
                rows=((json.loads(r.read().decode()).get("result") or {}).get("data")) or []
            if rows and isinstance(rows[0],dict):
                fields=[k for k,v in rows[0].items()
                        if k not in ("date","datetime") and isinstance(v,(int,float))]
                if fields:
                    entitled.append({"path":path,"fields":fields[:6]})
                    catalog[path]={"probed":"ops4362"}
                else:
                    denied.append({"path":path,"why":"200 no numeric fields"})
            else:
                denied.append({"path":path,"why":"200 empty"})
        except urllib.error.HTTPError as e:
            denied.append({"path":path,"why":f"HTTP {e.code}"})
        except Exception as e:
            denied.append({"path":path,"why":str(e)[:60]})
        time.sleep(0.3)

R["entitled_new"]=entitled
R["denied"]=denied
R["catalog_after"]=len(catalog)

if entitled:
    cat["catalog"]=catalog
    cat["expanded_by"]="ops4362"
    cat["expanded_at"]=datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET,Key="data/cq-catalog.json",
                  Body=json.dumps(cat).encode(),ContentType="application/json")
    try:
        from botocore.config import Config
        lam2=boto3.client("lambda",region_name=REGION,
                          config=Config(read_timeout=320,retries={"max_attempts":0}))
        inv=lam2.invoke(FunctionName="justhodl-cq-feed",
                        InvocationType="RequestResponse",Payload=b"{}")
        R["cq_feed_invoke"]={"status":inv.get("StatusCode"),
                             "error":inv.get("FunctionError"),
                             "payload":inv["Payload"].read().decode()[:200]}
        fd=json.loads(s3.get_object(Bucket=BUCKET,Key="data/cq-feed.json")["Body"].read())
        R["cq_feed_after"]={"n_metrics":fd.get("n_metrics"),
                            "generated_at":fd.get("generated_at")}
    except Exception as e:
        R["cq_feed_err"]=str(e)[:150]

R["verdict"]=(f"PASS — catalog {R['catalog_before']} -> {R['catalog_after']}"
              if entitled else
              ("NO GROWTH — all candidates denied/existing" if key else "BLOCKED — no key"))
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4362_cq_expand.json","w"),indent=1,default=str)
open("aws/ops/reports/4362_cq_expand.md","w").write(
    f"# ops 4362 — CQ entitlement expansion — {R['verdict']}\n"
    f"- entitled new ({len(entitled)}): {[e['path'] for e in entitled]}\n"
    f"- denied ({len(denied)}): {[(d['path'],d['why']) for d in denied][:20]}\n"
    f"- cq-feed after: {json.dumps(R.get('cq_feed_after') or R.get('cq_feed_err'))}\n")
print(json.dumps(R,indent=1,default=str))
