import os, glob, json, datetime, boto3
events=boto3.client("events",region_name="us-east-1")
cw=boto3.client("cloudwatch",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1")

PROVIDERS = {
 "FMP":["financialmodelingprep.com"],"Polygon":["api.polygon.io"],"AlphaVantage":["alphavantage.co"],
 "Yahoo":["finance.yahoo.com"],"Stooq":["stooq.com"],"Finviz":["finviz.com"],"Benzinga":["benzinga.com"],
 "Nasdaq":["nasdaq.com"],"SqueezeMetrics":["squeezemetrics.com"],
 "FRED":["stlouisfed.org"],"ECB":["ecb.europa.eu"],"NYFed":["newyorkfed.org"],
 "Treasury":["treasury.gov","treasurydirect"],"OFR":["financialresearch.gov"],"FedReserve":["federalreserve.gov"],
 "WorldGovBonds":["worldgovernmentbonds.com"],"BLS":["bls.gov"],"BEA":["bea.gov"],"Census":["census.gov"],
 "EIA":["eia.gov"],"USASpending":["usaspending.gov"],"OECD":["oecd.org"],"DBnomics":["db.nomics"],
 "SEC_EDGAR":["sec.gov"],"CFTC":["cftc.gov"],"FINRA":["finra.org"],"CBOE":["cboe.com"],"PatentsView":["patentsview"],
 "QuiverQuant":["quiverquant"],"CongressData":["unitedstates.github.io","theunitedstates.io"],"WhiteHouse":["whitehouse.gov"],
 "CFR":["cfr.org"],"Reddit":["reddit.com"],"Stocktwits":["stocktwits"],"ApeWisdom":["apewisdom"],
 "CoinGecko":["coingecko"],"CMC":["coinmarketcap"],"DefiLlama":["llama.fi"],"OKX":["okx.com"],"Bybit":["bybit.com"],
 "Binance":["binance"],"AltMe_FearGreed":["alternative.me"],"Coinbase":["coinbase"],"Coinglass":["coinglass"],
 "Coinmetrics":["coinmetrics"],"Etherscan":["etherscan.io"],"Mempool":["mempool.space"],"Blockchain.info":["blockchain.info"],
 "NewsAPI":["newsapi.org"],"GoogleNews":["news.google.com"],"GoogleTrends":["trends.google.com"],"GDELT":["gdeltproject"],
 "AAII":["aaii.com"],"Wikimedia":["wikimedia.org"],
 "Anthropic":["anthropic.com"],"Zai_GLM":["z.ai"],"OpenAI":["openai.com"],
}

# lambda -> providers (from source)
lam_prov={}
for d in sorted(glob.glob("aws/lambdas/*")):
    fn=os.path.basename(d)
    src=""
    for f in glob.glob(d+"/source/*.py"):
        try: src+=open(f,encoding="utf-8",errors="ignore").read()
        except: pass
    if not src: continue
    used=[p for p,hosts in PROVIDERS.items() if any(h in src for h in hosts)]
    if used: lam_prov[fn]=set(used)

# EventBridge: scheduled fn -> (rule,state,cron)
sched={}
p=events.get_paginator("list_rules")
for pg in p.paginate():
    for r in pg["Rules"]:
        if not r.get("ScheduleExpression"): continue
        try:
            for t in events.list_targets_by_rule(Rule=r["Name"])["Targets"]:
                arn=t.get("Arn","")
                if ":function:" in arn:
                    f=arn.split(":function:")[1].split(":")[0]
                    sched[f]=(r["Name"],r.get("State"),r["ScheduleExpression"])
        except: pass

# CloudWatch invocations last 30d (batched GetMetricData)
fns=sorted(lam_prov.keys())
end=datetime.datetime.utcnow(); start=end-datetime.timedelta(days=30)
inv={}
for i in range(0,len(fns),450):
    chunk=fns[i:i+450]
    q=[{"Id":f"m{j}","MetricStat":{"Metric":{"Namespace":"AWS/Lambda","MetricName":"Invocations",
        "Dimensions":[{"Name":"FunctionName","Value":fn}]},"Period":2592000,"Stat":"Sum"},"ReturnData":True}
       for j,fn in enumerate(chunk)]
    r=cw.get_metric_data(MetricDataQueries=q,StartTime=start,EndTime=end)
    for m in r["MetricDataResults"]:
        idx=int(m["Id"][1:]); inv[chunk[idx]]=sum(m["Values"]) if m["Values"] else 0

def status(fn):
    s=sched.get(fn); n=inv.get(fn,0)
    if s and s[1]=="ENABLED": return f"SCHED({s[2].replace('cron(','').replace(' * ? *)','')}) inv30d={int(n)}"
    if s: return f"SCHED-DISABLED inv30d={int(n)}"
    return (f"on-demand inv30d={int(n)}" if n>0 else "DORMANT inv30d=0")

# provider -> consumers
prov_cons={}
for fn,ps in lam_prov.items():
    for p_ in ps: prov_cons.setdefault(p_,[]).append(fn)

print(f"Engines with external data deps: {len(lam_prov)} | scheduled rules hit: {len(sched)} | inv data for {len(inv)} fns\n")
rows=[]
for prov in PROVIDERS:
    cons=prov_cons.get(prov,[])
    live=[c for c in cons if (sched.get(c) and sched[c][1]=="ENABLED") or inv.get(c,0)>0]
    dormant=[c for c in cons if c not in live]
    rows.append((prov,len(cons),len(live),len(dormant)))
rows.sort(key=lambda x:-x[2])
print("PROVIDER             total live dormant")
for prov,tot,nl,nd in rows:
    print(f"  {prov:18} {tot:5} {nl:4} {nd:4}")
print("\n=== fully DORMANT data sources (0 live consumers) ===")
for prov,tot,nl,nd in rows:
    if tot>0 and nl==0:
        print(f"  {prov}: {prov_cons.get(prov,[])[:6]}")
print("\n=== per-provider live engine detail (top providers) ===")
for prov in ["FMP","Polygon","FRED","SEC_EDGAR","ECB","CFTC","CMC","Finviz","Polygon","QuiverQuant","NewsAPI"]:
    cons=sorted(prov_cons.get(prov,[]))
    live=[c for c in cons if (sched.get(c) and sched[c][1]=="ENABLED") or inv.get(c,0)>0]
    print(f"\n{prov}: {len(live)} live / {len(cons)} total")
    for c in sorted(live, key=lambda x:-inv.get(x,0))[:8]:
        print(f"   {c:36} {status(c)}")
