"""ops 2038: verify expanded supply-chain-graph v1.1 — node/edge/theme counts, perf populated, laggards, harvester."""
import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); ddb=boto3.client("dynamodb","us-east-1"); B="justhodl-dashboard-live"
# ensure latest code active
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-supply-chain-graph")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(4)
r=lam.invoke(FunctionName="justhodl-supply-chain-graph",InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],"|",r["Payload"].read().decode()[:600])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/supply-chain-graph.json")["Body"].read())
print("\nversion:",d.get("version"),"nodes:",d.get("n_nodes"),"edges:",d.get("n_edges"),"themes:",d.get("n_themes"))
print("themes:",d.get("themes"))
withperf=sum(1 for n in d["nodes"] if n.get("perf_30d") is not None)
print(f"nodes with perf: {withperf}/{d['n_nodes']}")
print("booming hubs:",len(d.get("booming_hubs",[])),"->",d.get("booming_hubs",[])[:14])
print("\nSUPPLY-CHAIN LAGGARDS (suppliers of booming hubs, not yet moved):")
for l in (d.get("supply_chain_laggards") or [])[:15]:
    print(f"  {l['ticker']:<6} ({l['theme']}) {l['relationship']}→{l['supplies_to']} | own {l['own_perf_30d']}% vs cust {l['customer_perf_30d']}% gap {l['lag_gap_pct']}")
print("\nharvester…")
rr=lam.invoke(FunctionName="justhodl-signal-harvester",InvocationType="RequestResponse"); print(" ",rr["Payload"].read().decode()[:120])
time.sleep(3)
n=0
for pg in ddb.get_paginator("scan").paginate(TableName="justhodl-signals",FilterExpression="signal_type=:t",
        ExpressionAttributeValues={":t":{"S":"eng:supply-chain-graph"}},ProjectionExpression="signal_id",Limit=200):
    n+=len(pg.get("Items",[]))
    if n>=200:break
print(" eng:supply-chain-graph rows:",n)
# page still 200
import urllib.request
with urllib.request.urlopen("https://justhodl.ai/supply-chain.html?t="+str(int(time.time())),timeout=15) as rp:
    print("\nsupply-chain.html HTTP",rp.getcode())
print("DONE 2038")
