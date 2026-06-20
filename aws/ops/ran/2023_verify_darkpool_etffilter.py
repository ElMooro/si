"""ops 2023: confirm ETF filter live (single names lead), ignition dark_to_adv populated, harvester."""
import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); ddb=boto3.client("dynamodb","us-east-1"); B="justhodl-dashboard-live"
r=lam.invoke(FunctionName="justhodl-dark-pool",InvocationType="RequestResponse")
print("dark-pool:",r["StatusCode"],"|",r["Payload"].read().decode()[:300])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/dark-pool.json")["Body"].read())
print("scored:",d.get("n_scored"),"accum:",d.get("distribution",{}).get("accumulation"))
ETF_HINT={"HIMU","HYD","VGSH","IUSB","SUSB","IBTG","EFG","URTH","BCI","SCHE","IEMG","JQUA","QUAL","TLH","XOVR","RONB"}
top=[r["ticker"] for r in (d.get("top_accumulation") or [])[:15]]
print("TOP15:",top)
leaked=[t for t in top if t in ETF_HINT]
print("ETF leakage in top15:",leaked if leaked else "NONE ✓")
for r in (d.get("top_accumulation") or [])[:12]:
    print(f"  {r['ticker']:<6} sc={r['score']:<5} dark%={r['dark_pool_pct']:<6} accel={r['dark_accel']} wkRet={r['week_return_pct']}%")
print("PICKS:",[p['ticker'] for p in d.get('top_picks',[])])
print("\nharvester…")
rr=lam.invoke(FunctionName="justhodl-signal-harvester",InvocationType="RequestResponse"); print(" ",rr["Payload"].read().decode()[:140])
time.sleep(3)
n=0
for pg in ddb.get_paginator("scan").paginate(TableName="justhodl-signals",FilterExpression="signal_type=:t",
        ExpressionAttributeValues={":t":{"S":"eng:dark-pool"}},ProjectionExpression="signal_id",Limit=300):
    n+=len(pg.get("Items",[]))
    if n>=300:break
print(" eng:dark-pool rows:",n)
print("\nignition dark wire…")
ri=lam.invoke(FunctionName="justhodl-ignition",InvocationType="RequestResponse"); print(" ignition:",ri["StatusCode"])
time.sleep(2)
ig=json.loads(s3.get_object(Bucket=B,Key="data/ignition.json")["Body"].read())
board=ig.get("board") or ig.get("rows") or []
withdark=[(x.get("ticker"),x.get("dark_to_adv_w")) for x in board if x.get("dark_to_adv_w") is not None]
print(" probes.dark:",(ig.get("probes") or {}).get("dark")," board rows w/ dark_to_adv:",len(withdark)," sample:",withdark[:6])
print("DONE 2023")
