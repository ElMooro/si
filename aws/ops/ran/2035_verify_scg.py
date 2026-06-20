"""ops 2035: ensure schedule, invoke supply-chain-graph, verify graph + laggard alpha + harvester."""
import boto3, json, time
REGION="us-east-1"; FN="justhodl-supply-chain-graph"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION); ddb=boto3.client("dynamodb",REGION)
for _ in range(40):
    try:
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": break
    except Exception: pass
    time.sleep(4)
print("state:",c.get("State"),c.get("LastUpdateStatus"))
arn=c["FunctionArn"]
rule="justhodl-supply-chain-graph-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(45 13 ? * TUE-SAT *)",State="ENABLED",Description="daily supply-chain-graph")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-scg",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],"|",r["Payload"].read().decode()[:500])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/supply-chain-graph.json")["Body"].read())
print("\nnodes:",d.get("n_nodes"),"edges:",d.get("n_edges"),"booming_hubs:",d.get("booming_hubs"))
print("\nSUPPLY-CHAIN LAGGARDS (suppliers of booming hubs, not yet moved):")
for l in (d.get("supply_chain_laggards") or [])[:12]:
    print(f"  {l['ticker']:<6} ({l['theme']}) supplies {l['relationship']} → {l['supplies_to']} | own {l['own_perf_30d']}% vs cust {l['customer_perf_30d']}% | gap {l['lag_gap_pct']}")
print("\nharvester…")
try:
    rr=lam.invoke(FunctionName="justhodl-signal-harvester",InvocationType="RequestResponse"); print(" ",rr["Payload"].read().decode()[:130])
    time.sleep(3)
    n=0
    for pg in ddb.get_paginator("scan").paginate(TableName="justhodl-signals",FilterExpression="signal_type=:t",
            ExpressionAttributeValues={":t":{"S":"eng:supply-chain-graph"}},ProjectionExpression="signal_id",Limit=100):
        n+=len(pg.get("Items",[]))
        if n>=100:break
    print(" eng:supply-chain-graph rows:",n)
except Exception as e: print(" harvester err:",str(e)[:120])
print("DONE 2035")
