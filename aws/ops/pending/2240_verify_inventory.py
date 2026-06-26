import boto3, json
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); ev=boto3.client("events","us-east-1")
FN="justhodl-inventory-drawdown"
# invoke + verify
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],r["Payload"].read().decode()[:160])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/inventory-drawdown.json")["Body"].read())
print("counts:",json.dumps(d.get("counts")))
print("\nSECTOR DRAWDOWN (falling I/S ratio = sector drawing down):")
for s in d.get("sector_drawdown",[])[:9]:
    print(f"  {s['sector']:<24} ratio={s['latest_ratio']} 6m={s['chg_6m']}% pctl5y={s['percentile_5y']} score={s['drawdown_score']} [{s['flag']}]")
print("\nBOOM SETUPS (DIO falling >=8% YoY into rising demand):")
bs=d.get("boom_setups",[])
if not bs: print("  (none today)")
for r in bs[:10]:
    print(f"  {r['ticker']:<6} DIO {r['dio_4q_ago']}->{r['dio_latest']} ({r['dio_chg_pct']}%) rev={r['rev_growth_yoy']}% boom={r['boom_score']} | {r.get('industry')}")
print("\nDRAWDOWN BOARD (top by drawdown, any class):")
for r in d.get("stock_drawdown_board",[])[:12]:
    print(f"  {r['ticker']:<6} [{r['classification']}] DIO {r['dio_chg_pct']}% rev={r['rev_growth_yoy']}% boom={r['boom_score']} draw={r['draw_score']}")
print("signals_logged:",d.get("signals_logged"))
# piggyback schedule on an existing weekly rule (account at EventBridge rule cap)
host="justhodl-capital-inflows-weekly"
try:
    tgts=ev.list_targets_by_rule(Rule=host).get("Targets",[])
    ids=[t["Id"] for t in tgts]
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    if not any(t["Arn"]==arn for t in tgts):
        newid=str(max([int(i) for i in ids if i.isdigit()]+[len(ids)])+1)
        ev.put_targets(Rule=host,Targets=[{"Id":newid,"Arn":arn}])
        try:
            lam.add_permission(FunctionName=FN,StatementId="inv-drawdown-piggyback",Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{host}")
        except Exception as e: print("perm:",str(e)[:40])
        print(f"ATTACHED to {host} as target {newid} (runs weekly Thu alongside capital-inflows)")
    else:
        print(f"already a target of {host}")
except Exception as e:
    print("schedule attach failed:",str(e)[:80])
print("DONE 2240")
