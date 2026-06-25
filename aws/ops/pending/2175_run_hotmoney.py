import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=790,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-hot-money"; ACC="857687956942"
try: lam.get_function(FunctionName=FN); print("exists")
except lam.exceptions.ResourceNotFoundException:
    print("MISSING — deploy-lambdas did not create; aborting"); raise SystemExit
# ensure schedule
try: ev.describe_rule(Name="justhodl-hot-money-daily"); print("rule present")
except Exception:
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    ev.put_rule(Name="justhodl-hot-money-daily",ScheduleExpression="cron(0 22 ? * MON-FRI *)",State="ENABLED")
    try: lam.add_permission(FunctionName=FN,StatementId="ev-justhodl-hot-money-daily",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-hot-money-daily")
    except Exception: pass
    ev.put_targets(Rule="justhodl-hot-money-daily",Targets=[{"Id":"1","Arn":arn}]); print("rule created")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hot-money.json")["Body"].read())
print(f"\nn_countries={d.get('n_countries')} world {d.get('world_benchmark')} ret20={d.get('world_ret_20d_pct')}% dur={d.get('duration_s')}s signals={d.get('signals_logged')}")
print("\n=== HOT MONEY INFLOW LEADERS (worldwide) ===")
for c in d.get("inflow_leaders",[])[:10]:
    print(f"  #{c['rank']:<2} {c['country']:<14} score {c['hot_money_score']:+.2f}  rel_mom20 {c.get('rel_mom_20d')}%  flow5d ${(c.get('net_flow_5d_usd') or 0)/1e6:.0f}M  etfs={c['etfs']}")
print("\n=== OUTFLOW (capital leaving) ===")
for c in d.get("outflow_leaders",[])[:6]:
    print(f"  {c['country']:<14} score {c['hot_money_score']:+.2f} rel_mom20 {c.get('rel_mom_20d')}%")
print("\n=== DRILL-DOWN (hot country -> sectors -> stocks) ===")
for country,dd in list(d.get("drilldowns",{}).items())[:4]:
    secs=", ".join(f"{s['sector']} {s['weight_pct']}%" for s in dd.get("top_sectors",[])[:4])
    print(f"\n  {country} [{dd['etf']}] score {dd['hot_money_score']}")
    print(f"    sectors: {secs}")
    hh=dd.get("top_holdings",[])[:8]
    print(f"    stocks: "+", ".join(f"{h['ticker']}({h['weight_pct']}%{(' '+str(h['day_chg_pct'])+'%') if h.get('day_chg_pct') is not None else ''})" for h in hh))
print("DONE 2175")
