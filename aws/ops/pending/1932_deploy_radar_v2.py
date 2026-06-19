import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"; FN="justhodl-capital-flow-radar"
src=open(glob.glob("**/justhodl-capital-flow-radar/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py",src)
    mp=glob.glob("aws/shared/massive.py")
    if mp: z.writestr("massive.py",open(mp[0]).read())
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("UPDATED radar v2"); break
    except ClientError as e:
        if "ResourceConflict" in str(e): time.sleep(5); continue
        raise
for _ in range(40):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:200])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/capital-flow-radar.json")["Body"].read())
lb=d.get("leveraged_positioning",{})
print("\n========== LEVERAGED POSITIONING BOARD ==========")
print("RISK APPETITE:",lb.get("risk_appetite"))
print("agg bull-lev inflow 5d: $%.0fM | agg bear-lev inflow 5d: $%.0fM"%(
    (lb.get("aggregate_bull_lev_inflow_5d") or 0)/1e6,(lb.get("aggregate_bear_lev_inflow_5d") or 0)/1e6))
print("\nMOST BULLISH leveraged positioning:")
for b in lb.get("most_bullish_positioning",[]):
    print("  %-22s %-12s net=$%sM (bull $%sM / bear $%sM) legs=%s"%(b["name"],b["kind"],
        round(b["net_lev_positioning_5d"]/1e6,1),round(b["bull_lev_flow_5d"]/1e6,1),round(b["bear_lev_flow_5d"]/1e6,1),b["legs"]))
print("\nMOST BEARISH leveraged positioning:")
for b in lb.get("most_bearish_positioning",[]):
    print("  %-22s %-12s net=$%sM (bull $%sM / bear $%sM) legs=%s"%(b["name"],b["kind"],
        round(b["net_lev_positioning_5d"]/1e6,1),round(b["bull_lev_flow_5d"]/1e6,1),round(b["bear_lev_flow_5d"]/1e6,1),b["legs"]))
print("\n========== %d COMPLEXES — TOP 12 BY PUMP PROBABILITY =========="%d.get("n_complexes"))
for c in d.get("complexes",[])[:12]:
    print("  %-24s pump=%-5s %-38s net5d=$%sM acc=%s"%(c["complex"],c["pump_probability"],c["regime"][:38],
        round((c["net_flow_5d_usd"] or 0)/1e6,1),c["accelerating"]))
print("\nPUMP SETUPS:",[c["complex"] for c in d.get("pump_setups",[])])
print("PARTY OVER:",[c["complex"] for c in d.get("party_over_alerts",[])])
print("CASCADE:",json.dumps(d.get("top_pick_cascade",[])[:6]))
