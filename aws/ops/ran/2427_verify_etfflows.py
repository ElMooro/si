import boto3, json, time
from botocore.config import Config
iam=boto3.client("iam"); sch=boto3.client("scheduler","us-east-1")
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); ACCT="857687956942"
role=iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
# schedule on EventBridge Scheduler (daily 23:00 UTC)
fn="justhodl-crypto-etf-flows"; name="crypto-etf-flows-sched"
farn="arn:aws:lambda:us-east-1:%s:function:%s"%(ACCT,fn)
try:
    sch.create_schedule(Name=name,ScheduleExpression="cron(0 23 * * ? *)",FlexibleTimeWindow={"Mode":"OFF"},Target={"Arn":farn,"RoleArn":role},State="ENABLED")
    print("scheduled:",name)
except sch.exceptions.ConflictException:
    sch.update_schedule(Name=name,ScheduleExpression="cron(0 23 * * ? *)",FlexibleTimeWindow={"Mode":"OFF"},Target={"Arn":farn,"RoleArn":role},State="ENABLED"); print("updated:",name)
# invoke
r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:200])
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-etf-flows.json")["Body"].read())
for leg in ("btc_etf","eth_etf"):
    s=d.get(leg) or {}
    if s.get("_err"): print(leg,"ERR",s["_err"]); continue
    print("\n%s (%s): today $%s | 5d $%s | 30d $%s (%sth) | %s | %d ETFs | AUM $%.1fB"%(
        leg, s.get("last_date"), f"{s.get('flow_today_usd'):,}", f"{s.get('cum_5d_usd'):,}", f"{s.get('cum_30d_usd'):,}",
        s.get("cum_30d_pctile"), s.get("regime"), s.get("n_etfs"), (s.get("aum_total_usd") or 0)/1e9))
    print("   top inflow:",[(x["etf"],f"${x['flow_usd']:,}") for x in s.get("top_inflow",[])])
    es=s.get("event_study") or {}
    print("   event study %s: %s"%(es.get("verdict"),es.get("hypothesis")))
    for h in ("fwd10d","fwd20d"):
        v=es.get(h) or {}; print("     %s: inflow %s%% vs outflow %s%% | edge %spp (n %s/%s)"%(h,v.get("inflow_mean"),v.get("outflow_mean"),v.get("edge_pp"),v.get("n_in"),v.get("n_out")))
print("\ninterp:",d.get("interpretation"),"| hist:",d.get("history_n"),"| diag:",d.get("_diag"))
print("DONE 2427")
