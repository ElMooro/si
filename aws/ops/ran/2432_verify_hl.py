import boto3, json, time
from botocore.config import Config
iam=boto3.client("iam"); sch=boto3.client("scheduler","us-east-1")
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=90,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); ACCT="857687956942"
role=iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
fn="justhodl-hyperliquid-perps"; name="hyperliquid-perps-sched"
farn="arn:aws:lambda:us-east-1:%s:function:%s"%(ACCT,fn)
try:
    sch.create_schedule(Name=name,ScheduleExpression="cron(10 * * * ? *)",FlexibleTimeWindow={"Mode":"OFF"},Target={"Arn":farn,"RoleArn":role},State="ENABLED"); print("scheduled hourly:",name)
except sch.exceptions.ConflictException:
    sch.update_schedule(Name=name,ScheduleExpression="cron(10 * * * ? *)",FlexibleTimeWindow={"Mode":"OFF"},Target={"Arn":farn,"RoleArn":role},State="ENABLED"); print("updated:",name)
r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:220])
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hyperliquid-perps.json")["Body"].read())
print("total OI: $%.2fB | BTC OI $%.2fB funding %s%%/yr premium %sbps | ETH OI $%.2fB | SOL OI $%.2fB"%(
    (d.get("total_oi_usd") or 0)/1e9,(d.get("btc",{}).get("oi_usd") or 0)/1e9,d.get("btc",{}).get("funding_ann_pct"),
    d.get("btc",{}).get("premium_bps"),(d.get("eth",{}).get("oi_usd") or 0)/1e9,(d.get("sol",{}).get("oi_usd") or 0)/1e9))
print("leverage_regime:",d.get("leverage_regime"),"| liq_proxy:",d.get("liq_pressure_proxy"),"| hist_n:",d.get("history_n"))
print("top OI:",[(x["coin"],"$%.1fB"%(x["oi_usd"]/1e9)) for x in (d.get("top_oi") or [])[:5]])
print("funding extremes most_long:",d.get("funding_extremes",{}).get("most_long")[:3])
print("interp:",d.get("interpretation"))
print("DONE 2432")
