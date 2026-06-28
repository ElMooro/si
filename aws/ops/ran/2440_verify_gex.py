import boto3, json, time
from botocore.config import Config
iam=boto3.client("iam"); sch=boto3.client("scheduler","us-east-1")
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); ACCT="857687956942"
fn="justhodl-crypto-gex"
try:
    c=lam.get_function_configuration(FunctionName=fn); print("EXISTS state",c.get("State"),"mem",c.get("MemorySize"))
except Exception as e:
    print("NOT FOUND (create bug again):",str(e)[:80]); print("DONE 2440"); raise SystemExit
# schedule every 30 min on Scheduler
role=iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
name="crypto-gex-sched"; farn="arn:aws:lambda:us-east-1:%s:function:%s"%(ACCT,fn)
try:
    sch.create_schedule(Name=name,ScheduleExpression="rate(30 minutes)",FlexibleTimeWindow={"Mode":"OFF"},Target={"Arn":farn,"RoleArn":role},State="ENABLED"); print("scheduled:",name)
except sch.exceptions.ConflictException:
    sch.update_schedule(Name=name,ScheduleExpression="rate(30 minutes)",FlexibleTimeWindow={"Mode":"OFF"},Target={"Arn":farn,"RoleArn":role},State="ENABLED"); print("updated:",name)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=fn).get("State")=="Active": break
    time.sleep(3)
r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:240])
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-gex.json")["Body"].read())
for ccy in ("btc","eth"):
    s=d.get(ccy) or {}
    if s.get("_err"): print(ccy,"ERR",s["_err"]); continue
    print("\n%s: spot $%s | net GEX $%.0fM | %s"%(ccy.upper(),s.get("spot"),(s.get("net_gex_usd") or 0)/1e6,s.get("regime")))
    print("   flip ~$%s (spot %s%%) | call wall $%s | put wall $%s | max-pain $%s (%s)"%(
        s.get("gamma_flip"),s.get("spot_vs_flip"),s.get("call_wall"),s.get("put_wall"),s.get("max_pain"),s.get("max_pain_exp")))
    print("   call OI %s | put OI %s | P/C %s | strikes %s"%(s.get("total_call_oi"),s.get("total_put_oi"),s.get("put_call_oi_ratio"),s.get("n_strikes")))
print("\ninterp:",d.get("interpretation"),"| dur",d.get("duration_s"),"s")
print("DONE 2440")
