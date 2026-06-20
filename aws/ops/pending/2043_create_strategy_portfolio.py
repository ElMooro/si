"""ops 2043: create justhodl-strategy-portfolio via boto3, schedule, invoke, verify combined book."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-strategy-portfolio"; B="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
ENV={"Variables":{"POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d","S3_BUCKET":B}}
SRC=f"aws/lambdas/{FN}/source"; buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for r,_,fs in os.walk(SRC):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,SRC))
zb=buf.getvalue()
try: lam.get_function(FunctionName=FN); ex=True
except lam.exceptions.ResourceNotFoundException: ex=False
if ex:
    print("update"); lam.update_function_code(FunctionName=FN,ZipFile=zb)
    for _ in range(30):
        if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")!="InProgress":break
        time.sleep(4)
    lam.update_function_configuration(FunctionName=FN,Environment=ENV,Timeout=600,MemorySize=1024,Runtime="python3.12",Handler="lambda_function.lambda_handler")
else:
    print("create"); lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zb},Timeout=600,MemorySize=1024,Environment=ENV,Architectures=["x86_64"],
        Description="Strategy-of-strategies combined proven-alpha book")
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful":break
    time.sleep(4)
arn=c["FunctionArn"];print("active")
rule="justhodl-strategy-portfolio-weekly"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(20 12 ? * SUN *)",State="ENABLED",Description="weekly strategy-portfolio")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-sp",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]);print("scheduled")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],"|",r["Payload"].read().decode()[:700])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/strategy-portfolio.json")["Body"].read())
if not d.get("ok"):
    print("NOT OK:",d.get("reason"),"candidates:",d.get("candidate_set") or d.get("candidates")); print("DONE 2043"); raise SystemExit
print("\nproven_set:",d["proven_set"])
print("candidate_set:",d["candidate_set"])
print("history:",d["history_from"],"→",d["history_to"],"|",d["n_weeks"],"weeks |",d["n_engines"],"engines")
print("\nPER-ENGINE:")
for e in d["per_engine"]:
    print(f"  {e['engine']:<26} {str(e['alpha_status']):<14} net_t {e['net_t_stat']} net_exc {e['net_mean_excess_pct']}% IR {e['info_ratio']} | wk_vol {e['weekly_vol_pct']}% n_wk {e['n_weeks']} n_pick {e['n_picks']} | {e['capacity_tier']}")
print("\nCORRELATION (labels):",d["correlation_matrix"]["labels"])
for i,row in enumerate(d["correlation_matrix"]["matrix"]):
    print("  ",d["correlation_matrix"]["labels"][i][:18].ljust(18),row)
print("\nWEIGHTINGS (Sharpe / annRet / annVol / maxDD / Calmar / divRatio / effBets):")
for k,v in d["weightings"].items():
    print(f"  {k:<13} Sharpe {v['sharpe']} | ret {v['ann_return_pct']}% vol {v['ann_vol_pct']}% maxDD {v['max_drawdown_pct']}% Calmar {v['calmar']} | divR {v['diversification_ratio']} effBets {v['effective_bets']}")
print("\nRECOMMENDED (HRP) weights:",d["recommended"]["weights"])
# SSM check
try:
    ssm=boto3.client("ssm",REGION); p=ssm.get_parameter(Name="/justhodl/calibration/strategy-weights")
    print("SSM strategy-weights written:",p["Parameter"]["Value"][:120])
except Exception as e: print("ssm read:",str(e)[:80])
print("DONE 2043")
