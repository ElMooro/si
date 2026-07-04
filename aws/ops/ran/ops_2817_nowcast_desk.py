"""ops 2817 — create justhodl-nowcast-desk (free Fed nowcast suite), schedule, seed, verify."""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
REGION="us-east-1"; ACCT="857687956942"; ROLE="arn:aws:iam::%s:role/lambda-execution-role"%ACCT
FN="justhodl-nowcast-desk"; SRC="aws/lambdas/%s/source"%FN
lam=boto3.client("lambda",region_name=REGION); events=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
R={"ops":2817,"ts":datetime.now(timezone.utc).isoformat(),"steps":{}}
def wait_ready(t=40):
    for _ in range(t):
        try:
            c=lam.get_function_configuration(FunctionName=FN)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return True
        except Exception: pass
        time.sleep(3)
    return False
try:
    FRED=""
    for eng in ("justhodl-china-liquidity","fedliquidityapi","justhodl-macro-leads"):
        env=lam.get_function_configuration(FunctionName=eng).get("Environment",{}).get("Variables",{})
        FRED=env.get("FRED_API_KEY") or env.get("FRED_KEY") or FRED
        if FRED: break
    R["steps"]["fred_key"]=bool(FRED)
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        for fn in os.listdir(SRC):
            if fn.endswith(".py"): z.write(os.path.join(SRC,fn),fn)
        for fn in os.listdir("aws/shared"):
            if fn.endswith(".py"): z.write(os.path.join("aws/shared",fn),fn)
    zb=buf.getvalue()
    envv={"Variables":{"FRED_API_KEY":FRED}}
    try:
        lam.get_function(FunctionName=FN)
        lam.update_function_code(FunctionName=FN,ZipFile=zb); wait_ready()
        lam.update_function_configuration(FunctionName=FN,Timeout=120,MemorySize=256,Environment=envv); wait_ready()
        R["steps"]["fn"]="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
            Timeout=120,MemorySize=256,Environment=envv,Code={"ZipFile":zb}); wait_ready()
        R["steps"]["fn"]="created"
    # schedule
    rule="justhodl-nowcast-desk-daily"
    events.put_rule(Name=rule,ScheduleExpression="cron(30 13 * * ? *)",State="ENABLED",Description="Daily Fed nowcast desk")
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    try: lam.add_permission(FunctionName=FN,StatementId=rule+"-inv",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
    except lam.exceptions.ResourceConflictException: pass
    events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}])
    R["steps"]["schedule"]="wired"
    # seed + verify
    lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/nowcast-desk.json")["Body"].read())
    infl=d.get("underlying_inflation") or {}; comp=infl.get("composite") or {}
    R["steps"]["verify"]={"blocks_live":d.get("_blocks_live"),
        "gdpnow":(d.get("gdp_nowcast") or {}).get("value"),"gdpnow_signal":(d.get("gdp_nowcast") or {}).get("signal"),
        "underlying_inflation":comp.get("underlying_inflation_pct"),"infl_trend":comp.get("trend"),"n_infl":comp.get("n_measures"),
        "sticky_cpi":(infl.get("sticky_cpi") or {}).get("value"),"median_cpi":(infl.get("median_cpi") or {}).get("value"),
        "wage_overall":(d.get("wage_growth_tracker") or {}).get("overall",{}).get("value"),
        "regime":(d.get("nowcast_quadrant") or {}).get("regime")}
    R["status"]="NOWCAST DESK LIVE" if d.get("_blocks_live",0)>=2 else "CHECK"
except Exception as e:
    R["status"]="ERR"; R["steps"]["ERROR"]=repr(e)[:200]
print(json.dumps(R["steps"],indent=1,default=str)); print("STATUS:",R["status"])
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2817_nowcast_desk.json","w"),indent=1,default=str)
print("OPS 2817 COMPLETE")
